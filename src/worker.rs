use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    contracts::{IO, Response},
    tasks::Task,
};
use anyhow::{Context, Result};
use base64::Engine;
use chrono::{DateTime, Utc};
use sha2::Digest;

use tokio::{sync::Semaphore, task::JoinSet};
use tracing::error;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Config {
    pub tick_interval: Duration,
    pub max_batch_size: u16,
    pub webhook_request_timeout: Duration,
    pub concurrency_limit: usize,
}

#[tracing::instrument(skip_all, fields(?cfg))]
pub async fn control_loop(io: IO, cfg: Config) {
    let mut interval = tokio::time::interval(cfg.tick_interval);

    loop {
        if let Err(err) = tick(io.clone(), &cfg, io.time.utc_now()).await {
            error!(?err, "worker errored while processing tasks");
        }
        interval.tick().await;
    }
}

#[tracing::instrument(skip_all, ret, err, fields(?cfg))]
pub async fn tick(io: IO, cfg: &Config, now: DateTime<Utc>) -> Result<()> {
    let (mut guard, tasks) = io
        .tasks
        .claim(cfg.max_batch_size, now)
        .await
        .context("fetching pending tasks")?;

    let mut joinset = JoinSet::new();

    let sema = Arc::new(Semaphore::new(cfg.concurrency_limit));

    for task in tasks {
        let io = io.clone();
        let cfg = cfg.clone();
        let task_id = task.id();
        let task_type = task.task_type();
        let _permit = sema
            .clone()
            .acquire_owned()
            .await
            .context("acquiring semaphore before processing task")?;

        joinset.spawn(async move {
            let _permit = _permit;
            match process_task(io, &cfg, task).await {
                Err(err) => {
                    anyhow::bail!("processing task {task_id} of type {task_type:?}: {err:?}")
                }
                Ok(()) => Ok(task_id),
            }
        });
    }

    // Wait for all futures to complete
    let mut completed_tasks = Vec::new();

    while let Some(result) = joinset.join_next().await {
        match result {
            Ok(Ok(task_id)) => completed_tasks.push(task_id),
            Ok(Err(err)) => error!(?err, "error processing task"),
            Err(err) => error!(?err, "error in call to JoinSet::join_next"),
        }
    }

    if !completed_tasks.is_empty() {
        io.tasks
            .complete(&mut guard, &completed_tasks)
            .await
            .context("setting completed tasks state to completed")?;
    }

    guard.release().await.context("releasing claimed tasks")?;

    Ok(())
}

async fn process_task(io: IO, cfg: &Config, task: Task) -> Result<()> {
    match task {
        Task::Webhook { id, url, body, .. } => {
            let idempotency_key = { base64::prelude::BASE64_URL_SAFE.encode(id.as_bytes()) };

            match tokio::time::timeout(
                cfg.webhook_request_timeout,
                execute_webhook(io.clone(), id, url, body, idempotency_key),
            )
            .await
            {
                Err(err) => anyhow::bail!("webhook execution timed out: {err:?}"),
                Ok(v) => v,
            }
        }
        Task::Hash { id, secret, .. } => {
            let salt = io.rng.random_salt();
            let handle = tokio::task::spawn_blocking(move || {
                execute_hash(id, &secret, salt);
            });

            handle.await.context("waiting for hash task execution")?;

            Ok(())
        }
    }
}

#[tracing::instrument(skip_all, ret, err, fields(?task_id, ?url, ?idempotency_key))]
async fn execute_webhook(
    io: IO,
    task_id: Uuid,
    url: String,
    body: Vec<u8>,
    idempotency_key: String,
) -> Result<()> {
    let headers = HashMap::from([("idempotency_key".to_owned(), idempotency_key)]);
    let response: Response = io
        .http
        .post(&headers, &url, body)
        .await
        .context("sending http request")?;
    // NOTE: println! may block, would have to use something else in the real codebase
    println!(
        "webhook: task {task_id} response_status={}",
        response.status
    );
    Ok(())
}

#[tracing::instrument(skip_all)]
fn execute_hash(task_id: Uuid, secret: &[u8], mut salt: Vec<u8>) {
    salt.extend_from_slice(secret);
    let mut result = salt;

    for _ in 0..600 {
        let mut digest = sha2::Sha512::new();
        digest.update(result);
        result = digest.finalize().to_vec();
    }

    let base64_encoded = base64::prelude::BASE64_STANDARD.encode(&result);
    println!("hash: task {task_id} = {base64_encoded}");
}
