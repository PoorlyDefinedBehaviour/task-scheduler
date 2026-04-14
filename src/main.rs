mod api;
mod clock;
mod contracts;
mod http;
mod rng;
mod storage;
mod tasks;
mod worker;
use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};

use crate::{
    clock::DefaultClock, contracts::IO, http::ReqwestHttp, rng::DefaultRng,
    storage::PostgresStorage,
};

#[tokio::main]
async fn main() -> Result<()> {
    let io = IO {
        tasks: Arc::new(PostgresStorage::new().await?),
        rng: Arc::new(DefaultRng::new()),
        time: Arc::new(DefaultClock::new()),
        http: Arc::new(ReqwestHttp::new()),
    };
    let _worker_handle = tokio::spawn(worker::control_loop(
        io.clone(),
        worker::Config {
            tick_interval: Duration::from_secs(1),
            max_batch_size: 2,
            webhook_request_timeout: Duration::from_secs(10),
            concurrency_limit: 2,
        },
    ));
    let app = api::router(io);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
        .await
        .context("binding tcp listener to port")?;

    axum::serve(listener, app).await.unwrap();

    Ok(())
}
