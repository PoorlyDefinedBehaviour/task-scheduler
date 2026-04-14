use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{Postgres, Transaction};
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

use crate::tasks::{CreateTaskInput, ListInput, Task};

#[derive(Clone)]
pub struct IO {
    pub tasks: Arc<dyn Storage>,
    pub rng: Arc<dyn Rng>,
    pub time: Arc<dyn Clock>,
    pub http: Arc<dyn Http>,
}

#[async_trait]
pub trait Storage: Send + Sync {
    async fn create(&self, input: &CreateTaskInput) -> Result<Uuid>;
    async fn list(&self, input: &ListInput) -> Result<Vec<Task>>;
    async fn get_by_id(&self, task_id: &Uuid) -> Result<Option<Task>>;
    async fn delete_by_id(&self, task_id: &Uuid) -> Result<bool>;
    async fn claim(
        &self,
        max_batch_size: u16,
        now: DateTime<Utc>,
    ) -> Result<(ClaimGuard, Vec<Task>)>;
    async fn complete(&self, guard: &mut ClaimGuard, task_ids: &[Uuid]) -> Result<()>;
}

pub struct ClaimGuard<'a> {
    // The guard is leaking the storage details but it would be possible make it opaque.
    pub tx: Transaction<'a, Postgres>,
}

pub trait Rng: Send + Sync {
    fn random_salt(&self) -> Vec<u8>;
}

pub trait Clock: Send + Sync {
    fn utc_now(&self) -> DateTime<Utc>;
}

#[async_trait]
pub trait Http: Send + Sync {
    async fn post(
        &self,
        headers: &HashMap<String, String>,
        url: &str,
        body: Vec<u8>,
    ) -> Result<Response>;
}

#[derive(Debug)]
pub struct Response {
    pub status: u16,
}
