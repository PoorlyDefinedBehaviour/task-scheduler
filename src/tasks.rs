use crate::contracts::IO;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug)]
pub enum Task {
    Webhook {
        id: Uuid,
        url: String,
        body: Vec<u8>,
        state: TaskState,
        execution_time_at: DateTime<Utc>,
    },
    Hash {
        id: Uuid,
        secret: Vec<u8>,
        state: TaskState,
        execution_time_at: DateTime<Utc>,
    },
}

impl Task {
    pub fn id(&self) -> Uuid {
        match self {
            Task::Webhook { id, .. } => *id,
            Task::Hash { id, .. } => *id,
        }
    }

    pub fn task_type(&self) -> TaskType {
        match self {
            Task::Webhook { .. } => TaskType::Webhook,
            Task::Hash { .. } => TaskType::Hash,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TaskState {
    Pending,
    Completed,
    Failed,
}

#[derive(Debug)]
pub enum CreateTaskInput {
    Webhook {
        url: String,
        body: Vec<u8>,
        execution_time_at: chrono::DateTime<Utc>,
    },
    Hash {
        secret: Vec<u8>,
        execution_time_at: chrono::DateTime<Utc>,
    },
}

#[tracing::instrument(skip_all, ret, err, fields(?input))]
pub async fn create(io: IO, input: CreateTaskInput) -> Result<Uuid> {
    io.tasks
        .create(&input)
        .await
        .context("saving task to storage")
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TaskType {
    Webhook,
    Hash,
}
#[derive(Debug)]
pub struct ListInput {
    pub task_type: Option<TaskType>,
    pub state: Option<TaskState>,
}

#[tracing::instrument(skip_all, ret, err, fields(?input))]
pub async fn list(io: IO, input: ListInput) -> Result<Vec<Task>> {
    io.tasks
        .list(&input)
        .await
        .context("fetching tasks from storage")
}

#[tracing::instrument(skip_all, ret, err, fields(?task_id))]
pub async fn get_by_id(io: IO, task_id: Uuid) -> Result<Option<Task>> {
    io.tasks
        .get_by_id(&task_id)
        .await
        .context("fetching task from storage by id")
}

#[tracing::instrument(skip_all, ret, err, fields(?task_id))]
pub async fn delete_task_by_id(io: IO, task_id: Uuid) -> Result<bool> {
    io.tasks
        .delete_by_id(&task_id)
        .await
        .context("delete task from storage by id")
}
