use axum::{
    Json,
    extract::{Path, Query, State},
    response::IntoResponse,
    routing::{delete, get, post},
};
use chrono::{DateTime, Utc};
use serde_json::json;
use tracing::error;
use uuid::Uuid;

use crate::{
    contracts::IO,
    tasks::{self, CreateTaskInput, TaskType},
};

pub fn router(io: IO) -> axum::Router {
    let app = axum::Router::new()
        .route("/tasks", post(handle_create_task))
        .route("/tasks", get(handle_list_tasks))
        .route("/tasks/{id}", get(handle_get_task))
        .route("/tasks/{id}", delete(handle_delete_task))
        .with_state(io);

    app
}

#[derive(Debug, serde::Deserialize)]
struct CreateTaskPayload {
    task_type: String,
    url: Option<String>,
    body: Option<String>,
    secret: Option<String>,
    execution_time_at: chrono::DateTime<Utc>,
}

impl TryInto<tasks::CreateTaskInput> for CreateTaskPayload {
    type Error = String;

    fn try_into(self) -> Result<tasks::CreateTaskInput, Self::Error> {
        let task_type = tasks::TaskType::try_from(self.task_type.as_str())?;
        let parsed = match task_type {
            tasks::TaskType::Webhook => {
                let Some(url) = self.url else {
                    return Err("url is required".to_owned());
                };
                let Some(body) = self.body else {
                    return Err("body is required".to_owned());
                };
                CreateTaskInput::Webhook {
                    url,
                    body: body.as_bytes().to_vec(),
                    execution_time_at: self.execution_time_at,
                }
            }
            tasks::TaskType::Hash => {
                let Some(secret) = self.secret else {
                    return Err("secret is required".to_owned());
                };
                CreateTaskInput::Hash {
                    secret: secret.as_bytes().to_vec(),
                    execution_time_at: self.execution_time_at,
                }
            }
        };
        Ok(parsed)
    }
}

#[axum::debug_handler]
#[tracing::instrument(skip_all, ret, fields(?payload))]
async fn handle_create_task(
    State(io): State<IO>,
    Json(payload): Json<CreateTaskPayload>,
) -> impl IntoResponse {
    let input = match payload.try_into() {
        Err(err) => {
            return (
                http::StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("{err}")})),
            )
                .into_response();
        }
        Ok(v) => v,
    };
    match tasks::create(io, input).await {
        Ok(id) => Json(json!({"data": id})).into_response(),
        Err(err) => {
            error!(?err, "creating task");
            http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct ListTasksQuery {
    task_type: Option<String>,
    state: Option<String>,
}

impl TryInto<tasks::ListInput> for ListTasksQuery {
    type Error = String;

    fn try_into(self) -> Result<tasks::ListInput, Self::Error> {
        let task_type = if let Some(task_type) = self.task_type {
            Some(tasks::TaskType::try_from(task_type.as_str())?)
        } else {
            None
        };
        let state = if let Some(state) = self.state {
            Some(tasks::TaskState::try_from(state.as_str())?)
        } else {
            None
        };
        Ok(tasks::ListInput { task_type, state })
    }
}

#[derive(Debug, serde::Serialize)]
struct Task {
    id: Uuid,
    task_type: String,
    state: String,
    url: Option<String>,
    body: Option<Vec<u8>>,
    secret: Option<Vec<u8>>,
    execution_time_at: DateTime<Utc>,
}

impl From<tasks::Task> for Task {
    fn from(value: tasks::Task) -> Self {
        match value {
            tasks::Task::Webhook {
                id,
                url,
                body,
                state,
                execution_time_at,
            } => Task {
                id,
                task_type: tasks::TaskType::Webhook.into(),
                url: Some(url),
                body: Some(body),
                state: state.into(),
                secret: None,
                execution_time_at,
            },
            tasks::Task::Hash {
                id,
                secret,
                state,
                execution_time_at,
            } => Task {
                id,
                task_type: tasks::TaskType::Hash.into(),
                url: None,
                body: None,
                secret: Some(secret),
                state: state.into(),
                execution_time_at: execution_time_at,
            },
        }
    }
}

#[axum::debug_handler]
#[tracing::instrument(skip_all, ret, fields(?query))]
async fn handle_list_tasks(
    State(io): State<IO>,
    Query(query): Query<ListTasksQuery>,
) -> impl IntoResponse {
    let input = match query.try_into() {
        Err(err) => {
            return (
                http::StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("{err}")})),
            )
                .into_response();
        }
        Ok(v) => v,
    };
    match tasks::list(io, input).await {
        Ok(tasks) => {
            let view: Vec<Task> = tasks.into_iter().map(Task::from).collect();
            Json(json!({"data": view})).into_response()
        }
        Err(err) => {
            error!(?err, "listing tasks");
            http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[tracing::instrument(skip_all, ret, fields(?task_id))]
async fn handle_get_task(State(io): State<IO>, Path(task_id): Path<Uuid>) -> impl IntoResponse {
    match tasks::get_by_id(io, task_id).await {
        Ok(Some(task)) => {
            let view = Task::from(task);
            Json(json!({"data": view})).into_response()
        }
        Ok(None) => http::StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(?err, "fetching task by id");
            http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[tracing::instrument(skip_all, ret, fields(?task_id))]
async fn handle_delete_task(State(io): State<IO>, Path(task_id): Path<Uuid>) -> impl IntoResponse {
    match tasks::delete_task_by_id(io, task_id).await {
        Ok(found) => {
            if found {
                http::StatusCode::OK.into_response()
            } else {
                http::StatusCode::NOT_FOUND.into_response()
            }
        }
        Err(err) => {
            error!(?err, "fetching task by id");
            http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

impl TryFrom<&str> for tasks::TaskType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parsed = match value {
            "webhook" => tasks::TaskType::Webhook,
            "hash" => tasks::TaskType::Hash,
            _ => return Err(format!("invalid task type: {value}")),
        };
        Ok(parsed)
    }
}

impl TryFrom<&str> for tasks::TaskState {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parsed = match value {
            "pending" => tasks::TaskState::Pending,
            "completed" => tasks::TaskState::Completed,
            "failed" => tasks::TaskState::Failed,
            _ => return Err(format!("invalid task type: {value}")),
        };
        Ok(parsed)
    }
}

impl From<tasks::TaskState> for String {
    fn from(value: tasks::TaskState) -> Self {
        match value {
            tasks::TaskState::Pending => "pending".to_owned(),
            tasks::TaskState::Completed => "completed".to_owned(),
            tasks::TaskState::Failed => "failed".to_owned(),
        }
    }
}

impl From<tasks::TaskType> for String {
    fn from(value: tasks::TaskType) -> Self {
        match value {
            TaskType::Webhook => "webhook".to_owned(),
            TaskType::Hash => "hash".to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tasks::TaskState;

    use super::*;

    #[test]
    fn test_try_from_str_for_task_type() {
        let cases = [TaskType::Webhook, TaskType::Hash];

        for v in cases {
            assert_eq!(v, TaskType::try_from(String::from(v).as_str()).unwrap())
        }
    }

    #[test]
    fn test_try_from_str_for_task_state() {
        let cases = [TaskState::Pending, TaskState::Completed, TaskState::Failed];

        for v in cases {
            assert_eq!(v, TaskState::try_from(String::from(v).as_str()).unwrap())
        }
    }
}
