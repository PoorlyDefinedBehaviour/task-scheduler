use crate::{
    contracts::{self, ClaimGuard},
    tasks::{self, CreateTaskInput, ListInput, Task, TaskState, TaskType},
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::stream::TryStreamExt;
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use uuid::Uuid;

const TASK_TYPE_WEBHOOK: &str = "1";
const TASK_TYPE_HASH: &str = "2";

const TASK_STATE_PENDING: &str = "1";
const TASK_STATE_COMPLETED: &str = "2";
const TASK_STATE_FAILED: &str = "3";

pub struct PostgresStorage {
    pool: Pool<Postgres>,
}

impl PostgresStorage {
    #[tracing::instrument(skip_all, err)]
    pub async fn new() -> Result<Self> {
        let pool = PgPoolOptions::new()
            // In a real project configuration would come from env variables or command line arguments.
            .max_connections(16)
            .connect("postgres://postgres:password@localhost/test")
            .await
            .context("connecting to postres")?;
        Ok(Self { pool })
    }
}

struct Row<'a> {
    task_type: &'a str,
    state: &'a str,
    execution_time_at: &'a chrono::DateTime<Utc>,

    // Some when task is a webhook task.
    url: Option<&'a str>,
    body: Option<&'a [u8]>,

    // Some when task is a hash task.
    secret: Option<&'a [u8]>,
}

#[async_trait]
impl contracts::Storage for PostgresStorage {
    #[tracing::instrument(skip_all, ret, err, fields(?input))]
    async fn create(&self, input: &CreateTaskInput) -> Result<Uuid> {
        let row = match input {
            tasks::CreateTaskInput::Webhook {
                url,
                body,
                execution_time_at,
            } => Row {
                task_type: TASK_TYPE_WEBHOOK,
                state: TASK_STATE_PENDING,
                url: Some(url),
                body: Some(body),
                secret: None,
                execution_time_at,
            },
            tasks::CreateTaskInput::Hash {
                secret,
                execution_time_at,
            } => Row {
                task_type: TASK_TYPE_HASH,
                state: TASK_STATE_PENDING,
                url: None,
                body: None,
                secret: Some(secret),
                execution_time_at,
            },
        };

        let result = sqlx::query!(
            "INSERT INTO tasks (task_type, state, url, body, secret, execution_time_at) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
            row.task_type,
            row.state,
            row.url,
            row.body,
            row.secret,
            row.execution_time_at
        ).fetch_one(&self.pool).await.context("inserting into tasks table")?;

        Ok(result.id)
    }

    #[tracing::instrument(skip_all, ret, err, fields(?input))]
    async fn list(&self, input: &ListInput) -> Result<Vec<Task>> {
        let mut result = sqlx::query!(
            "
            SELECT id, task_type, state, url, body, secret, execution_time_at FROM tasks 
            WHERE 
                ($1::TEXT IS NULL OR state = $1) AND
                ($2::TEXT IS NULL OR task_type = $2)
        ",
            input.state.map(|s| match s {
                tasks::TaskState::Pending => TASK_STATE_PENDING,
                tasks::TaskState::Completed => TASK_STATE_COMPLETED,
                tasks::TaskState::Failed => TASK_STATE_FAILED,
            }),
            input.task_type.map(|t| match t {
                tasks::TaskType::Webhook => TASK_TYPE_WEBHOOK,
                tasks::TaskType::Hash => TASK_TYPE_HASH,
            }),
        )
        .fetch(&self.pool);

        let mut tasks = Vec::new();

        while let Some(row) = result
            .try_next()
            .await
            .context("reading row from database")?
        {
            tasks.push(match parse_task_type(&row.task_type)? {
                TaskType::Webhook => Task::Webhook {
                    id: row.id,
                    url: row.url.unwrap(),
                    body: row.body.unwrap(),
                    state: parse_task_state(&row.state)?,
                    execution_time_at: row.execution_time_at,
                },
                TaskType::Hash => Task::Hash {
                    id: row.id,
                    secret: row.secret.unwrap(),
                    state: parse_task_state(&row.state)?,
                    execution_time_at: row.execution_time_at,
                },
            });
        }

        Ok(tasks)
    }

    #[tracing::instrument(skip_all, ret, err, fields(?task_id))]
    async fn get_by_id(&self, task_id: &Uuid) -> Result<Option<Task>> {
        let record = sqlx::query!("SELECT id, task_type, state, url, body, secret, execution_time_at FROM tasks WHERE id = $1", task_id)
            .fetch_optional(&self.pool).await.context("selecting task by id")?;

        let Some(record) = record else {
            return Ok(None);
        };
        let task = match parse_task_type(&record.task_type)? {
            TaskType::Webhook => Task::Webhook {
                id: record.id,
                url: record.url.unwrap(),
                body: record.body.unwrap(),
                state: parse_task_state(&record.state)?,
                execution_time_at: record.execution_time_at,
            },
            TaskType::Hash => Task::Hash {
                id: record.id,
                secret: record.secret.unwrap(),
                state: parse_task_state(&record.state)?,
                execution_time_at: record.execution_time_at,
            },
        };
        Ok(Some(task))
    }

    #[tracing::instrument(skip_all, ret, err, fields(?task_id))]
    async fn delete_by_id(&self, task_id: &Uuid) -> Result<bool> {
        let row = sqlx::query!("DELETE FROM tasks WHERE id = $1 RETURNING id", task_id)
            .fetch_optional(&self.pool)
            .await
            .context("deleting task from tasks table")?;
        Ok(row.is_some())
    }

    #[tracing::instrument(skip_all, err, fields(?max_batch_size, ?now))]
    async fn claim(
        &self,
        max_batch_size: u16,
        now: DateTime<Utc>,
    ) -> Result<(ClaimGuard, Vec<Task>)> {
        let mut tx = self.pool.begin().await.context("starting transaction")?;

        let tasks = {
            let mut result = sqlx::query!(
                "
            SELECT id, task_type, state, url, body, secret, execution_time_at
            FROM tasks
            WHERE 
                state = $1 AND
                execution_time_at <= $2
            ORDER BY execution_time_at ASC
            LIMIT $3
            FOR UPDATE SKIP LOCKED
        ",
                TASK_STATE_PENDING,
                now,
                i64::try_from(max_batch_size)?
            )
            .fetch(&mut *tx);

            let mut tasks = Vec::new();

            while let Some(row) = result
                .try_next()
                .await
                .context("reading row from database")?
            {
                tasks.push(match parse_task_type(&row.task_type)? {
                    TaskType::Webhook => Task::Webhook {
                        id: row.id,
                        url: row.url.unwrap(),
                        body: row.body.unwrap(),
                        state: parse_task_state(&row.state)?,
                        execution_time_at: row.execution_time_at,
                    },
                    TaskType::Hash => Task::Hash {
                        id: row.id,
                        secret: row.secret.unwrap(),
                        state: parse_task_state(&row.state)?,
                        execution_time_at: row.execution_time_at,
                    },
                });
            }

            tasks
        };

        Ok((ClaimGuard { tx }, tasks))
    }

    #[tracing::instrument(skip_all, ret, err, fields(?task_ids))]
    async fn complete(&self, guard: &mut ClaimGuard, task_ids: &[Uuid]) -> Result<()> {
        let _ = sqlx::query!(
            "UPDATE tasks SET state = $1 WHERE id = ANY($2::uuid[])",
            TASK_STATE_COMPLETED,
            task_ids
        )
        .execute(&mut *guard.tx)
        .await
        .context("updating tasks table to set task state to completed")?;

        Ok(())
    }
}

impl<'a> ClaimGuard<'a> {
    pub async fn release(self) -> Result<()> {
        self.tx.commit().await.context("committing transaction")
    }
}

#[tracing::instrument(skip_all, ret, err, fields(?s))]
fn parse_task_state(s: &str) -> Result<TaskState> {
    let parsed = match s {
        TASK_STATE_PENDING => TaskState::Pending,
        TASK_STATE_COMPLETED => TaskState::Completed,
        TASK_STATE_FAILED => TaskState::Failed,
        _ => anyhow::bail!("invalid task state found: state={s}"),
    };
    Ok(parsed)
}

#[tracing::instrument(skip_all, ret, err, fields(?t))]
fn parse_task_type(t: &str) -> Result<TaskType> {
    let parsed = match t {
        TASK_TYPE_WEBHOOK => TaskType::Webhook,
        TASK_TYPE_HASH => TaskType::Hash,
        _ => anyhow::bail!("invalid task type found: type={t}"),
    };
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::contracts::Storage;

    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;

    impl Arbitrary for TaskType {
        type Parameters = ();
        type Strategy = BoxedStrategy<TaskType>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            any::<bool>()
                .prop_map(|b| if b { TaskType::Webhook } else { TaskType::Hash })
                .boxed()
        }
    }

    impl Arbitrary for CreateTaskInput {
        type Parameters = ();
        type Strategy = BoxedStrategy<CreateTaskInput>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            let now = Utc::now();
            (any::<TaskType>(), any::<u8>())
                .prop_flat_map(move |(task_type, time_diff)| {
                    let execution_time_at = if time_diff % 2 == 0 {
                        now + Duration::from_secs(time_diff as u64)
                    } else {
                        now - Duration::from_secs(time_diff as u64)
                    };

                    match task_type {
                        TaskType::Webhook => (any::<String>(), any::<Vec<u8>>())
                            .prop_map(move |(url, body)| CreateTaskInput::Webhook {
                                url,
                                body,
                                execution_time_at,
                            })
                            .boxed(),
                        TaskType::Hash => any::<Vec<u8>>()
                            .prop_map(move |secret| CreateTaskInput::Hash {
                                secret,
                                execution_time_at,
                            })
                            .boxed(),
                    }
                })
                .boxed()
        }
    }

    #[test]
    fn test_parse_task_type() {
        let cases = [
            (TASK_TYPE_WEBHOOK, TaskType::Webhook),
            (TASK_TYPE_HASH, TaskType::Hash),
        ];

        for (input, expected) in cases {
            assert_eq!(expected, parse_task_type(input).unwrap());
        }
    }

    #[test]
    fn test_parse_task_state() {
        let cases = [
            (TASK_STATE_PENDING, TaskState::Pending),
            (TASK_STATE_COMPLETED, TaskState::Completed),
            (TASK_STATE_FAILED, TaskState::Failed),
        ];

        for (input, expected) in cases {
            assert_eq!(expected, parse_task_state(input).unwrap());
        }
    }

    fn execution_time(task: &Task) -> DateTime<Utc> {
        match task {
            Task::Webhook {
                execution_time_at, ..
            } => *execution_time_at,
            Task::Hash {
                execution_time_at, ..
            } => *execution_time_at,
        }
    }

    fn task_state(task: &Task) -> TaskState {
        match task {
            Task::Webhook { state, .. } => *state,
            Task::Hash { state, .. } => *state,
        }
    }

    #[derive(Debug, proptest_derive::Arbitrary)]
    enum Op {
        Create(CreateTaskInput),
        List,
        Get,
        Delete,
    }

    proptest! {
        #[test]
        fn basic_actions(ops in vec(any::<Op>(), 1..=10)) {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let s = PostgresStorage::new().await.unwrap();
                    for op in ops {
                        match op {
                            Op::Create(input) => {
                                s.create(&input).await.unwrap();
                            },
                            Op::List => {
                                s.list(&ListInput{state:None,task_type:None}).await.unwrap();
                            },
                            Op::Get => {
                                // s.get_by_id()
                            },
                            Op::Delete => {
                                // s.delete_by_id(task_id)
                            }
                        }
                    }
                })
        }

        #[test]
        fn claim_returns_tasks_that_are_scheduled_for_now_or_in_the_past(inputs in vec(any::<CreateTaskInput>(), 0..=10), max_batch_size in any::<u16>()) {
             tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let s = PostgresStorage::new().await.unwrap();
                    for input in inputs {
                        s.create(&input).await.unwrap();
                    }
                    let now = Utc::now();
                    let (_, tasks) = s.claim(max_batch_size, now).await.unwrap();
                    for task in tasks {
                        assert!(execution_time(&task) <= now);
                    }
                })
        }

        #[test]
        fn claim_returns_tasks_ordered_by_execution_time(inputs in vec(any::<CreateTaskInput>(), 0..=10), max_batch_size in any::<u16>()) {
             tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let s = PostgresStorage::new().await.unwrap();
                    for input in inputs {
                        s.create(&input).await.unwrap();
                    }
                    let now = Utc::now();
                    let (_, tasks) = s.claim(max_batch_size, now).await.unwrap();
                    for i in 0..tasks.len() {
                        let j = std::cmp::min(i + 1, tasks.len() - 1);
                        assert!(execution_time(&tasks[i]) <= execution_time(&tasks[j]));
                    }
                })
        }

        #[test]
        fn all_claimed_tasks_have_pending_state(inputs in vec(any::<CreateTaskInput>(), 0..=10), max_batch_size in any::<u16>()) {
             tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let s = PostgresStorage::new().await.unwrap();
                    for input in inputs {
                        s.create(&input).await.unwrap();
                    }
                    let now = Utc::now();
                    let (_, tasks) = s.claim(max_batch_size, now).await.unwrap();
                    for task in tasks {
                        assert_eq!(TaskState::Pending, task_state(&task));
                    }
                })
        }
    }
}
