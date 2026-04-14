CREATE TABLE tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_type CHAR NOT NULL,
    state CHAR NOT NULL,
    url TEXT,
    body BYTEA,
    secret BYTEA,
    execution_time_at timestamptz NOT NULL
);

CREATE INDEX idx_tasks_execution_time_at ON tasks (state, execution_time_at) WHERE tasks.state = '1';
CREATE INDEX idx_tasks_state ON tasks (state);