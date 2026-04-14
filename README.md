### Dependencies

- Rust
- Cargo
- [sqlx-cli](https://github.com/launchbadge/sqlx/blob/main/sqlx-cli/README.md#enable-building-in-offline-mode-with-query): cargo install sqlx-cli --no-default-features --features rustls,postgres

### Running

- Start postgres

```sh
docker run --name postgres \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=test \
  -p 5432:5432 \
  -d postgres
```

- Create the database

```sh
sqlx database create
```

- Run migrations

```sh
sqlx migrate run
```

- Run the http server
```sh
cargo r
```

### About

The API is listening for requests on 127.0.0.1:8080

- Listing tasks
```sh
curl localhost:8080/tasks
```

- Creating tasks
```sh
curl localhost:8080/tasks -H "Content-Type: application/json" -d '{"task_type": "webhook", "execution_time_at": "2026-mm-dd hh:mm:ss.798275355 UTC", "url": "url", "body": "hello world"}'
```

- Fetching task by id
```
curl localhost:8080/tasks/{id}
```

- Delete task by id
```
curl -XDELETE localhost:8080/tasks/{id}
```

### Notes

#### Starvation

It's possible to starve tasks that are meant to execute in the future by adding a huge number of tasks that will execute before it.

#### Why not use pub(crate) fn

No special reason. It doesn't add much value for a binary project.

#### Why .env is needed for sqlx development

sqlx checks queries at compile time and it looks for an .env file containing `DATABASE_URL`

#### 2 Thread pools

Hash tasks are meant to be more CPU intensive so they are run in a distinct thread pool to avoid blocking [Futures](https://doc.rust-lang.org/std/future/trait.Future.html).

#### Why exactly-once is impossible but exactly-once processing is possible.

This problem can be seen as an instance of the [2 generals problem](https://poorlydefinedbehaviour.github.io/posts/fair_loss_links_and_two_generals/). In general, it's not possible to deliver a message exactly once so assuming we want the webhook to be delivered at some point we need to go with at-least once delivery. The request includes an idempotency key to allow the receiver to deduplicate webhooks it has already received.

#### IO struct

Not everything is in `IO` for convenience. The `IO` struct is meant to be used every time IO needs to be performed (e.g. network request, opening a file), the fields of the struct can be set to mocks during tests if needed.

#### Ulids for better index locality

The `tasks` table uses normal uuids but it could use [ulids](https://github.com/ulid/spec) to avoid random b+tree insertions.

#### tracing::instrument

[tracing](https://docs.rs/tracing/latest/tracing/) is used to collection instrumentation such as logs. It's not wired to anything because I thought sending output to stdout wouldn't be that interesting. In a real project, logs and traces would be sent to an external system to be consumed by people later on.

#### anyhow

The callers don't have special error handling for different error types so having a generic error type with [anyhow](https://github.com/dtolnay/anyhow) is convenient.

#### metrics

No metrics for simplicity. In a real project, there would be several different metrics to collect such as queue depth, processing time and error rate.

#### No pagination

For simplicity there's no pagination. Obviously it is a problem in production since the api may OOM.

#### Different types at each layer

Different types at each layer or having layers closer to IO depend on types from layers that don't do IO allows greater flexibility when changes are required.

#### Timeouts

Almost no timeouts. Real project would required more thought about where to timeout and how to long to wait before timing out.

#### ClaimGuard

Similar to [MutexGuard](https://doc.rust-lang.org/std/sync/struct.MutexGuard.html) but for a database transaction. It is held while the transaction is in flight.

#### FOR UPDATE

The `FOR UPDATE` locks is held during the processing of a batch of tasks. Holding the locks may stop postgres from vacuuming.

#### Why no validation

Time

#### Worker tick could be better but it takes as long as the lowest task (with timeout)

The worker fetches a batch of tasks and executes them all before proceeding which means the amount of time the worker waits before fetching new tasks is the amount of time it takes to execute the lowest task. There are timeouts but workers could process messages as soon as they are available instead.

#### Why no deadletter queue and how it would work

In a real project tasks that failed to be processed would be processed again after some time instead of being processed again immediately. After N failures, the worker would give up on the task and move it to a dead letter queue. There would be alerts tied to the messages being found in the dead letter queue. No DLQ because of time.

#### Why no postgres enums?

sqlx doesn't support enums when compile-time query validation is being used.

#### Testing

There would be integration tests and unit tests but they were not that interesting so I decided to add tests such as `claim_returns_tasks_ordered_by_execution_time`