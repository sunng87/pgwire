# Client API integration tests

Integration tests for pgwire's **client API** (`pgwire::api::client`,
`pgwire::tokio::PgWireClient`) against a real PostgreSQL server.

Unlike the other suites under `tests-integration/`, which test the pgwire
*server* implementation against real clients, this suite drives the pgwire
*client* against a real PostgreSQL server.

## Run

```bash
./run.sh
```

`run.sh` starts a PostgreSQL 18 container with podman (exposed on
`127.0.0.1:54329`), waits until it accepts connections, runs
`cargo test -p client-api-tests`, and removes the container again. Extra
arguments are forwarded to `cargo test`:

```bash
./run.sh --test cancel           # only the cancellation tests
KEEP_CONTAINER=1 ./run.sh        # leave the container running for iteration
```

To run against an already-running PostgreSQL instead (e.g. a CI service
container or a local install), skip `run.sh` and configure the target with
environment variables:

```bash
PGWIRE_ITEST_HOST=127.0.0.1 \
PGWIRE_ITEST_PORT=5432 \
PGWIRE_ITEST_USER=postgres \
PGWIRE_ITEST_PASSWORD=postgres \
cargo test -p client-api-tests
```

PostgreSQL 18 is required for the protocol negotiation tests
(`connection.rs`): the server must speak protocol 3.2.

## What is covered

| File               | Client APIs exercised                                                                 |
| ------------------ | ------------------------------------------------------------------------------------- |
| `connection.rs`    | `PgWireClient::connect`, `DefaultStartupHandler` (SCRAM against a real server), `Config` conninfo parsing, `ParameterStatus` collection, `BackendKeyData` (`process_id`/`secret_key`), protocol version negotiation (3.0 / 3.2 / 3.9999), auth failures (`28P01`, `3D000`) |
| `simple_query.rs`  | `DefaultSimpleQueryHandler`: row/field metadata, command tags, text-format decoding via `DataRowsReader`/`DataRowDecoder`, multi-statement queries, empty query, `ErrorResponse` propagation and connection recovery, notices |
| `extended_query.rs`| `ExtendedQueryClient`: prepare/describe (statement & portal), bind & execute with parameters, one-shot `query`, text & binary result formats, portal suspension (`max_rows` fetch loop), statement reuse, error recovery |
| `cancel.rs`        | `PgWireClient::cancel`, raw `CancelRequest` on a second connection, wrong-secret-key rejection |
