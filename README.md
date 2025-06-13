# pgwire

[![CI](https://github.com/sunng87/pgwire/actions/workflows/ci.yml/badge.svg)](https://github.com/sunng87/pgwire/actions/workflows/ci.yml)
[![](https://img.shields.io/crates/v/pgwire)](https://crates.io/crates/pgwire)
[![Docs](https://docs.rs/pgwire/badge.svg)](https://docs.rs/pgwire/latest/pgwire/)

Build Postgres compatible access layer for your data service.

This library implements PostgreSQL Wire Protocol, and provide essential APIs to
write PostgreSQL compatible servers and clients. It's like
[hyper](https://github.com/hyperium/hyper/), but for postgres wire protocol.

If you are interested in related topic, you can check [project
ideas](https://github.com/sunng87/pgwire/discussions/204) to build on top of
this library.


## Status

- Message format
  - [x] Frontend-Backend protocol messages
  - [ ] Streaming replication protocol
  - [ ] Logical streaming replication protocol message
- [x] Backend TCP/TLS server on Tokio
- [ ] Frontend TCP/TLS client on Tokio
- Frontend-Backend interaction over TCP
  - [x] SSL Request and Response
    - [x] PostgreSQL 17 direct SSL negotiation
  - [x] Startup
    - [x] No authentication
    - [x] Clear-text password authentication
    - [x] Md5 Password authentication
    - [x] SASL SCRAM authentication (optional feature `server-api-scram-ring` or
          `server-api-scram-aws-lc-rs`)
      - [x] SCRAM-SHA-256
      - [x] SCRAM-SHA-256-PLUS
  - [x] Simple Query and Response
  - [x] Extended Query and Response
    - [x] Parse
    - [x] Bind
    - [x] Execute
    - [x] Describe
    - [x] Sync
  - [x] Termination
  - [x] Cancel
  - [x] Error and Notice
  - [x] Copy
  - [x] Notification
- [ ] Streaming replication over TCP
- [ ] Logical streaming replication over TCP
- [x] Data types
  - [x] Text format
  - [x] Binary format, implemented in `postgres-types`
- APIs
  - Backend/Server
    - [x] Startup APIs
      - [x] AuthSource API, fetching and hashing passwords
      - [x] Server parameters API, ready but not very good
    - [x] Simple Query API
    - [x] Extended Query API
      - [x] QueryParser API, for transforming prepared statement
    - [x] ResultSet builder/encoder API
    - [x] Query Cancellation API
    - [x] Error and Notice API
    - [x] Copy API
      - [x] Copy-in
      - [x] Copy-out
      - [x] Copy-both
    - [x] Transaction state
    - [ ] Streaming replication over TCP
    - [ ] Logical streaming replication server API
  - Frontend/Client
    - [x] Startup APIs
    - [x] Simple Query API
    - [ ] Extended Query API
    - [ ] ResultSet decoder API
    - [ ] Query Cancellation API
    - [ ] Error and Notice API
    - [ ] Copy API
    - [ ] Transaction state
    - [ ] Streaming replication over TCP
    - [ ] Logical streaming replication server API

## About Postgres Wire Protocol

Postgres Wire Protocol is a relatively general-purpose Layer-7 protocol. There
are 6 parts of the protocol:

- Startup: client-server handshake and authentication.
- Simple Query: The text-based query protocol of postgresql. Query are provided
  as string, and server is allowed to stream data in response.
- Extended Query: A new sub-protocol for query which has ability to cache the
  query on server-side and reuse it with new parameters. The response part is
  identical to Simple Query.
- Copy: the subprotocol to copy data from and to postgresql.
- Replication
- Logical Replication

Also note that Postgres Wire Protocol has no semantics about SQL, so literally
you can use any query language, data formats or even natural language to
interact with the backend.

The response are always encoded as data row format. And there is a field
description as header of the data to describe its name, type and format.

[Jelte Fennema-Nio](https://github.com/JelteF)'s on talk on PgConf.dev 2024 has
a great coverage of how the wire protocol works:
https://www.youtube.com/watch?v=nh62VgNj6hY

## Usage

### Server/Backend

To use `pgwire` in your server application, you will need to implement two key
components: **startup processor** and **query processor**. For query
processing, there are two kinds of queries: simple and extended. By adding
`SimpleQueryHandler` to your application, you will get `psql` command-line tool
compatibility. And for more language drivers and additional prepared statement,
binary encoding support, `ExtendedQueryHandler` is required.

Examples are provided to demo the very basic usage of `pgwire` on server side:

- `examples/sqlite.rs`: uses an in-memory sqlite database at its core and serves
  it with postgresql protocol. This is a full example with both simple and
  extended query implementation. `cargo run --features _sqlite --example
  sqlite`
- `examples/duckdb.rs`: similar to sqlite example but with duckdb backend. Note
  that not all data types are implemented in this example. `cargo run --features
  _duckdb --example duckdb`
- `examples/gluesql.rs`: uses an in-memory
  [gluesql](https://github.com/gluesql/gluesql) at its core and serves
  it with postgresql protocol.
- `examples/server.rs`: demos a server that always returns fixed results.
- `examples/secure_server.rs`: demos a server with ssl support and always
  returns fixed results.
- `examples/scram.rs`: demos how to configure more secure authentication
  mechanism:
  [SCRAM](https://en.wikipedia.org/wiki/Salted_Challenge_Response_Authentication_Mechanism)
- `examples/transaction.rs`: see how to control transaction state at wire
  protocol level.
- `examples/datafusion.rs`: Now moved to
  [datafusion-postgres](https://github.com/sunng87/datafusion-postgres)

### Client/Frontend

The client/frontend API is progress of development. This API will focus on
providing full access of postgres wire protocol. It's designed to build
components like postgres proxy. For general purpose postgres driver for
application development, you can use
[rust-postgres](https://github.com/sfackler/rust-postgres).

## Projects using pgwire

* [GreptimeDB](https://github.com/GrepTimeTeam/greptimedb): Cloud-native
  time-series database
* [risinglight](https://github.com/risinglightdb/risinglight): OLAP database
  system for educational purpose
* [PeerDB](https://github.com/PeerDB-io/peerdb) Postgres first ETL/ELT, enabling
  10x faster data movement in and out of Postgres
* [CeresDB](https://github.com/CeresDB/ceresdb) CeresDB is a high-performance,
  distributed, cloud native time-series database from AntGroup.
* [dozer](https://github.com/getdozer/dozer) a real-time data platform for
  building, deploying and maintaining data products.
* [restate](https://github.com/restatedev/restate) Framework for building
  resilient workflow

Submit a pull request if your project isn't listed here.

## License

This library is released under MIT/Apache dual license.
