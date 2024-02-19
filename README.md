# pgwire

[![CI](https://github.com/sunng87/pgwire/actions/workflows/ci.yml/badge.svg)](https://github.com/sunng87/pgwire/actions/workflows/ci.yml)
[![](https://img.shields.io/crates/v/pgwire)](https://crates.io/crates/pgwire)
[![Docs](https://docs.rs/pgwire/badge.svg)](https://docs.rs/pgwire/latest/pgwire/)

Build Postgres compatible access layer for your data service.

This library implements PostgreSQL Wire Protocol, and provide essential APIs to
write PostgreSQL comptible servers and clients.

This library is a work in progress and in its relatively early stage. There is
no guarantee for API stability. I'm constantly introducing break changes during
this period. And also sorry for lack of API docs, examples may get you familiar
with its usage.

## Status

- [x] Message format
  - [x] Frontend-Backend protocol messages
  - [ ] Logical replication streaming protocol message
- [x] Backend TCP/TLS server on Tokio
- [x] Frontend-Backend interaction over TCP
  - [x] SSL Request and Response
  - [x] Startup
    - [x] No authentication
    - [x] Clear-text password authentication
    - [x] Md5 Password authentication
    - [x] SASL SCRAM authentication
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
- [ ] Logical replication over TCP
- [ ] APIs
  - [x] Startup APIs
    - [x] AuthSource API, fetching and hashing passwords
    - [x] Server parameters API, ready but not very good
  - [x] Simple Query API
  - [x] Extended Query API
    - [x] QueryParser API, for transforming prepared statement
    - [x] PortalStore API, for caching statements and portals
  - [x] ResultSet builder/encoder API
  - [ ] Query Cancellation API
  - [x] Error and Notice API
  - [ ] Copy API
    - [ ] Copy-in
    - [ ] Copy-out
    - [ ] Copy-both
  - [ ] Logical replication server API

## About Postgres Wire Protocol

Postgres Wire Protocol is a relatively general-purpose Layer-7 protocol. There
are 3 parts of the protocol:

- Startup: client-server handshake and authentication.
- Simple Query: The text-based query protocol of postgresql. Query are provided
  as string, and server is allowed to stream data in response.
- Extended Query: A new sub-protocol for query which has ability to cache the
  query on server-side and reuse it with new parameters. The response part is
  identical to Simple Query.

Also note that Postgres Wire Protocol has no semantics about SQL, so literally
you can use any query language, data formats or even natural language to
interact with the backend.

The response are always encoded as data row format. And there is a field
description as header of the data to describe its name, type and format.


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
  extended query implementation.
- `examples/duckdb.rs`: similar to sqlite example but with duckdb backend. Note
  that not all data types are implemented in this example.
- `examples/gluesql.rs`: uses an in-memory
  [gluesql](https://github.com/gluesql/gluesql) at its core and serves
  it with postgresql protocol.
- `examples/server.rs`: demos a server that always returns fixed results.
- `examples/secure_server.rs`: demos a server with ssl support and always
  returns fixed results.
- `examples/scram.rs`: demos how to configure more secure authentication
  mechanism:
  [SCRAM](https://en.wikipedia.org/wiki/Salted_Challenge_Response_Authentication_Mechanism)
- `examples/datafusion.rs`: demos a postgres compatible server backed by
  datafusion query engine. This example allows you to `LOAD` csv files as
  datafusion table and run `SELECT` queries on them.

### Client/Frontend

I think in most case you do not need pgwire to build a postgresql client,
existing postgresql client like
[rust-postgres](https://github.com/sfackler/rust-postgres) should fit your
scenarios. Please rise an issue if there is a scenario.

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

## License

This library is released under MIT/Apache dual license.
