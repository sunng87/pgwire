# pgwire

[![CI](https://github.com/sunng87/pgwire/actions/workflows/ci.yml/badge.svg)](https://github.com/sunng87/pgwire/actions/workflows/ci.yml)
[![](https://img.shields.io/crates/v/pgwire)](https://crates.io/crates/pgwire)
[![Docs](https://docs.rs/pgwire/badge.svg)](https://docs.rs/pgwire/latest/pgwire/)


This library implements PostgreSQL Wire Protocol, and provide essential APIs to
write PostgreSQL comptible servers and clients.

This library is a work in progress and in its relatively early stage. There is
no guarantee for API stability. I'm constantly introducing break changes during
this period. And also sorry for lack of API docs, examples may get you familiar
with its usage.

## Status

- [x] Message format
- [x] Backend TCP/TLS server on Tokio
- [ ] Frontend-Backend interaction over TCP
  - [x] SSL Request and Response
  - [ ] Startup
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
  - [ ] Cancel
  - [x] Error and Notice
  - [ ] Copy
- [ ] APIs
  - [ ] Startup APIs
    - [x] Password authentication
    - [ ] Server parameters API, ready but not very good
  - [x] Simple Query API
  - [x] Extended Query API, verification required
    - [ ] Portal API, implemented but not perfect
  - [x] ResultSet builder/encoder API
  - [ ] Query Cancellation API
  - [x] Error and Notice API

## Usage

### Server/Backend

To use `pgwire` in your server application, you will need to implement two key
components: **startup processor*** and **query processor**. For query
processing, there are two kinds of queries: simple and extended. In simple mode,
the sql command is passed to postgresql server as a string. In extended query
mode, a sql command follows `parse`-`bind`-`describe`(optional)-`execute`
lifecycle.

Examples are provided to demo the very basic usage of `pgwire` on server side:

- `examples/sqlite.rs`: uses an in-memory sqlite database at its core and serves
  it with postgresql protocol.
- `examples/gluesql.rs`: uses an in-memory
  [gluesql](https://github.com/gluesql/gluesql) at its core and serves
  it with postgresql protocol.
- `examples/server.rs`: demos a server that always returns fixed results.
- `examples/secure_server.rs`: demos a server with ssl support and always
  returns fixed results.

### Client/Frontend

I think in most case you do not need pgwire to build a postgresql client,
existing postgresql client like
[rust-postgres](https://github.com/sfackler/rust-postgres) should fit your
scenarios. Please rise an issue if there is a scenario.

## Projects using pgwire

* [GreptimeDB](https://github.com/GrepTimeTeam/greptimedb): Cloud-native
  time-series database
* [sqld](https://github.com/libsql/sqld): A server frontend for
  [libSQL](https://github.com/libsql/libsql)
* [risinglight](https://github.com/risinglightdb/risinglight): OLAP database
  system for educational purpose

## License

This library is released under MIT/Apache dual license.
