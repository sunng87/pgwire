# pgwire

[![CI](https://github.com/sunng87/pgwire/actions/workflows/ci.yml/badge.svg)](https://github.com/sunng87/pgwire/actions/workflows/ci.yml)
[![](https://img.shields.io/crates/v/pgwire)](https://crates.io/crates/pgwire)
[![Docs](https://docs.rs/pgwire/badge.svg)](https://docs.rs/pgwire/latest/pgwire/)


This library implements PostgreSQL Wire Protocol, and provide essential APIs to
write PostgreSQL comptible servers and clients.

This library is a work in progress.

## Usage

### Server

To use `pgwire` in your server application, you will need to implement two key
components: startup processor and query processor. For query processing, there
are two kinds of queries: simple and extended. In simple mode, the sql command
is passed to postgresql server as a string. In extended query mode, a sql
command follows `parse`-`bind`-`describe`(optional)-`execute` lifecycle.

Examples are provided to demo the very basic usage of `pgwire` on server side:

- `examples/sqlite.rs`: uses an in-memory sqlite database at its core and serves
  it with postgresql protocol.
- `examples/server.rs`: demos a server that always returns fixed results.
- `examples/secure_server.rs`: demos a server with ssl support and always
  returns fixed results.

## License

This library is released under MIT/Apache dual license.
