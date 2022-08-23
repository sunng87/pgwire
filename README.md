# pgwire

This library implements PostgreSQL Wire Protocol, and provide essential APIs to
write PostgreSQL comptible servers and clients.

This library is a work in progress.

## Usage

### Server

To use `pgwire` in your server application, you will need to implement two key
components: authentication and query processor. For query processing, there are
two kinds of queries: simple and extended. In simple mode, the sql command is
passed to postgresql server as a string. In extended query mode, a sql command
follows `parse`-`bind`-`describe`-`execute` lifecycle.

Examples are provided to demo the very basic usage of `pgwire` in server side:

- `examples/sqlite.rs`: uses a sqlite database at its core and serve it with
  postgresql protocol.
- `examples/server.rs`: demos a server that always returns fixed results.

## License

This library is released under MIT license.
