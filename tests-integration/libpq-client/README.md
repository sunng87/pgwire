# libpq-client

A simple PostgreSQL client built using libpq.

## Building

Prerequisites:
- libpq development library (e.g., `libpq-dev` on Debian/Ubuntu, `postgresql-devel` on CentOS/RHEL)

Build:
```bash
make
```

Clean:
```bash
make clean
```

## Usage

Run with default connection string:
```bash
./client
```

Run with custom connection string:
```bash
./client "host=localhost port=5432 dbname=postgres"
```

The client will connect to the PostgreSQL server and execute `SELECT version()` to test the connection.
