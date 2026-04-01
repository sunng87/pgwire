#!/bin/bash
set -e

cd tests-integration

## start test server
pushd test-server
cargo build
../../target/debug/test-server &
popd

## run rust-client
pushd rust-client
cargo run
popd

## run python-clients
pushd python
python client2.py
python client3.py
popd

### jdbc
pushd jdbc
bb test.bb
jbang test.java
popd

### node
pushd nodejs
npm install
npm run test
popd

### golang
pushd go
go run client.go
popd

### c, libpq
pushd libpq-client/
make
./client
popd
