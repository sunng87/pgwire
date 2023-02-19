#!/bin/bash
set -e

trap "kill 0" EXIT
cd tests-integration

## start test server
pushd test-server
cargo build
./target/debug/test-server &
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
