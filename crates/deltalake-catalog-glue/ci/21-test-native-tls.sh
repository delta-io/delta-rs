#!/bin/sh -xe

echo ">> Running native-tls tests"
cargo test --features native-tls --no-default-features
