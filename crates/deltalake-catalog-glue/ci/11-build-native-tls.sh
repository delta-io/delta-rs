#!/bin/sh -xe

echo ">> Running native-tls build"
cargo build --features native-tls --no-default-features
