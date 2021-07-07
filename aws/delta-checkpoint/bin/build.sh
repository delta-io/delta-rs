#!/bin/bash

set -eu

RELEASE_PATH="$PWD/builder/target/x86_64-unknown-linux-musl/release"
PACKAGE=lambda-delta-checkpoint

mkdir -p "$PWD/builder/registry" \
  "$PWD/builder/git" \
  "$PWD/builder/target"

docker run --rm \
  -v "$PWD/../..":/home/rust/src \
  -v "$PWD/builder/registry":/root/.cargo/registry \
  -v "$PWD/builder/git":/root/.cargo/git \
  -v "$PWD/builder/target":/home/rust/src/target \
    ekidd/rust-musl-builder cargo build -p $PACKAGE --release

zip -j lambda-delta-checkpoint.zip ./builder/target/x86_64-unknown-linux-musl/release/bootstrap

