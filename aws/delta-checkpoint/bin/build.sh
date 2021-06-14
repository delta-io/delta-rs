#!/bin/bash

RELEASE_PATH="$(pwd)/builder/target/x86_64-unknown-linux-musl/release"
PACKAGE=delta-checkpoint

docker run --rm \
  -v ${PWD}/../..:/home/rust/src \
  -v ${PWD}/builder/registry:/root/.cargo/registry \
  -v ${PWD}/builder/git:/root/.cargo/git \
  -v ${PWD}/builder/target:/home/rust/src/target \
    ekidd/rust-musl-builder cargo build -p $PACKAGE --release

mkdir -p ./target/lambda/$PACKAGE

(cd ./target/lambda && \
  cp $RELEASE_PATH/$PACKAGE ./bootstrap && \
  zip $PACKAGE.zip bootstrap && rm bootstrap)

