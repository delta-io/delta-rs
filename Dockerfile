FROM --platform=linux/amd64 ghcr.io/pyo3/maturin

ARG CARGO_VERSION=1.81.0
ENV CARGO_VERSION=$CARGO_VERSION

VOLUME /io

WORKDIR /

# Solve openssl issue: https://github.com/sfackler/rust-openssl/issues/1550
RUN yum update -y && yum install -y perl-core openssl openssl-devel python3-devel

RUN rustup target add x86_64-unknown-linux-gnu

RUN pip install ziglang

# Solves https://github.com/time-rs/time/issues/693
RUN rustup install $CARGO_VERSION && rustup default $CARGO_VERSION

# To build, run in delta-rs root:
# docker build --tag delta-maturin --build-arg TARGETARCH=amd64 --platform linux/amd64 .
# docker run --rm -v $(pwd):/io delta-maturin build -m /io/python/Cargo.toml --zig --release --target x86_64-unknown-linux-gnu --compatibility manylinux2014
ENTRYPOINT ["/usr/bin/maturin"]
