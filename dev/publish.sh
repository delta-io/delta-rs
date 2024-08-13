#!/usr/bin/env bash

set -xe

for crate in "mount" "catalog-glue" "hdfs" "azure" "aws" "gcp" "core" "deltalake"; do
        echo ">> Dry-run publishing ${crate}"
        (cd crates/${crate} && \
                cargo publish \
                        --allow-dirty \
                        --dry-run)
done;
