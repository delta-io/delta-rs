#!/usr/bin/env bash

set -xe

for crate in "core" "mount" "catalog-glue" "catalog-unity" "hdfs" "lakefs" "azure" "aws" "gcp" "deltalake"; do
        echo ">> Dry-run publishing ${crate}"
        (cd crates/${crate} && \
                cargo publish \
                        --allow-dirty \
                        --dry-run)
done;
