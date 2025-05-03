#!/usr/bin/env bash

set -xe

for crate in "derive" "core" "mount" "catalog-glue" "aws" "azure" "gcp" "catalog-unity" "hdfs" "lakefs" "deltalake"; do
        echo ">> Dry-run publishing ${crate}"
        (cd crates/${crate} && \
                cargo publish \
                        --allow-dirty \
                        --dry-run)
done;
