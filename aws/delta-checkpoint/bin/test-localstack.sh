#!/bin/bash

set -eu

AWS_DEFAULT_REGION=us-east-2
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
ENDPOINT=http://localhost:4566
TEST_DATA_PATH=../../rust/tests/data/checkpoints/_delta_log/

aws s3 cp $TEST_DATA_PATH s3://delta-checkpoint/checkpoint-test/_delta_log/ \
  --recursive \
  --exclude "*" \
  --include "*.json" \
  --endpoint-url=$ENDPOINT

sleep 5

aws s3 ls s3://delta-checkpoint/checkpoint-test/_delta_log/ --endpoint-url=$ENDPOINT


