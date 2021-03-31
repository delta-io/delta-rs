#!/bin/sh

export AWS_DEFAULT_REGION=us-east-2
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export ENDPOINT=http://localstack:4566

timeout=120 #seconds
while [ "$(curl -s $ENDPOINT/health | jq '.services.s3')" != "\"running\"" ]; do
  if [ "$timeout" -lt "0" ]; then
    echo 'S3 is offline for more than 2 minutes';
    exit 1;
  fi
  echo 'waiting on S3 to start...'
  sleep 5
  timeout=$((timeout - 5))
done

echo 'S3 is running'

aws s3api create-bucket --bucket deltars --endpoint-url=$ENDPOINT
aws s3 sync /data/golden s3://deltars/golden/ --endpoint-url=$ENDPOINT
aws s3 sync /data/simple s3://deltars/simple/ --endpoint-url=$ENDPOINT

echo localstack is configured!
