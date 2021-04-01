#!/bin/bash

export AWS_DEFAULT_REGION=us-east-2
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export ENDPOINT=http://localstack:4566

retries=10 #seconds
until aws s3api list-buckets --endpoint-url=$ENDPOINT > /dev/null 2>&1
do
  if [ "$retries" -lt "0" ]; then
    echo 'S3 is still offline after 10 retries';
    exit 1;
  fi
  echo 'waiting on S3 to start...'
  sleep 5
  retries=$((retries - 1))
done

echo 'S3 is running'

aws s3api create-bucket --bucket deltars --endpoint-url=$ENDPOINT
aws s3 sync /data/golden s3://deltars/golden/ --endpoint-url=$ENDPOINT
aws s3 sync /data/simple s3://deltars/simple/ --endpoint-url=$ENDPOINT

echo localstack is configured!
