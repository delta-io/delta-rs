#!/bin/bash

export AWS_DEFAULT_REGION=us-east-2
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export ENDPOINT=http://localstack:4566


function wait_for() {
  retries=10
  until eval $2 > /dev/null 2>&1
  do
    if [ "$retries" -lt "0" ]; then
      echo "$1 is still offline after 10 retries";
      exit 1;
    fi
    echo "Waiting on $1 to start..."
    sleep 5
    retries=$((retries - 1))
  done
}

wait_for "S3" "aws s3api list-buckets --endpoint-url=$ENDPOINT"

echo "Uploading S3 test delta tables..."
aws s3api create-bucket --bucket deltars --endpoint-url=$ENDPOINT > /dev/null 2>&1
aws s3 sync /data/golden s3://deltars/golden/ --delete --endpoint-url=$ENDPOINT > /dev/null
aws s3 sync /data/simple_table s3://deltars/simple/ --delete --endpoint-url=$ENDPOINT > /dev/null
aws s3 sync /data/simple_commit s3://deltars/simple_commit_rw/ --delete --endpoint-url=$ENDPOINT > /dev/null
aws s3 sync /data/concurrent_workers s3://deltars/concurrent_workers/ --delete --endpoint-url=$ENDPOINT > /dev/null

wait_for "DynamoDB" "aws dynamodb list-tables --endpoint-url=$ENDPOINT"

echo "Creating DynamoDB test lock table..."
aws dynamodb delete-table --table-name test_table --endpoint-url=$ENDPOINT > /dev/null 2>&1
aws dynamodb create-table --table-name test_table --endpoint-url=$ENDPOINT \
    --attribute-definitions \
        AttributeName=key,AttributeType=S \
    --key-schema \
        AttributeName=key,KeyType=HASH \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=10 > /dev/null

echo Localstack is configured!
