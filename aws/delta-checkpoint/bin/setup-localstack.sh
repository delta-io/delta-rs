#!/bin/bash

set -eu

export AWS_DEFAULT_REGION=us-east-2
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export ENDPOINT=http://localstack:4566

LAMBDA_ROLE="lambda-delta-checkpoint"

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

echo "Creating S3 bucket."

aws s3api create-bucket --endpoint-url=$ENDPOINT \
  --bucket delta-checkpoint

echo "Creating Lambda."

aws iam create-role --endpoint-url=$ENDPOINT \
  --role-name $LAMBDA_ROLE \
  --assume-role-policy-document '{"Version": "2012-10-17","Statement": [{ "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}'

aws iam attach-role-policy --endpoint-url=$ENDPOINT \
  --role-name $LAMBDA_ROLE \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

aws lambda create-function --endpoint-url=$ENDPOINT \
  --function-name deltaCheckpoint \
  --handler delta.checkpoint \
  --zip-file fileb:///lambda-delta-checkpoint.zip \
  --runtime provided.al2 \
  --role $LAMBDA_ROLE \
  --environment "Variables={RUST_LOG=debug,RUST_BACKTRACE=1,AWS_REGION=$AWS_DEFAULT_REGION,AWS_ENDPOINT_URL=$ENDPOINT,AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY}" \

lambda_arn=$(aws lambda get-function --function-name deltaCheckpoint --endpoint $ENDPOINT --query Configuration.FunctionArn)

# echo "Creating bucket notification configuration."

notification_config=`cat <<EOF
{
  "LambdaFunctionConfigurations": [
    {
      "Id": "delta-checkpoint-notification",
      "LambdaFunctionArn": ${lambda_arn},
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            {
              "Name": "prefix",
              "Value": "checkpoint-test/_delta_log/"
            },
            {
              "Name": "suffix",
              "Value": "0.json"
            }
          ]
        }
      }
    }
  ]
}
EOF
`

aws s3api put-bucket-notification-configuration --endpoint-url=$ENDPOINT \
  --bucket delta-checkpoint \
  --notification-configuration "$notification_config"

echo "All done!"

