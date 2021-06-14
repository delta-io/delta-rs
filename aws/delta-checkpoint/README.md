delta-checkpoint
================

Rust lambda function for creating deltalake checkpoints

Example
-------

The package contains an example that can be run on `localstack`:

```bash
./bin/build.sh
docker network create delta-checkpoint
docker-compose up setup
./bin/test-localstack.sh
```

`test-localstack.sh` copies json log entries for delta versions 0-10 to localstack S3. After running the above, view the parquet and last checkpoint files in S3 with:

```
ENDPOINT=http://localhost:4566 aws ls s3://delta-checkpoint/checkpoint-test/_delta_log/ --endpoint-url=$ENDPOINT
```


