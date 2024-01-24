"""Test concurrent writes between pyspark and deltalake(delta-rs)."""

import concurrent.futures as ft
import os
import subprocess
import time
import uuid

import pandas as pd
import pyspark
import pytest

from deltalake import DeltaTable, write_deltalake
from tests.pyspark_integration.utils import get_spark


def configure_spark_session_for_s3(
    spark: pyspark.sql.SparkSession, lock_table_name: str
):
    spark.conf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.profile.ProfileCredentialsProvider",
    )
    spark.conf.set(
        "spark.delta.logStore.s3a.impl", "io.delta.storage.S3DynamoDBLogStore"
    )
    spark.conf.set(
        "spark.io.delta.storage.S3DynamoDBLogStore.ddb.region", "eu-central-1"
    )
    spark.conf.set(
        "spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName", lock_table_name
    )
    return spark.newSession()


def written_by_spark(filename: str) -> bool:
    return filename.startswith("part-00000-")


def run_spark_insert(table_uri: str, lock_table_name: str, run_until: float) -> int:
    region = os.environ.get("AWS_REGION", "us-east-1")
    spark = get_spark()
    spark.conf.set(
        "spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName", lock_table_name
    )
    spark.conf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.profile.ProfileCredentialsProvider",
    )
    spark.conf.set("spark.io.delta.storage.S3DynamoDBLogStore.ddb.region", region)
    row = 0
    while True:
        row += 1
        spark.createDataFrame(
            [(row, "created by spark")], schema=["row", "creator"]
        ).coalesce(1).write.save(
            table_uri,
            mode="append",
            format="delta",
        )
        if time.time() >= run_until:
            break
    return row


def run_delta_rs_insert(dt: DeltaTable, run_until: float) -> int:
    row = 0
    while True:
        row += 1
        df = pd.DataFrame({"row": [row], "creator": ["created by delta-rs"]})
        write_deltalake(dt, df, schema=dt.schema(), mode="append")
        if time.time() > run_until:
            break
    return row


@pytest.mark.pyspark
@pytest.mark.integration
def test_concurrent_writes_pyspark_delta(setup):
    run_duration = 30

    (table_uri, table_name) = setup
    # this is to create the table, in a non-concurrent way
    initial_insert = run_spark_insert(table_uri, table_name, time.time())
    assert initial_insert == 1

    os.environ["AWS_S3_LOCKING_PROVIDER"] = "dynamodb"
    os.environ["DELTA_DYNAMO_TABLE_NAME"] = table_name
    dt = DeltaTable(table_uri)
    with ft.ThreadPoolExecutor(max_workers=2) as executor:
        spark_insert = executor.submit(
            run_spark_insert,
            table_uri,
            table_name,
            time.time() + run_duration,
        )
        delta_insert = executor.submit(
            run_delta_rs_insert,
            dt,
            time.time() + run_duration,
        )
        spark_total_inserts = spark_insert.result()
        delta_total_inserts = delta_insert.result()
    dt = DeltaTable(table_uri)
    assert dt.version() == spark_total_inserts + delta_total_inserts

    files_from_spark = len(list(filter(written_by_spark, dt.files())))
    assert files_from_spark == spark_total_inserts + 1
    # +1 is for the initial write that created the table
    assert dt.files().__len__() == spark_total_inserts + delta_total_inserts + 1


def create_bucket(bucket_url: str):
    env = os.environ.copy()
    (
        subprocess.run(
            [
                "aws",
                "s3",
                "mb",
                bucket_url,
            ],
            env=env,
        ),
    )


def remove_bucket(bucket_url: str):
    env = os.environ.copy()
    (
        subprocess.run(
            [
                "aws",
                "s3",
                "rb",
                bucket_url,
                "--force",
            ],
            env=env,
        ),
    )


def create_dynamodb_table(table_name: str):
    env = os.environ.copy()
    (
        subprocess.run(
            [
                "aws",
                "dynamodb",
                "create-table",
                "--table-name",
                table_name,
                "--attribute-definitions",
                "AttributeName=tablePath,AttributeType=S",
                "AttributeName=fileName,AttributeType=S",
                "--key-schema",
                "AttributeName=tablePath,KeyType=HASH",
                "AttributeName=fileName,KeyType=RANGE",
                "--provisioned-throughput",
                "ReadCapacityUnits=5,WriteCapacityUnits=5",
            ],
            env=env,
        ),
    )


def delete_dynamodb_table(table_name: str):
    env = os.environ.copy()
    (
        subprocess.run(
            [
                "aws",
                "dynamodb",
                "delete-table",
                "--table-name",
                table_name,
            ],
            env=env,
        ),
    )


@pytest.fixture
def setup():
    os.environ['AWS_ENDPOINT_URL'] = 'http://localhost:4566'
    os.environ['AWS_REGION'] = 'us-east-1'
    os.environ['AWS_ACCESS_KEY_ID'] = 'deltalake'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'weloverust'
    os.environ['AWS_ALLOW_HTTP'] = 'true'
    id = uuid.uuid4()
    bucket_name = f"delta-rs-integration-{id}"
    bucket_url = f"s3://{bucket_name}"

    create_bucket(bucket_url)
    create_dynamodb_table(bucket_name)
    # spark always uses s3a://, so delta has to as well, because the dynamodb primary key is the
    # table root path and it must match between all writers
    yield (f"s3a://{bucket_name}/test-concurrent-writes", bucket_name)
    delete_dynamodb_table(bucket_name)
    remove_bucket(bucket_url)
