import os

import pytest


@pytest.fixture(scope="session")
def s3cred() -> None:
    os.environ["AWS_REGION"] = "us-west-2"
    os.environ["AWS_ACCESS_KEY_ID"] = "AKIAX7EGEQ7FT6CLQGWH"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "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"
