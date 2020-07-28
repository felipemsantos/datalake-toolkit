import json

import pytest

from ingestion.data_ingestion import app


@pytest.fixture()
def context():
    class FakeContext:
        function_name = "FUNCTION_NAME"
        memory_limit_in_mb = 1024
        invoked_function_arn = "INVOKED_FUNCTION_ARN"
        aws_request_id = "AWS_REQUEST_ID"
        log_group_name = "LOG_GROUP_NAME"
        log_stream_name = "LOG_STREAM_NAME"

        def get_remaining_time_in_millis(self):
            return 300000

    return FakeContext()


@pytest.fixture()
def sns_s3_event():
    with open("../../events/data_ingestion_event.json", "r") as fp:
        return json.load(fp)


def test_lambda_handler(sns_s3_event, context):
    ret = app.lambda_handler(sns_s3_event, context)
    assert ret['incoming_bytes'] > 0
    assert ret['incoming_records'] > 0
