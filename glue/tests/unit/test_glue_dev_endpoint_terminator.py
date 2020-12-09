import json

import pytest

from glue.dev_endpoint_terminator import main


@pytest.fixture()
def cw_event():
    with open("../../events/cloudwatch-scheduled-event.json", "r") as fp:
        return json.load(fp)


@pytest.fixture()
def lambda_context():
    class MockContext():

        def __init__(self):
            self.function_name = "TestFunction"

    return MockContext()


def test_glue_dev_endpoint_starter(cw_event, lambda_context, faker):
    response = main.lambda_handler(cw_event, lambda_context)

    assert response != None
