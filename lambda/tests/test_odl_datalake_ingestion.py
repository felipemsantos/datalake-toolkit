# -*- coding: utf-8 -*-
#
# tests/test_odl_datalake_ingestion.py
#
# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import sys

import mock
from botocore.exceptions import ClientError
# We need to add the parent directory to the path to find the module to test
lambda_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../odl_datalake_ingestion'))
sys.path.insert(0, os.path.abspath(lambda_path))

mock_event = {
    "Records": [
        {
            "awsRegion": "us-east-2",
            "eventName": "ObjectCreated:Put",
            "eventSource": "aws:s3",
            "eventTime": "1970-01-01T00:00:00.000Z",
            "s3": {
                "bucket": {
                    "arn": "arn:aws:s3:::customer-raw-dev",
                    "name": "bucket-raw-dev",
                    "ownerIdentity": {
                        "principalId": "EXAMPLE"
                    }
                },
                "object": {
                    "key": "servicedesk/customer/ca_sdm/tb_call_req/latest/call_req.csv",
                    "size": 1024
                },
                "s3SchemaVersion": "1.0"
            }
        }
    ]
}


class MockContext(object):
    def __init__(self):
        self.function_name = 'mock'


@mock.patch('boto3.resource')
@mock.patch('boto3.client')
def test_invoke_default_processor(mock_boto3_client, mock_boto3_resource):
    """
    Test the odl_datalake_ingestion function with valid event invocation
    :return:
    """
    mock_boto3_client.return_value.head_object.return_value = {
        "AcceptRanges": "bytes",
        "ContentLength": 4372,
        "ContentType": "binary/octet-stream",
        "ETag": "287e8177e8ffbc9ca08fb2afabd237ba",
        "LastModified": "Wed, 21 Nov 2018 20:05:15 GMT",
        "Metadata": {
            "s3_object_name_raw_tag": "s3://mock-datalake-bucket/dummy/dummy-0.txt"
        },
        "ResponseMetadata": {
            "HTTPHeaders": {
                "accept-ranges": "bytes",
                "content-length": "4372",
                "content-type": "binary/octet-stream",
                "date": "Wed, 21 Nov 2018 20:05:19 GMT",
                "etag": "287e8177e8ffbc9ca08fb2afabd237ba",
                "last-modified": "Wed, 21 Nov 2018 20:05:15 GMT",
                "server": "AmazonS3",
                "x-amz-id-2": "Fe8soQAbr2qCBMt04hu4cMvD59ugsNYRrVHlpzgFn4tV8DbQKzYStMXAvWKmgi5/+ttJjIk07e0=",
                "x-amz-meta-s3_object_name_raw_tag": "s3://mock-datalake-bucket/dummy/dummy-0.txt",
                "x-amz-request-id": "F1BE1D8588C74F8F",
                "x-amz-version-id": "X2YgsAhqxyZh9_6aTOUJC0B1.BGKX6iN"
            },
            "HTTPStatusCode": 200,
            "HostId": "Fe8soQAbr2qCBMt04hu4cMvD59ugsNYRrVHlpzgFn4tV8DbQKzYStMXAvWKmgi5/+ttJjIk07e0=",
            "RequestId": "F1BE1D8588C74F8F",
            "RetryAttempts": 0
        },
        "VersionId": "X2YgsAhqxyZh9_6aTOUJC0B1.BGKX6iN"
    }
    from odl_datalake_ingestion import lambda_handler
    mock_context = MockContext()
    mock_event["Records"][0]["s3"]["object"]["key"] = "servicedesk/customer/ca_sdm/tb_call_req/latest/call_req.csv"

    lambda_handler(mock_event, mock_context)


@mock.patch('boto3.resource', autospec=True)
@mock.patch('boto3.client', autospec=True)
def test_invoke_default_processor_exception_head_object(mock_boto3_client, mock_boto3_resource):
    """
    Test the odl_datalake_ingestion function with valid event invocation
    :return:
    """
    from odl_datalake_ingestion import lambda_handler
    mock_context = MockContext()
    error_response = {'Error': {'Code': 'MockErrorException'}}
    mock_boto3_client.return_value.head_object.side_effect = ClientError(error_response, 'head_object')
    lambda_handler(mock_event, mock_context)


@mock.patch('boto3.resource', autospec=True)
@mock.patch('boto3.client', autospec=True)
def test_invoke_invalid_object(mock_boto3_client, mock_boto3_resource):
    """
    Test the odl_datalake_ingestion function with valid event invocation
    :return:
    """
    from odl_datalake_ingestion import lambda_handler
    mock_context = MockContext()
    mock_event["Records"][0]["s3"]["object"]["key"] = "this/path/doesnt/exist.ext"
    lambda_handler(mock_event, mock_context)


@mock.patch('boto3.resource', autospec=True)
@mock.patch('boto3.client', autospec=True)
def test_invoke_dummy_plugin(mock_boto3_client, mock_boto3_resource):
    """
    Test the odl_datalake_ingestion function with dummy object
    :return:
    """
    from odl_datalake_ingestion import lambda_handler
    mock_context = MockContext()
    mock_event["Records"][0]["s3"]["object"]["key"] = "dummy/dummy00.txt"
    lambda_handler(mock_event, mock_context)


@mock.patch('boto3.resource', autospec=True)
@mock.patch('boto3.client', autospec=True)
def test_invoke_iba_plugin(mock_boto3_client, mock_boto3_resource):
    """
    Test the odl_datalake_ingestion function with dummy object
    :return:
    """
    from odl_datalake_ingestion import lambda_handler
    mock_context = MockContext()
    mock_event["Records"][0]["s3"]["object"]["key"] = "iba/br/laminacao/year=2018/month=05/day=30/pda00.txt"
    mock_boto3_client.return_value.head_object.return_value = {
        "ResponseMetadata": {
            "HTTPHeaders": {
                "content-length": 1024,
                "content-type": "text/plain",
                "last-modified": "Sun, 1 Jan 2006 12:00:00 GMT"
            }
        }
    }
    lambda_handler(mock_event, mock_context)


@mock.patch('boto3.resource')
@mock.patch('boto3.client')
def test_invoke_iba_plugin_with_biggest_size(mock_boto3_client, mock_boto3_resource):
    """
    Test the odl_datalake_ingestion function with iba object bigger than 500MB
    :return:
    """
    from odl_datalake_ingestion import lambda_handler
    mock_context = MockContext()
    mock_event["Records"][0]["s3"]["object"]["key"] = "iba/br/laminacao/year=2018/month=05/day=30/pda00.txt"
    mock_boto3_client.return_value.head_object.return_value = {
        "ResponseMetadata": {
            "HTTPHeaders": {
                "content-length": 500001,
                "content-type": "text/plain",
                "last-modified": "Sun, 1 Jan 2006 12:00:00 GMT"
            }
        }
    }
    lambda_handler(mock_event, mock_context)


@mock.patch('boto3.resource')
@mock.patch('boto3.client')
def test_invoke_iba_plugin_exception_download_fileobj(mock_boto3_client, mock_boto3_resource):
    """
    Test the odl_datalake_ingestion function with iba processor and download_fileobj exception
    :return:
    """
    from odl_datalake_ingestion import lambda_handler
    mock_context = MockContext()
    mock_event["Records"][0]["s3"]["object"]["key"] = "iba/br/laminacao/year=2018/month=05/day=30/pda00.txt"
    mock_boto3_client.return_value.head_object.return_value = {
        "ResponseMetadata": {
            "HTTPHeaders": {
                "content-length": 1024,
                "content-type": "text/plain",
                "last-modified": "Sun, 1 Jan 2006 12:00:00 GMT"
            }
        }
    }
    error_response = {'Error': {'Code': 'MockErrorException'}}
    mock_boto3_client.return_value.download_fileobj.side_effect = ClientError(error_response, 'download_fileobj')
    lambda_handler(mock_event, mock_context)


@mock.patch('boto3.resource')
@mock.patch('boto3.client')
def test_invoke_iba_plugin_exception_put_object(mock_boto3_client, mock_boto3_resource):
    """
    Test the odl_datalake_ingestion function with iba processor and download_fileobj exception
    :return:
    """
    from odl_datalake_ingestion import lambda_handler
    mock_context = MockContext()
    mock_event["Records"][0]["s3"]["object"]["key"] = "iba/br/laminacao/year=2018/month=05/day=30/pda00.txt"
    mock_boto3_client.return_value.head_object.return_value = {
        "ResponseMetadata": {
            "HTTPHeaders": {
                "content-length": 1024,
                "content-type": "text/plain",
                "last-modified": "Sun, 1 Jan 2006 12:00:00 GMT"
            }
        }
    }
    error_response = {'Error': {'Code': 'MockErrorException'}}
    mock_boto3_client.return_value.put_object.side_effect = ClientError(error_response, 'put_object')
    lambda_handler(mock_event, mock_context)


@mock.patch('boto3.resource')
@mock.patch('boto3.client')
def test_invoke_skip_plugin(mock_boto3_client, mock_boto3_resource):
    """
    Test the odl_datalake_ingestion function with skip_file plugin
    :return:
    """
    from odl_datalake_ingestion import lambda_handler
    mock_context = MockContext()
    mock_event["Records"][0]["s3"]["object"]["key"] = "servicedesk/customer/ca_sdm/tb_call_req/2018-07-02/call_req.csv"
    mock_boto3_client.return_value.head_object.return_value = {
        "ResponseMetadata": {
            "HTTPHeaders": {
                "content-length": 1024,
                "content-type": "text/plain",
                "last-modified": "Sun, 1 Jan 2006 12:00:00 GMT"
            }
        }
    }
    lambda_handler(mock_event, mock_context)
