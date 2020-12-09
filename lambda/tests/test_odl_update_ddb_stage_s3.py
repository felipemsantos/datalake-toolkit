# -*- coding: utf-8 -*-
#
# tests/test_odl_validate_job_submit.py
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
import pytest
import sys

import mock
from botocore.exceptions import ClientError
# We need to add the parent directory to the path to find the module to test
lambda_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../odl_update_ddb_stage_s3'))
sys.path.insert(0, os.path.abspath(lambda_path))


class MockContext(object):
    def __init__(self):
        self.function_name = 'mock'


# We need to mock boto3 before we load the lambda module because the lambda function setup some instances when loaded
# If we just import the module without the proper preparation the lambda will try to call AWS endpoints and this is
# not what we want in a unit-test.
# This test is using context instead of decorator @mock.patch because with decorators we can't reset the MagicMock
# correctly
mock_vars = {
    'CLUSTER_LABEL': 'mock_cluster',
    'SNS_TOPIC_ARN': 'arn:aws:sns:us-east-1:111111111111:mock-datalake',
    'S3_BOOTSTRAP_BUCKET': 'mock_artifacts',
    'MY_LOG_BUCKET': 'mock_logs',
    'EC2_KEYPAIR_NAME': 'mock_keypair',
    'SUBNET_ID_FOR_CLUSTER': 'subnet-ffffffff',
    'EMR_RELEASE': 'emr-5.13.0',
    'DYNAMO_DB_STAGE_TABLE': 'mock-datalake-OdlStageControl-FFFFFFFFFFF',
    'ENI_MASTER': 'eni-ffffffffffffffff'
}
with mock.patch.dict('os.environ', mock_vars):
    with mock.patch('boto3.client') as mock_boto3_client:
        with mock.patch('boto3.resource') as mock_boto3_resource:
            # We need to load the lambda function here to mock the boto3 objects that are initialized
            # when the module is loaded
            from odl_update_ddb_stage_s3 import lambda_handler

            def test_invoke_update_ddb_stage_s3_get_tagging_exception():
                """
                Test the odl_update_ddb_stage_s3 function get tagging exception
                :return:
                """
                mock_context = MockContext()
                mock_event = {
                    "Records": [
                        {
                            "awsRegion": "us-east-2",
                            "eventName": "ObjectCreated:Copy",
                            "eventSource": "aws:s3",
                            "eventTime": "2018-11-16T18:19:08.864Z",
                            "eventVersion": "2.0",
                            "requestParameters": {
                                "sourceIPAddress": "18.223.170.189"
                            },
                            "responseElements": {
                                "x-amz-id-2": "0EuXVJiCVZ4AhblfZF9XeuOx3KbWIoiOzsnZJ9oLFtBuhB0vCE8F7vwD/WyXLRSxmVJ4S0=",
                                "x-amz-request-id": "9DC65C2015E6D097"
                            },
                            "s3": {
                                "bucket": {
                                    "arn": "arn:aws:s3:::koiker-bigdata-stage-dev",
                                    "name": "mock-bigdata-stage-dev",
                                    "ownerIdentity": {
                                        "principalId": "AGVL4EEVT32C5"
                                    }
                                },
                                "configurationId": "403ee87c-337c-446b-8411-2f78754d82f8",
                                "object": {
                                    "eTag": "275876e34cf609db118f3d84b799a790",
                                    "key": "dummy/dummy-0.txt",
                                    "sequencer": "005BEF0A1CCB896AB5",
                                    "size": 5,
                                    "versionId": "cx8bUwE8XIvv.Vs1F5FqE_il_zLVXmVE"
                                },
                                "s3SchemaVersion": "1.0"
                            },
                            "userIdentity": {
                                "principalId": "AWS:AROAIQLGWVDU3EZ6KOVCG:sample-datalake-odlDatalakeIngestion-13MORNY7"
                            }
                        }
                    ]
                }
                with pytest.raises(Exception, match=r'.*(Unable to get the raw object name from metadata or tags).*'):
                    lambda_handler(mock_event, mock_context)

            def test_invoke_update_ddb_stage_s3():
                """
                Test the odl_update_ddb_stage_s3 function with skip
                :return:
                """
                mock_context = MockContext()
                mock_event = {
                    "Records": [
                        {
                            "awsRegion": "us-east-2",
                            "eventName": "ObjectCreated:Copy",
                            "eventSource": "aws:s3",
                            "eventTime": "2018-11-16T18:19:08.864Z",
                            "eventVersion": "2.0",
                            "requestParameters": {
                                "sourceIPAddress": "18.223.170.189"
                            },
                            "responseElements": {
                                "x-amz-id-2": "0EuXVJiCVZ4AhblfZF9XeuOx3KbWIoiOzsnZJ9oLFtBuhB0vCE8F7vwD/WyXL5HQlMJ4S0=",
                                "x-amz-request-id": "9DC65C2015E6D097"
                            },
                            "s3": {
                                "bucket": {
                                    "arn": "arn:aws:s3:::koiker-bigdata-stage-dev",
                                    "name": "mock-bigdata-stage-dev",
                                    "ownerIdentity": {
                                        "principalId": "AGVL4EEVT32C5"
                                    }
                                },
                                "configurationId": "403ee87c-337c-446b-8411-2f78754d82f8",
                                "object": {
                                    "eTag": "275876e34cf609db118f3d84b799a790",
                                    "key": "dummy/dummy-0.txt",
                                    "sequencer": "005BEF0A1CCB896AB5",
                                    "size": 5,
                                    "versionId": "cx8bUwE8XIvv.Vs1F5FqE_il_zLVXmVE"
                                },
                                "s3SchemaVersion": "1.0"
                            },
                            "userIdentity": {
                                "principalId": "AWS:AROAIQLGWVDU3EZ6KOVCG:sample-datalake-odlDatalakeIngestion-13MRRNY7"
                            }
                        }
                    ]
                }
                mock_object_tags = {
                    "ResponseMetadata": {
                        "HTTPHeaders": {
                            "date": "Sat, 17 Nov 2018 01:29:52 GMT",
                            "server": "AmazonS3",
                            "transfer-encoding": "chunked",
                            "x-amz-id-2": "4KNBeoMXy/vxiEokqsZUS6KftsmAP80iepwT/o5jpDtGoNWn/0zny/04gLJpVSOBqLtcDd4u8=",
                            "x-amz-request-id": "D0788C2651B96E1A",
                            "x-amz-version-id": "GfTYrn7uVjlCKuARn4XAVOu6SQLTOzA9"
                        },
                        "HTTPStatusCode": 200,
                        "HostId": "4KNBeoMXy/vxiEokqsZUS6KftsmAP80iepwT/o5jpDtGoNWn/0zny/04gLJpVSOBqLK0tcDd4u8=",
                        "RequestId": "D0788C2651B96E1A",
                        "RetryAttempts": 0
                    },
                    "TagSet": [
                        {
                            "Key": "s3_object_name_raw_tag",
                            "Value": "s3://koiker-bigdata-raw-dev/dummy/dummy-0.txt"
                        }
                    ],
                    "VersionId": "GfTYrn7uVjlCKuARn4XAVOu6SQLTOzA9"
                }
                mock_boto3_client.return_value.get_object_tagging.return_value = mock_object_tags
                lambda_handler(mock_event, mock_context)
                mock_boto3_client.return_value.get_object_tagging.return_value = mock.DEFAULT

            def test_invoke_update_ddb_stage_s3_update_item_exception():
                """
                Test the odl_update_ddb_stage_s3 function with update_item exception
                :return:
                """
                mock_context = MockContext()
                mock_event = {
                    "Records": [
                        {
                            "awsRegion": "us-east-2",
                            "eventName": "ObjectCreated:Copy",
                            "eventSource": "aws:s3",
                            "eventTime": "2018-11-16T18:19:08.864Z",
                            "eventVersion": "2.0",
                            "requestParameters": {
                                "sourceIPAddress": "1.1.1.1"
                            },
                            "responseElements": {
                                "x-amz-id-2": "00000/00000=",
                                "x-amz-request-id": "9DC65C2015E6D097"
                            },
                            "s3": {
                                "bucket": {
                                    "arn": "arn:aws:s3:::mock-bigdata-stage-dev",
                                    "name": "mock-bigdata-stage-dev",
                                    "ownerIdentity": {
                                        "principalId": "AGVL4EEVT32C5"
                                    }
                                },
                                "configurationId": "403ee87c-337c-446b-8411-2f78754d82f8",
                                "object": {
                                    "eTag": "275876e34cf609db118f3d84b799a790",
                                    "key": "dummy/dummy-0.txt",
                                    "sequencer": "005BEF0A1CCB896AB5",
                                    "size": 5,
                                    "versionId": "cx8bUwE8XIvv.Vs1F5FqE_il_zLVXmVE"
                                },
                                "s3SchemaVersion": "1.0"
                            },
                            "userIdentity": {
                                "principalId": "AWS:AAAAAAAAAA:sample-datalake-odlDatalakeIngestion"
                            }
                        }
                    ]
                }
                mock_object_tags = {
                    "ResponseMetadata": {
                        "HTTPHeaders": {
                            "date": "Sat, 17 Nov 2018 01:29:52 GMT",
                            "server": "AmazonS3",
                            "transfer-encoding": "chunked",
                            "x-amz-id-2": "4KNBeoMXy/vxiEokqsZUS6KftsmAP80iepwT/o5jpDtGoNWn/0zny/04gLJpVSOBqLK0Dd4u8=",
                            "x-amz-request-id": "D0788C2651B96E1A",
                            "x-amz-version-id": "GfTYrn7uVjlCKuARn4XAVOu6SQLTOzA9"
                        },
                        "HTTPStatusCode": 200,
                        "HostId": "4KNBeoMXy/vxiEokqsZUS6KftsmAP80iepwT/o5jpDtGoNWn/0zny/04gLJpVSOBqLK0tcDd4u8=",
                        "RequestId": "D0788C2651B96E1A",
                        "RetryAttempts": 0
                    },
                    "TagSet": [
                        {
                            "Key": "s3_object_name_raw_tag",
                            "Value": "s3://koiker-bigdata-raw-dev/dummy/dummy-0.txt"
                        }
                    ],
                    "VersionId": "GfTYrn7uVjlCKuARn4XAVOu6SQLTOzA9"
                }
                mock_boto3_client.return_value.get_object_tagging.return_value = mock_object_tags
                error_response = {'Error': {'Code': 'MockErrorException'}}
                mock_boto3_resource.return_value.Table.return_value.update_item.side_effect \
                    = ClientError(error_response, 'update_item')
                lambda_handler(mock_event, mock_context)
                mock_boto3_resource.return_value.Table.return_value.update_item.return_value = mock.DEFAULT
