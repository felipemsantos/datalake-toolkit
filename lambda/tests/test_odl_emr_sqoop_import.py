# -*- coding: utf-8 -*-
#
# tests/test_odl_create_emr_cluster.py
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
lambda_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../odl_emr_sqoop_import'))
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
    'INSTANCE_COUNT_CORE_NODE': '1',
    'INSTANCE_COUNT_TASK_NODE': '1',
    'INSTANCE_TYPE_CORE': 'm4.large',
    'INSTANCE_TYPE_MASTER': 'm4.large',
    'DYNAMO_DB_STAGE_TABLE': 'mock-datalake-OdlStageControl-FFFFFFFFFFF',
    'ENI_MASTER': 'eni-ffffffffffffffff'
}
with mock.patch.dict('os.environ', mock_vars):
    with mock.patch('common.boto3.client') as mock_boto3_client:
        with mock.patch('boto3.resource') as mock_boto3_resource:
            # We need to load the lambda function here to mock the boto3 objects that are initialized
            # when the module is loaded
            from odl_emr_sqoop_import import lambda_handler

            def test_invoke_emr_sqoop_import():
                """
                Test the odl_emr_sqoop_import function
                :return:
                """
                mock_context = MockContext()
                mock_event = {}
                lambda_handler(mock_event, mock_context)
                mock_boto3_resource.reset_mock()
                mock_boto3_client.reset_mock()

            def test_invoke_emr_sqoop_import_cluster_is_running():
                """
                Test the odl_emr_sqoop_import function
                :return:
                """
                mock_boto3_resource.return_value.Table.return_value.scan.return_value = {"Items": "mock"}
                mock_list_clusters_resp = {
                   "Clusters": [
                      {
                         "Id": "j-FFFFFFFFFFFFF",
                         "Name": "mock_cluster",
                         "NormalizedInstanceHours": 3,
                         "Status": {
                            "State": "WAITING",
                            "StateChangeReason": {
                               "Message": "Waiting after step completed"
                            }
                         }
                      }
                   ]
                }
                mock_tags = {
                    'Cluster': {
                        'Tags': [
                            {
                                'Value': 'mock_cluster', 'Key': 'Label'
                            }
                        ]
                    }
                }
                with mock.patch('common.boto3.client') as mock_common_boto3_client:
                    mock_common_boto3_client.return_value.get_paginator.return_value.paginate.return_value \
                        = [mock_list_clusters_resp]
                    mock_common_boto3_client.return_value.describe_cluster.return_value = mock_tags
                    mock_context = MockContext()
                    mock_event = {}
                    lambda_handler(mock_event, mock_context)
                    mock_common_boto3_client.reset_mock()

            def test_invoke_emr_sqoop_import_run_job_flow_exception():
                """
                Test the odl_emr_sqoop_import function
                :return:
                """
                mock_context = MockContext()
                mock_event = {}
                error_response = {'Error': {'Code': 'MockErrorException'}}
                mock_boto3_client.return_value.run_job_flow.side_effect \
                    = ClientError(error_response, 'run_job_flow')
                with pytest.raises(Exception, match=r'.*(MockErrorException).*'):
                    lambda_handler(mock_event, mock_context)
                mock_boto3_resource.reset_mock()
                mock_boto3_client.reset_mock()
