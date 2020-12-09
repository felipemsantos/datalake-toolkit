# Copyright 2018 Amazon.com, Inc. and its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#   http://aws.amazon.com/asl/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# Triggers based on S3 events
# Lambda Function for Data Lake Ingestion
# Author: Samuel Otero Schmidt (samschs@amazon.com)
# Date: 2017-02-27

from __future__ import print_function

import importlib
import logging
import os
import pkgutil
import re
import urllib


import boto3

# REGION NAME
REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

# SNS topic to post email alerts to
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')

# DynamoDB table to Datalake Control
DYNAMO_DB_CONTROL = os.getenv('DYNAMO_DB_CONTROL')

# S3 bucket source
BUCKET_SOURCE = os.getenv('BUCKET_SOURCE')

# S3 bucket target
BUCKET_TARGET = os.getenv('BUCKET_TARGET')

# Enable or disable get the dataset header (true/false)
HEADER = os.getenv('HEADER')

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading Lambda Function {}'.format(__name__))


def lambda_handler(event, context):
    sns_client = boto3.client('sns')
    logger.info("Invoked Lambda Function Name : " + context.function_name)
    __plugins__ = 'plugins'
    __path__ = os.path.dirname(os.path.abspath(__file__))
    __path__ = pkgutil.extend_path(os.path.join(__path__, __plugins__), 'datalake')
    logger.info('Loading plugins')
    processors = list()
    for _, package_name, _ in pkgutil.iter_modules([__path__]):
        full_package_name = '%s.%s' % (__plugins__, package_name)
        module = importlib.import_module(full_package_name)
        processors.append((module.REGEX, module.processor))

    processors = tuple(processors)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    for regex, processor in processors:
        matcher = re.match(regex, key)
        if matcher:
            # Parameters passed to the function
            params = {
                'bucket': bucket,
                'key': key,
                'sns_topic_arn': SNS_TOPIC_ARN,
                'header': HEADER,
                'metadata': 's3-object-raw',
                'dynamo_db_control': DYNAMO_DB_CONTROL,
                'bucket_target': BUCKET_TARGET,
                'context': context
            }
            processor(**params)
            break
    else:
        msg_exception = "Error processing object {1} from bucket {0}. " \
                        "The file path/format is unknown and this lambda function can't parse them.".format(bucket, key)
        logger.info(msg_exception)
        response = sns_client.publish(
            TargetArn=SNS_TOPIC_ARN,
            Message="Lambda Function Name : " + context.function_name + '\n' + msg_exception
        )
        logger.info("Published the error to SNS topic. {}".format(response))
        return


# Lambda function end HERE!


# This main function is for local execution & debugging
if __name__ == '__main__':
    mock_event = {
        "Records": [
            {
                "awsRegion": "us-east-2",
                "eventName": "ObjectCreated:Put",
                "eventSource": "aws:s3",
                "eventTime": "1970-01-01T00:00:00.000Z",
                "eventVersion": "2.0",
                "responseElements": {
                    "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
                    "x-amz-request-id": "EXAMPLE123456789"
                },
                "s3": {
                    "bucket": {
                        "arn": "arn:aws:s3:::customer-raw-dev",
                        "name": "bucket-raw-dev",
                        "ownerIdentity": {
                            "principalId": "EXAMPLE"
                        }
                    },
                    "configurationId": "testConfigRule",
                    "object": {
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "key": "servicedesk/customer/ca_sdm/tb_call_req/latest/call_req.csv",
                        # "key": "picture1.jpg",
                        "sequencer": "0A1B2C3D4E5F678901",
                        "size": 1024
                    },
                    "s3SchemaVersion": "1.0"
                },
                "userIdentity": {
                    "principalId": "EXAMPLE"
                }
            }
        ]
    }

    class MockContext(object):
        def __init__(self):
            self.function_name = 'mock'

    mock_context = MockContext()
    lambda_handler(mock_event, mock_context)
