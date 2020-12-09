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
# Triggers based on DynamoDB Streams
# Lambda Function to Control the status of Stage DynamoDB table
# Author: Samuel Otero Schmidt (samschs@amazon.com)
# Date: 2017-02-27

from __future__ import print_function

import datetime
import logging
import os

import boto3

# REGION NAME
REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

# SNS topic to post email alerts to
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')

# DynamoDB table for Data Lake Control
DYNAMO_DB_CONTROL = os.getenv('DYNAMO_DB_CONTROL')

# DynamoDB table for Stage Control
DYNAMO_DB_STAGE_TABLE = os.getenv('DYNAMO_DB_STAGE_TABLE')

sns_client = boto3.client('sns')
s3_client = boto3.client('s3')
dynamodb_resource = boto3.resource('dynamodb', region_name=REGION)
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading Lambda Function {}'.format(__name__))


def lambda_handler(event, context):
    records = event['Records']
    logger.debug(records)
    for record in records:
        event = record['eventName']
        logger.info(event)
        # if str(event) == 'INSERT' or str(event) == 'MODIFY':
        if str(event) == 'MODIFY':
            s3_object_name_raw = record['dynamodb']['Keys'].get('s3_object_name', {}).get('S')
            partition = record['dynamodb']['NewImage'].get('partition', {}).get('S', 'false')
            s3_dir_stage = record['dynamodb']['NewImage'].get('s3_dir_stage', {}).get('S')
            s3_object_name_stage = record['dynamodb']['NewImage'].get('s3_object_name_stage', {}).get('S')
            file_status = record['dynamodb']['NewImage'].get('file_status', {}).get('S')
            if file_status == 'STAGE':
                table_stage = dynamodb_resource.Table(DYNAMO_DB_STAGE_TABLE)
                stage_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000")

                try:
                    response = table_stage.put_item(
                        Item={
                            's3_object_name_stage': s3_object_name_stage,
                            'stage_timestamp': stage_timestamp,
                            's3_object_name_raw': s3_object_name_raw,
                            's3_dir_stage': s3_dir_stage,
                            'partition': partition
                        }
                    )
                    logger.info("insert DynamoDB Stage: {}".format(response))
                except Exception as e:
                    msg_exception = "DynamoDB Exception Table {}: {}".format(DYNAMO_DB_STAGE_TABLE, e)
                    logger.error(msg_exception)
                    try:
                        response = sns_client.publish(
                            TargetArn=SNS_TOPIC_ARN,
                            Message="Lambda Function Name: {}\n{}".format(context.function_name, msg_exception)
                        )
                        logger.info('SNS response: {}'.format(response))
                        return
                    except Exception as ne:
                        logger.error("SNS Exception: {}".format(ne))
                        return
            else:
                return
