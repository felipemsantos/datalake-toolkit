# -*- coding: utf-8 -*-
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
# Triggers based on S3 Events
# Description: Lambda Function to Update DynamoDB Stage Table
# Author: Samuel Otero Schmidt (samschs@amazon.com)
# Date: 2017-02-27

from __future__ import print_function

import logging
import os
import time
import urllib

import boto3

from common import send_notification, DatalakeStatus

# REGION NAME
REGION = os.getenv('AWS_DEFAULT_REGION')

# SNS topic to post email alerts to
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')

# DynamoDB table for Data Lake Control
DYNAMO_DB_CONTROL = os.getenv('DYNAMO_DB_CONTROL')

# DynamoDB table for Stage Control
DYNAMO_DB_STAGE_TABLE = os.getenv('DYNAMO_DB_STAGE_TABLE')

# Environment
ENVIRONMENT = os.getenv('ENVIRONMENT', 'DEV')

# Key tag with the raw path
TAG_OBJECT_NAME_RAW = 's3_object_name_raw_tag'

# Key metadata with the raw path
METADATA_OBJECT_NAME_RAW = 's3-raw-object'

WAIT_TIME = 2

sns_client = boto3.client('sns')
s3_client = boto3.client('s3')
dynamodb_client = boto3.resource('dynamodb', region_name=REGION)
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading Lambda Function {}'.format(__name__))


def get_object_tag(bucket, key):
    s3_object_name_raw = None
    try:
        resp = s3_client.get_object_tagging(
            Bucket=bucket,
            Key=key
        )
        logger.debug('S3 object tags: {}'.format(resp))
        for tag in resp.get('TagSet', []):
            if tag.get('Key') == TAG_OBJECT_NAME_RAW:
                s3_object_name_raw = tag.get('Value')

        return s3_object_name_raw
    except Exception as e:
        logger.error("S3 Exception: {}".format(e))
        send_notification(
            SNS_TOPIC_ARN,
            "Data Lake: Update DynamoDB Stage Exception",
            "S3 Get Tags error: {}".format(e)
        )
        raise e


def get_object_metadata(bucket, key):
    s3_object_name_raw = None
    try:
        resp = s3_client.head_object(
            Bucket=bucket,
            Key=key
        )
        logger.debug('S3 object head: {}'.format(resp))
        for key, value in resp.get('Metadata', {}).items():
            if key == METADATA_OBJECT_NAME_RAW:
                s3_object_name_raw = value

        return s3_object_name_raw
    except Exception as e:
        logger.error("S3 Exception: {}".format(e))
        send_notification(
            SNS_TOPIC_ARN,
            "Data Lake: Update DynamoDB Stage Exception",
            "S3 Head object error: {}".format(e)
        )
        raise e


def lambda_handler(event, context):
    logger.info("Lambda Function Name : {}".format(context.function_name))
    bucket = event['Records'][0]['s3']['bucket']['name']
    logger.debug('Event: {}'.format(event))
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))

    s3_object_name_raw = get_object_metadata(bucket, key)
    if not s3_object_name_raw:
        logger.info('Unable to extract raw object name from Metadata. Trying from tags')
        # There is a risk that this lambda is being triggered while the raw object is still updating the tags
        # We will delay a few seconds before run the api request.
        time.sleep(WAIT_TIME)
        s3_object_name_raw = get_object_tag(bucket, key)

        if not s3_object_name_raw:
            logger.error('Unable to extract raw object name from Metadata or Tags. Exiting')
            raise Exception('Unable to get the raw object name from metadata or tags')

    logger.debug("### Debug mode enabled ## ")
    logger.debug("s3_object_name_raw: {}".format(s3_object_name_raw))

    try:
        table_control = dynamodb_client.Table(DYNAMO_DB_CONTROL)
        response = table_control.update_item(
            TableName=DYNAMO_DB_CONTROL,
            Key={
                's3_object_name': s3_object_name_raw
            },
            UpdateExpression="set file_status = :stage",
            ExpressionAttributeValues={':stage': DatalakeStatus.STAGE}
        )
        logger.debug('DynamoDB Update response: {}'.format(response))
    except Exception as e:
        msg_exception = "DynamoDB Exception Table {} : {}".format(DYNAMO_DB_CONTROL, e)
        logger.error(msg_exception)

        try:
            response = sns_client.publish(
                TargetArn=SNS_TOPIC_ARN,
                Message="Lambda Function Name: {}\n{}".format(context.function_name, msg_exception)
            )
            logger.debug('SNS publish response: {}'.format(response))
            return
        except Exception as ne:
            logger.error("SNS Exception: {}".format(ne))
            return
