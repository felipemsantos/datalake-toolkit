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
# Default processor

from __future__ import print_function

import datetime
import logging
import os

import boto3

from common import DatalakeIngestion


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading processor DEFAULT')


# Input pattern
# pattern:
# s3://bucket-raw
# Example:
# servicedesk/customer/ca_sdm/tb_call_req/latest/call_req.csv
REGEX = r"([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+)/(latest)/([a-zA-Z0-9_]+.csv)"


def processor(**kwargs):
    key = kwargs.get('key')
    bucket = kwargs.get('bucket')
    context = kwargs.get('context')
    sns_topic_arn = kwargs.get('sns_topic_arn')
    header = kwargs.get('header')
    dynamo_db_control = kwargs.get('dynamo_db_control')
    bucket_target = kwargs.get('bucket_target')

    sns_client = boto3.client('sns')
    s3_client = boto3.client('s3')
    business, operation, system, table, version, filename = key.split("/")

    load_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000")

    logger.debug("### Debug mode enabled ## ")
    logger.debug("bucket: {}".format(bucket))
    logger.debug("key: {}".format(key))
    logger.debug("business: {}".format(business))
    logger.debug("operation: {}".format(operation))
    logger.debug("system: {}".format(system))
    logger.debug("table: {}".format(table))
    logger.debug("filename: {}".format(filename))
    logger.debug("version: {}".format(version))

    ingestion = DatalakeIngestion(context, sns_topic_arn, header, dynamo_db_control)
    try:
        obj = s3_client.head_object(Bucket=bucket, Key=key)
        logger.debug("Object: {}".format(obj))
        # This processor set the PATH with latest but other tables can use partitions like: year=yyyy/month=mm/day=dd
        s3_dir_stage = "{}/{}/{}/{}".format(
            business,
            operation,
            system,
            table
        )
        key_target = "{}/{}/{}".format(s3_dir_stage, version, filename)

        logger.debug("key_target: {}".format(key_target))
        logger.debug("s3_dir_stage: {}".format(s3_dir_stage))
        logger.info("S3 Copy from: s3://{}/{}".format(bucket, key))
        logger.info("          to: s3://{}/{}".format(bucket_target, key_target))
        data = {
            's3_object_name': "s3://{}/{}".format(bucket, key),
            'raw_timestamp': load_timestamp,
            'data_source': business,
            'bucket': bucket,
            'object_name': filename,
            's3_object_name_stage': "s3://{}/{}".format(bucket_target, key_target),
            'file_status': 'INITIAL_LOAD',
            's3_dir_stage': 's3://{}/{}'.format(bucket_target, s3_dir_stage),
            'size': int(obj['ResponseMetadata']['HTTPHeaders']['content-length']),
            'type': obj['ResponseMetadata']['HTTPHeaders']['content-type'],
            'file_timestamp': obj['ResponseMetadata']['HTTPHeaders']['last-modified']
        }

        ingestion.copy_to_stage(bucket, key, bucket_target, key_target)
        ingestion.get_header(bucket, key)
        ingestion.send_to_dynamodb(data)
        ingestion.send_to_catalog(key, data)
        logger.debug('Finished the Datalake Ingestion process successfully')
        return

    except Exception as e:
        msg_exception = "Error getting object {object} from bucket {bucket}.\n" \
                        "Make sure they exist and your bucket is in the same region as this function.\n" \
                        "S3 Bucket source: {bucket}\n Key and filename: {object}\nError: {error}".format(
                            bucket=bucket,
                            object=key,
                            error=e)
        logger.info(msg_exception)
        response = sns_client.publish(
            TargetArn=sns_topic_arn,
            Message="Lambda Function Name : {}\n{}".format(context.function_name, msg_exception)
        )
        logger.info("Published the error to SNS topic. {}".format(response))
