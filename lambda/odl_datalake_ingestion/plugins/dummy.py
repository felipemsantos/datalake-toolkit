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
# Dummy processor

from __future__ import print_function

import datetime
import logging
import os

from common import DatalakeIngestion, DatalakeStatus


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading processor DUMMY')


# This pattern process dummy files (Used for testing)
REGEX = r"(dummy)/([a-zA-Z0-9_.-]+.txt)"


def processor(**kwargs):
    key = kwargs.get('key')
    bucket = kwargs.get('bucket')
    context = kwargs.get('context')
    sns_topic_arn = kwargs.get('sns_topic_arn')
    header = kwargs.get('header')
    dynamo_db_control = kwargs.get('dynamo_db_control')
    bucket_target = kwargs.get('bucket_target')

    path, filename = key.split('/')

    logger.debug("### Debug mode enabled ## ")
    logger.debug("bucket: {}".format(bucket))
    logger.debug("key: {}".format(key))
    ingestion = DatalakeIngestion(context, sns_topic_arn, header, dynamo_db_control)
    load_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000")
    # This is the data sent to DynamoDB and ElasticSearch, add extra data if you wish
    data = {
        's3_object_name': "s3://{}/{}".format(bucket, key),
        'raw_timestamp': load_timestamp,
        'data_source': path,    # dummy
        'bucket': bucket,
        'object_name': filename,
        's3_object_name_stage': "s3://{}/{}".format(bucket_target, key),
        'partition': "false",
        'file_status': DatalakeStatus.INITIAL_LOAD,
        's3_dir_stage': 's3://{}/{}'.format(bucket_target, path),  # dummy
        'size': 0,
        'type': 'text/plain',
        'file_timestamp': load_timestamp
    }
    ingestion.copy_to_stage(bucket, key, bucket_target, key)
    ingestion.get_header(bucket, key)
    ingestion.send_to_dynamodb(data)
    ingestion.send_to_catalog(key, data)
    logger.debug('Finished the Datalake Dummy Ingestion process successfully')
    return
