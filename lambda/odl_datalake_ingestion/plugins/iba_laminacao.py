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
import StringIO

import boto3
import botocore

from common import DatalakeIngestion


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading processor Iba Laminacao')


# Regex for iba laminacao source
# iba/br/laminacao/year=2018/month=05/day=30/pda000_2018-05-30_20.43.00.txt
REGEX = r"([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+)/(year=[0-9]{4})/(month=0[0-9]|month=1[0-2])" \
        r"/(day=[0-3][0-9])/([a-zA-Z0-9_.-]+.txt)"
MAX_FILE_SIZE = 500000


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

    # iba/br/laminacao/year=2018/month=05/day=30/pda000_2018-05-30_20.43.00.txt
    source, region, table, year, month, day, filename = key.split('/')
    year = year.split("=")[1]
    month = month.split("=")[1]
    day = day.split("=")[1]
    load_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000")

    logger.debug("### Debug mode enabled ## ")
    logger.debug("bucket: {}".format(bucket))
    logger.debug("key: {}".format(key))
    logger.debug("source: {}".format(source))
    logger.debug("region: {}".format(region))
    logger.debug("table: {}".format(table))
    logger.debug("filename: {}".format(filename))
    logger.debug("partition: {} {} {}".format(year, month, day))
    ingestion = DatalakeIngestion(context, sns_topic_arn, header, dynamo_db_control)
    try:
        obj = s3_client.head_object(Bucket=bucket, Key=key)
        logger.debug("Object: {}".format(obj))
        s3_dir_stage = "{}/{}/{}".format(
            region,
            source,
            table
        )
        key_target = "{}/dt={}-{}-{}/{}".format(s3_dir_stage, year, month, day, filename)

        logger.debug("key_target: {}".format(key_target))
        logger.debug("s3_dir_stage: {}".format(s3_dir_stage))
        logger.info("S3 Copy from: s3://{}/{}".format(bucket, key))
        logger.info("          to: s3://{}/{}".format(bucket_target, key_target))
        data = {
            's3_object_name': "s3://{}/{}".format(bucket, key),
            'raw_timestamp': load_timestamp,
            'data_source': source,
            'bucket': bucket,
            'object_name': filename,
            's3_object_name_stage': "s3://{}/{}".format(bucket_target, key_target),
            'partition': "{}-{}-{}".format(year, month, day),
            'file_status': 'INITIAL_LOAD',
            's3_dir_stage': 's3://{}/{}'.format(bucket_target, s3_dir_stage),
            'size': int(obj['ResponseMetadata']['HTTPHeaders']['content-length']),
            'type': obj['ResponseMetadata']['HTTPHeaders']['content-type'],
            'file_timestamp': obj['ResponseMetadata']['HTTPHeaders']['last-modified']
        }

        # This processor need to change the RAW file removing the first 2 lines that contain header information
        if int(obj['ResponseMetadata']['HTTPHeaders']['content-length']) > MAX_FILE_SIZE:
            logger.info('The Object size is bigger than 500MB and we can\'t process with this processor function')
            raise Exception('Unable to process big files (>500MB) with the function processor_iba_laminacao')

        try:
            # We will manipulate the file in-memory to avoid using disk and improve performance
            source_obj = StringIO.StringIO()
            destination_obj = StringIO.StringIO()
            s3_client.download_fileobj(bucket, key, source_obj)
            # After getting the object into our string_io we need to move the pointer to the start position (0)
            source_obj.seek(0)
            source_obj.readline()  # Skipping the first line
            destination_obj.write(source_obj.readline())
            destination_obj.writelines(source_obj.readlines())

        except botocore.exceptions.ClientError as e:
            logger.info('Error downloading  object: {}/{}'.format(bucket, key))
            logger.debug('Error: {}'.format(e))
            raise Exception('Unable to download the s3 object {}/{}'.format(bucket, key))

        try:
            destination_obj.seek(0)
            s3_client.put_object(Bucket=bucket_target, Key=key_target, Body=destination_obj.read())

        except botocore.exceptions.ClientError as e:
            logger.info('Error uploading  object: {}/{}'.format(bucket_target, key_target))
            logger.debug('Error: {}'.format(e))
            raise Exception('Unable to upload the s3 object {}/{}'.format(bucket_target, key_target))

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
