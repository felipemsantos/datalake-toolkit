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
# Triggers based on CloudWatch Events
# Description: Lambda Function to get events of EMR step change to validate of the step was finished completed,
# cancelled or failed.
# Author: Samuel Otero Schmidt (samschs@amazon.com)
# Date: 2017-02-27

from __future__ import print_function

import traceback
import json
import logging
import os
import time

import boto3

from common import send_notification, DatalakeStatus

# SNS topic to post email alerts to
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')

# DynamoDB table for Data Lake Control
DYNAMO_DB_CONTROL = os.getenv('DYNAMO_DB_CONTROL')

# DynamoDB table for Stage Control
DYNAMO_DB_STAGE_TABLE = os.getenv('DYNAMO_DB_STAGE_TABLE')

# REGION NAME
REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

# ENVIRONMENT
ENVIRONMENT = os.getenv('ENVIRONMENT', 'DEV')

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
dynamodb_resource = boto3.resource('dynamodb', region_name=REGION)
emr_client = boto3.client("emr")
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading Lambda Function {}'.format(__name__))


def check_files_shutdown_emr(event_cluster_id, context):
    # check if there are files pending to be processed
    try:
        table_stage = dynamodb_resource.Table(DYNAMO_DB_STAGE_TABLE)
        results_stage = table_stage.scan()
    except Exception as e:
        logger.error("Error Reading DynamoDB Table: {}".format(e))
        send_notification(
            SNS_TOPIC_ARN,
            'AWS Lambda: {function_name}'
            ' error: Failed shutdown EMR cluster.\nError: {error}'.format(
                function_name=context.function_name,
                error=e
            ),
            'Datalake:{} Lambda Error'.format(ENVIRONMENT)
        )
        return 'Unable to Scan table'

    if not results_stage.get('Items'):
        msg = 'DynamoDB Stage Table {} is empty'.format(DYNAMO_DB_STAGE_TABLE)
        logger.info(msg)
        # EMR shutdown
        response = emr_client.terminate_job_flows(
            JobFlowIds=[
                event_cluster_id,
            ]
        )
        return response


def update_ddb_stage_control(item, file_status, timestamp):
    try:
        table_stage = dynamodb_resource.Table(DYNAMO_DB_STAGE_TABLE)

        response = table_stage.update_item(
            TableName=DYNAMO_DB_STAGE_TABLE,
            Key={
                's3_object_name_stage': item
            },
            UpdateExpression="set file_status = :file_status, timestamp_step_finished =:timestamp_step_finished",
            ExpressionAttributeValues={
                ':file_status': file_status,
                ':timestamp_step_finished': timestamp}
        )
        logger.debug('SNS publish response: {}'.format(response))
    except Exception as e:
        msg_exception = "DynamoDB Exception: {}".format(e)
        logger.info(msg_exception)
        send_notification(
            SNS_TOPIC_ARN,
            'AWS Lambda: ValidateJobSubmit'
            ' error: Unable to update DynamoDB Item.\nError: {}'.format(e),
            'Datalake:{} Lambda Error'.format(ENVIRONMENT)
        )
        return 'Unable to update Item from table'


def lambda_handler(event, context):
    step_name = event.get('detail', {}).get('name')
    event_step_message = event.get('detail', {}).get('message')
    event_step_state = event.get('detail', {}).get('state')
    event_step_id = event.get('detail', {}).get('stepId')
    event_cluster_id = event.get('detail', {}).get('clusterId')

    if 's3://' not in step_name:
        logger.info("It is not a catalog job.")
        return

    cluster_info = emr_client.describe_cluster(ClusterId=event_cluster_id)
    logger.info(cluster_info)
    cluster_name = cluster_info.get("Cluster", {}).get("Name")
    logger.info(cluster_name)

    timestamp_step_finished = time.strftime("%Y-%m-%dT%H:%M:%S-%Z")

    logger.debug("### Debug mode enabled ###")
    logger.debug("Received event: {}".format(json.dumps(event, indent=2)))
    logger.debug("Step Name: {}".format(step_name))
    logger.debug("Cluster Name: {}".format(event_cluster_id))
    logger.debug("Message event step changed: {}".format(event_step_message))

    if 'COMPLETED' in event_step_state:

        message_step_completed = "job execution completed: Name: {}; ID: {}".format(step_name, event_step_id)
        logger.info(message_step_completed)

        try:
            table_stage = dynamodb_resource.Table(DYNAMO_DB_STAGE_TABLE)

            response = table_stage.get_item(Key={'s3_object_name_stage': str(step_name)})
            logger.info(response)

        except Exception as e:
            msg_exception = "DynamoDB Exception: {}".format(e)
            logger.error(msg_exception)
            logger.debug(traceback.print_exc())
            send_notification(
                SNS_TOPIC_ARN,
                'AWS Lambda: {function_name}'
                ' error: Unable to get DynamoDB Item.\nError: {error}'.format(
                    function_name=context.function_name,
                    error=e
                ),
                'Datalake:{} Lambda Error'.format(ENVIRONMENT)
            )
            return 'Unable to Get Item from table'

        if response.get('Item'):
            hive_database_analytics = response['Item']['hive_database_analytics']
            hive_table_analytics = response['Item']['hive_table_analytics']
            s3_target = response['Item']['s3_target']
            s3_object_name_raw = response['Item']['s3_object_name_raw']
            logger.debug("### Debug mode enabled ###")
            logger.debug("Updating Table: {}".format(DYNAMO_DB_CONTROL))
            logger.debug("s3_object_name_raw: {}".format(s3_object_name_raw))
            try:
                table_control = dynamodb_resource.Table(DYNAMO_DB_CONTROL)
                response_control = table_control.update_item(
                    TableName=DYNAMO_DB_CONTROL,
                    Key={
                        's3_object_name': str(s3_object_name_raw)
                    },
                    UpdateExpression="set file_status = :file_status, "
                                     "timestamp_step_finished = :timestamp_step_finished, "
                                     "hive_table_analytics = :hive_table_analytics, "
                                     "hive_database_analytics = :hive_database_analytics, "
                                     "s3_target = :s3_target",
                    ExpressionAttributeValues={
                        ':file_status': DatalakeStatus.LOADED,
                        ':timestamp_step_finished': str(timestamp_step_finished),
                        ':hive_table_analytics': str(hive_table_analytics),
                        ':hive_database_analytics': str(hive_database_analytics),
                        ':s3_target': str(s3_target)}
                )
                logger.debug('DDB update_item response: {}'.format(response_control))
            except Exception as e:
                msg_exception = "DynamoDB Exception: {}".format(e)
                logger.error(msg_exception)
                logger.debug(traceback.print_exc())
                send_notification(
                    SNS_TOPIC_ARN,
                    'AWS Lambda: {function_name}'
                    ' error: Unable to update DynamoDB Item.\nError: {error}'.format(
                        function_name=context.function_name,
                        error=e
                    ),
                    'Datalake:{} Lambda Error'.format(ENVIRONMENT)
                )
                return 'Unable to Update Item from table'
        else:
            logger.info('There is no items returned from DynamoDB!')
            return 'No items to process'

        # TODO: Create a parameter to Delete or Keep the item in the DynamoDB StageControl
        logger.info("Cleaning Table DynamoDB: {}; s3_object_name_stage: {}".format(DYNAMO_DB_STAGE_TABLE, step_name))
        try:
            response_stage = table_stage.delete_item(Key={'s3_object_name_stage': step_name})
            http_status_code_delete_stage = response_stage['ResponseMetadata']['HTTPStatusCode']
            logger.debug("### Debug mode enabled ###")
            logger.debug(response)
            logger.debug("HTTPStatusCode: {}".format(http_status_code_delete_stage))
        except Exception as e:
            msg_exception = "DynamoDB Exception: {}".format(e)
            logger.error(msg_exception)
            send_notification(
                SNS_TOPIC_ARN,
                'AWS Lambda: {function_name}'
                ' error: Unable to delete DynamoDB Item.\nError: {error}'.format(
                    function_name=context.function_name,
                    error=e
                ),
                'Datalake:{} Lambda Error'.format(ENVIRONMENT)
            )
            return 'Unable to delete Item from table'

        logger.info("Cleaning s3 object stage: {}".format(step_name))
        bucket_stage = step_name.split("/")[2]
        logger.info("bucket: {}".format(bucket_stage))
        key_stage = step_name.split('/', 3)[3]
        logger.info("Key: {}".format(key_stage))
        try:
            s3_client.delete_object(Bucket=bucket_stage, Key=key_stage)
            # check if there are files pending to be processed
            # This step shutdown the cluster if there are no items in the StageControl Table
            check_files_shutdown_emr(event_cluster_id, context)
        except Exception as e:
            logger.error("S3 Exception: {}".format(e))
            return

    elif 'FAILED' in event_step_state:
        message_step_failed = "job execution failed: Name: {}; ID: {}".format(step_name, event_step_id)
        logger.info(message_step_failed)
        update_ddb_stage_control(step_name, DatalakeStatus.FAILED, timestamp_step_finished)

    elif 'CANCELLED' in event_step_state:
        message_step_cancelled = "job execution cancelled: Name: {}; ID: {}".format(step_name, event_step_id)
        logger.info(message_step_cancelled)
        update_ddb_stage_control(step_name, DatalakeStatus.CANCELED, timestamp_step_finished)
    else:
        return


if __name__ == '__main__':
    class Context(object):
        def __init__(self):
            self.function_name = 'mock'
            self.function_version = '1.0'

    mock_event = {
        "region": "us-east-2",
        "detail": {
            "stepId": "s-2PZOH669N5LUO",
            "severity": "INFO",
            "clusterId": "j-PES1EPZ6LHJU",
            "actionOnFailure": "CONTINUE",
            "state": "COMPLETED",
            "message": "Step s-2PZOH669N5LUO (s3://datalake-stage/sap/ge2...) in Amazon EMR cluster j-PES1EPZ6LHJU "
                       "(customer-emr-datalake) completed execution at 2018-04-03 19:16 UTC. The step started running "
                       "at 2018-04-03 19:16 UTC and took 0 minutes to complete.",
            "name": "s3://datalake-stage/sap/ge2/global/financial/bkpf/dt=2018-03-08/ge2_bkpf_201803081134.csv"
        },
        "detail-type": "EMR Step Status Change",
        "source": "aws.emr",
        "version": "0",
        "time": "2018-04-03T19:17:34Z",
        "id": "151cff45-1c4d-e4e9-ff29-1730d96b7d6c",
        "resources": []
    }
    mock_context = Context()
    lambda_handler(mock_event, mock_context)
