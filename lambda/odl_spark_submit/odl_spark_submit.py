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
# Triggers based on CloudWatch Events
# Lambda Function to Submit Spark Programs
# Author: Samuel Otero Schmidt (samschs@amazon.com)
# Date: 2017-02-27

from __future__ import print_function

import logging
import os
import time

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from common import send_notification, DatalakeStatus

# SNS topic to post email alerts to
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')

# DynamoDB table for Data Lake Control
DYNAMO_DB_CONTROL = os.getenv('DYNAMO_DB_CONTROL')

# DynamoDB table for Stage Control
DYNAMO_DB_STAGE_TABLE = os.getenv('DYNAMO_DB_STAGE_TABLE')

# DynamoDB table for Job Catalog
DYNAMO_DB_JOB_CATALOG = os.getenv('DYNAMO_DB_JOB_CATALOG')

# Enable or disable get the dataset header (true/false)
HEADER = os.getenv('HEADER')

# REGION NAME
REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

# CLUSTER NAME
CLUSTER_NAME = os.getenv('CLUSTER_NAME')

# S3_key_programs - KEY for Spark programs
S3_KEY_PROGRAMS = os.getenv('S3_key_programs')

# S3_bucket_programs - Bucket for Spark programs
S3_BUCKET_PROGRAMS = os.getenv('S3_bucket_programs')

# Shell to Setup Jobs
SETUP_JOBS = os.getenv('SETUP_JOBS')

ENVIRONMENT = os.getenv('ENVIRONMENT', 'DEV')

# Cloudwatch Event Rule (Used to continue the job submission when the number of jobs exceed the EMR limits
EVENT_SPARK_SUBMIT = os.getenv('EVENT_SPARK_SUBMIT')

STEPS_EXCEEDED = u"Maximum number of active steps(State = 'Running', 'Pending' or 'Cancel_Pending') for cluster " \
                 u"exceeded."

emr_client = boto3.client("emr")
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
events_client = boto3.client('events')
dynamodb_client = boto3.resource('dynamodb', region_name=REGION)

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading Lambda Function {}'.format(__name__))


def check_spark_submit_rule_enabled():
    try:
        resp = events_client.describe_rule(Name=EVENT_SPARK_SUBMIT)
        logger.debug('Describe Rule response: {}'.format(resp))
        rule_status = resp.get('State', 'DISABLED')
    except ClientError as er:
        msg_exception = "Describing Events Rule Exception: {}".format(er)
        logger.error(msg_exception)
        send_notification(
            SNS_TOPIC_ARN,
            "Data Lake:{} Spark Submit Exception".format(ENVIRONMENT),
            "CloudWatch Describe Rule request error: {}".format(msg_exception)
        )
        raise Exception('Unable to request API events:DescribeRule')
    return rule_status


def set_spark_submit_rule_status(status):
    if status not in ['ENABLED', 'DISABLED']:
        logger.error('Missing status parameter. Must set status to ENABLED or DISABLED')
        raise Exception('Missing status to set the event rule')
    logger.info('We are going to {} the scheduled trigger to continue'.format(status))
    try:
        if status == 'ENABLED':
            api_request = 'events:EnableRule'
            resp = events_client.enable_rule(Name=EVENT_SPARK_SUBMIT)
        else:
            api_request = 'events:DisableRule'
            resp = events_client.disable_rule(Name=EVENT_SPARK_SUBMIT)
        logger.debug('{} Rule response: {}'.format(status, resp))
    except ClientError as er:
        msg_exception = "{} Events Rule Exception: {}".format(status, er)
        logger.error(msg_exception)
        send_notification(
            SNS_TOPIC_ARN,
            "Data Lake: Spark Submit Exception",
            "CloudWatch Enable Rule request error: {}".format(msg_exception)
        )
        raise Exception('Unable to request API {}'.format(api_request))
    return


def lambda_handler(event, context):
    # chooses the first cluster which is Running or Waiting
    # possibly can also choose by name or already have the cluster id
    skip = None
    if isinstance(event, dict):
        skip = event.get('skip')

    clusters = emr_client.list_clusters(ClusterStates=['STARTING', 'RUNNING', 'WAITING'])
    logger.info(clusters)

    # ClusterName
    logger.info(event.values())

    logger.info(event.get('detail', {}).get('name', {}))
    clusterValue = event.get('detail', {}).get('name', {})

    if clusterValue != CLUSTER_NAME:
        logger.error("No valid cluster")
        return 'No valid cluster'

    # choose the correct cluster
    for cluster in clusters.get('Clusters', []):
        if cluster['Name'] == CLUSTER_NAME:
            # take the first relevant cluster
            cluster_id = cluster['Id']
            break
    else:
        logger.error("No valid clusters")
        return 'No valid clusters'

    step_args = [SETUP_JOBS, "s3://{}/{}".format(S3_BUCKET_PROGRAMS, S3_KEY_PROGRAMS)]

    step = {"Name": 'Setup_jobs',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3://{}.elasticmapreduce/libs/script-runner/script-runner.jar'.format(REGION),
                'Args': step_args
            }
            }

    logger.debug("### Debug mode enabled ###")
    logger.debug("EMR Step: {}".format(step))
    logger.debug("EMR Cluster_id: {}".format(cluster_id))

    try:
        action = emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
        logger.info('EMR action: {}'.format(action))

    except Exception as e:
        msg_exception = "EMR Exception: " + str(e)
        logger.error(msg_exception)
        send_notification(
            SNS_TOPIC_ARN,
            "Data Lake: Spark Submit Exception",
            "Lambda Function Name: {}\n{}".format(context.function_name, msg_exception)
        )
        return

    try:
        table_stage = dynamodb_client.Table(DYNAMO_DB_STAGE_TABLE)
        table_job = dynamodb_client.Table(DYNAMO_DB_JOB_CATALOG)
        results = table_stage.scan(FilterExpression=Attr('file_status').ne(skip))

    except Exception as e:
        msg_exception = "DynamoDB Scan Exception: {}".format(e)
        logger.error(msg_exception)
        send_notification(
            SNS_TOPIC_ARN,
            "Data Lake: Spark Submit Exception",
            "Lambda Function Name: {}\n{}".format(context.function_name, msg_exception)
        )
        return

    for item in results.get('Items'):
        s3_object_name_stage = item.get('s3_object_name_stage')
        partition_date = item.get('partition')
        s3_dir_stage = item.get('s3_dir_stage')

        logger.debug("### Debug mode enabled ###")
        logger.debug("Items: {}".format(item))
        logger.debug("partition_date: {}".format(partition_date))
        logger.debug("s3_dir_stage: {}".format(s3_dir_stage))

        try:
            responses = table_job.get_item(Key={'s3_data_source': str(s3_dir_stage)})
        except Exception as e:
            msg_exception = "DynamoDB Job GetItem Exception: {}".format(e)
            logger.error(msg_exception)
            send_notification(
                SNS_TOPIC_ARN,
                "Data Lake: Spark Submit Exception",
                "Lambda Function Name: {}\n{}".format(context.function_name, msg_exception)
            )
            return

        if responses.get('Item'):
            spark_program_s3_path = responses['Item']['programs']
            spark_program = spark_program_s3_path.split("/")[-1]
            hive_database_raw = responses['Item']['hive_database_raw']
            hive_database_analytics = responses['Item']['hive_database_analytics']
            hive_table_raw = responses['Item']['hive_table_raw']
            hive_table_analytics = responses['Item']['hive_table_analytics']
            s3_target = responses['Item']['s3_target']
            partition_name_stage = responses['Item']['partition_name_stage']
            status_enabled = responses['Item']['Enabled']
            params_type = responses.get('Item', {}).get('params_type')
            params = responses.get('Item', {}).get('params')

            logger.debug("responses: {}".format(responses['Item']))
            logger.debug("spark_program_s3_path: {}".format(spark_program_s3_path))
            logger.debug("spark_program: {}".format(spark_program))
            logger.debug("hive_database_raw: {}".format(hive_database_raw))
            logger.debug("hive_database_analytics: {}".format(hive_database_analytics))
            logger.debug("hive_table_raw: {}".format(hive_table_raw))
            logger.debug("hive_table_analytycs: {}".format(hive_table_analytics))
            logger.debug("s3_target: {}".format(s3_target))
            logger.debug("partition_name_stage: {}".format(partition_name_stage))
            logger.debug("status_enabled: {}".format(status_enabled))

            # code location on your emr master node
            code_path = "/home/hadoop/code/"

            # spark configuration example
            # step_args = ["/usr/bin/spark-submit", "--spark-conf", "your-configuration",
            #             code_path + "your_file.py", '--your-parameters', 'parameters']
            step_args = [
                "/usr/bin/spark-submit",
                "--conf",
                "spark.yarn.appMasterEnv.PYTHONIOENCODING=utf8"
            ]
            if params_type and params_type == 'json':
                step_args.append(code_path + spark_program)
            elif params_type and params_type == 'cli':
                step_args.append(code_path + spark_program)
                for param in params.split(' '):
                    step_args.append(param)
            else:
                step_args.append(code_path + spark_program)
                step_args.append(hive_database_raw)
                step_args.append(hive_table_raw)
                step_args.append(s3_dir_stage)
                step_args.append(hive_database_analytics)
                step_args.append(hive_table_analytics)
                step_args.append(s3_target)
                if partition_name_stage != 'false':
                    step_args.append('{}={}'.format(partition_name_stage, partition_date))

            step = {"Name": s3_object_name_stage,
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': step_args
                    }
                    }

            if status_enabled == "True":
                timestamp_step_submitted = time.strftime("%Y-%m-%dT%H:%M:%S-%Z")
                try:
                    action = emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])

                    logger.debug("### Debug mode enabled ###")
                    logger.debug("EMR Step: {}".format(step))
                    logger.debug("EMR Step timestamp_submitted: {}".format(timestamp_step_submitted))
                    logger.debug("EMR Cluster_id: {}".format(cluster_id))
                    logger.debug("Added step:  {}".format(action))
                except ClientError as e:
                    if e.operation_name == 'AddJobFlowSteps' and e.response['Error']['Message'] == STEPS_EXCEEDED:
                        logger.info('The maximum number of steps for cluster exceeded')
                        rule_status = check_spark_submit_rule_enabled()
                        if rule_status == 'DISABLED':
                            set_spark_submit_rule_status('ENABLED')
                        else:
                            logger.info('The Spark Submit Rule is enabled, exiting')
                            return
                    else:
                        msg_exception = "EMR Add Steps Exception: {}".format(e)
                        logger.error(msg_exception)
                        send_notification(
                            SNS_TOPIC_ARN,
                            "Data Lake: Spark Submit Exception",
                            "Lambda Function Name: {}\n{}".format(context.function_name, msg_exception)
                        )
                        return 'Error Sending Job Flow Steps'
                    return 'Finished sending step Jobs but with more on queue'

                # If we were able to send the job we need to update the DDB table with the new Status
                try:
                    response = table_stage.update_item(
                        TableName=DYNAMO_DB_STAGE_TABLE,
                        Key={
                            's3_object_name_stage': s3_object_name_stage
                        },
                        UpdateExpression="set hive_table_analytics = :hive_table_analytics,"
                                         "hive_database_analytics = :hive_database_analytics,"
                                         "s3_target = :s3_target,"
                                         "timestamp_step_submitted = :timestamp_step_submitted,"
                                         "file_status = :file_status",
                        ExpressionAttributeValues={
                            ':hive_table_analytics': hive_table_analytics,
                            ':hive_database_analytics': hive_database_analytics,
                            ':s3_target': s3_target,
                            ':timestamp_step_submitted': timestamp_step_submitted,
                            ':file_status': DatalakeStatus.PROCESSING}
                       )
                    logger.info('DynamoDB update response: {}'.format(response))
                except Exception as e:
                    msg_exception = "DynamoDB Stage Update Item Exception: {}".format(e)
                    logger.error(msg_exception)
                    send_notification(
                        SNS_TOPIC_ARN,
                        "Data Lake: Spark Submit Exception",
                        "Lambda Function Name: {}\n{}".format(context.function_name, msg_exception)
                    )
                    return

            else:
                logger.info("The program is not enabled: {}".format(spark_program_s3_path))
        else:
            logger.info('There is no items returned from DynamoDB')

    if skip:
        # We are running from a scheduled rule and there is no more jobs to submit
        # Let's disable the scheduled rule
        logger.info('Disabling Scheduled Event Rule due to no more jobs to submit')
        set_spark_submit_rule_status('DISABLED')

    logger.info('Finished processing the Spark Submit function')
    return


if __name__ == '__main__':

    class Context(object):
        def __init__(self):
            self.function_name = 'mock'
            self.function_version = '1.0'

    event_mock = {"skip": DatalakeStatus.PROCESSING}
    context_mock = Context()
    lambda_handler(event_mock, context_mock)
