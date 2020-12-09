# -*- coding: utf-8 -*-
#
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
# Lambda must have permissions to create EMR clusters and to publish SNS messages
# See provided example policy json.
# Change all parameters in the run_job_flow function to customize your cluster!
# Create prefix /bootstrap under the s3 bootstrap bucket and place all scripts in it
#
# Triggers based on CloudWatch Events
# Lambda Function that process S3 events and copy new objects
# to destination bucket
# Author: Samuel Otero Schmidt (samschs@amazon.com)
# Date: 2017-02-27

from __future__ import print_function

import logging
import os
import json

import boto3

from common import send_notification, cluster_is_running

# label that will uniquely identify this cluster, also used as cluster name e.g. "daily-reporting-emr"
label = os.getenv('CLUSTER_LABEL')

# SNS topic to post email alerts (users must subscribe to this topic)
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')

# S3 bucket where the bootstrap keys are located, install scripts in prefix "/bootstrap"
S3_BOOTSTRAP_BUCKET = os.getenv('S3_BOOTSTRAP_BUCKET')

# S3 bucket for EMR logs
S3_LOG_URI = os.getenv('MY_LOG_BUCKET')

# Name of the EC2 ssh key pair
EC2_KEYPAIR = os.getenv('EC2_KEYPAIR_NAME')

# Subnet where the cluster will be installed
EC2_SUBNET_ID = os.getenv('SUBNET_ID_FOR_CLUSTER')

# EMR release - example: 'emr-5.1.0'
EMR_RELEASE = os.getenv('EMR_RELEASE')

# EMR Role (If missing we assume EMR_DefaultRole)
EMR_ROLE = os.getenv('EMR_ROLE', 'EMR_DefaultRole')
EMR_EC2_ROLE = os.getenv('EMR_EC2_ROLE', 'Datalake_EMR_EC2_Role')

# EMR Custom AMI (Default is False)
EMR_CUSTOM_AMI = os.getenv('EMR_CUSTOM_AMI', False)
EMR_CUSTOM_AMI_ID = os.getenv('EMR_CUSTOM_AMI_ID', None)

# Instance type for Master Node
INSTANCE_TYPE_MASTER = os.getenv('INSTANCE_TYPE_MASTER', 'm4.large')

# Instance type for Core Node
INSTANCE_TYPE_CORE = os.getenv('INSTANCE_TYPE_CORE', 'm4.large')

# Instance type for Task Node
INSTANCE_TYPE_TASK = os.getenv('INSTANCE_TYPE_TASK', 'm4.large')

# Amount of Core Nodes
INSTANCE_COUNT_CORE_NODE = os.getenv('INSTANCE_COUNT_CORE_NODE', 1)

# Amount of Task Nodes
INSTANCE_COUNT_TASK_NODE = os.getenv('INSTANCE_COUNT_TASK_NODE', 1)

# Environment (DEV/QA/PROD)
ENVIRONMENT = os.getenv('ENVIRONMENT', 'DEV')

# REGION NAME
REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

# DYNAMO_DB_STAGE_TABLE
DYNAMO_DB_STAGE_TABLE = os.getenv('DYNAMO_DB_STAGE_TABLE')

# ENI for EMR Master node
ENI_MASTER = os.getenv('ENI_MASTER')

# EMR local home path
EMR_HOME_SCRIPTS = os.getenv('EMR_HOME_SCRIPTS', '/home/hadoop')

# Do not modify below this line, except for job_flow
emr_client = boto3.client('emr')
sns_client = boto3.client('sns')
s3_client = boto3.client('s3')
dynamodb_client = boto3.resource('dynamodb', region_name=REGION)
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading Lambda Function {}'.format(__name__))
    
def stage_is_empty():
    try:
        table_stage = dynamodb_client.Table(DYNAMO_DB_STAGE_TABLE)
        results_stage = table_stage.scan()
        if results_stage.get('Items'):
            return False
        else:
            return True

    except Exception as e:
        logger.error("Error Reading DynamoDB Table: {}".format(e))
        send_notification(
            sns_arn=SNS_TOPIC_ARN,
            subject='Datalake:{} Create EMR Cluster error'.format(ENVIRONMENT),
            message=str(e)
        )
        raise e


def create_cluster():
    logger.info('There is no Cluster created to execute the jobs')
    logger.info('We are going to create a new one to run the jobs.')
    
    # JSON
    args = {
        "Name":
        label,
        "LogUri":
        "s3://{}".format(S3_LOG_URI),
        "ReleaseLabel":
        EMR_RELEASE,
    }
    if EMR_CUSTOM_AMI:
        args.update({
            "CustomAmiId": (EMR_CUSTOM_AMI_ID)
        })
        
    args.update({
        "Instances": {
            "InstanceGroups": [
                {
                    "InstanceRole": "MASTER",
                    "InstanceType": str(INSTANCE_TYPE_MASTER),
                    "Name": "Master instance group",
                    "InstanceCount": 1
                }, {
                    "InstanceRole": "CORE",
                    "InstanceType": str(INSTANCE_TYPE_CORE),
                    "Name": "Core instance group",
                    "InstanceCount": int(INSTANCE_COUNT_CORE_NODE),
                    "EbsConfiguration": {
                        "EbsBlockDeviceConfigs": [{
                            "VolumeSpecification": {
                                "SizeInGB": 500,
                                "VolumeType": "gp2"
                            },
                            "VolumesPerInstance": 1
                        }
                        ],
                        "EbsOptimized": True
                    }
                }, {
                    "InstanceRole": "TASK",
                    "InstanceType": str(INSTANCE_TYPE_TASK),
                    "Name": "Task instance group",
                    "InstanceCount": int(INSTANCE_COUNT_TASK_NODE)
                }
            ],
            "Ec2KeyName": EC2_KEYPAIR,
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
            "Ec2SubnetId": EC2_SUBNET_ID
        },
        "BootstrapActions": [{
            'Name': 'Install Libs and Bootstrap Scripts',
            'ScriptBootstrapAction': {
                'Path': 's3://{}/bootstrap/emr-bootstrap/install_libs.sh'.format(S3_BOOTSTRAP_BUCKET),
                'Args': [S3_BOOTSTRAP_BUCKET]
            }
        },
        {
            'Name': 'Boostrap ENI MASTER',
            'ScriptBootstrapAction': {
                'Path': 's3://{}/bootstrap/emr-bootstrap/emr-eni-proc.sh'.format(S3_BOOTSTRAP_BUCKET),
                'Args': [ENI_MASTER]
            }
        }],
        "Steps": [{
            'Name': 'Install the Manage cron job to terminate EMR cluster',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    '{}/manage_emr_shutdown_install.sh'.format(EMR_HOME_SCRIPTS),
                    '{}/manage_emr_shutdown.sh'.format(EMR_HOME_SCRIPTS),
                    S3_BOOTSTRAP_BUCKET,
                    SNS_TOPIC_ARN
                ]
            }
        }],
        "Applications": [{
            'Name': 'Hadoop'
        }, {
            'Name': 'Hive'
        }, {
            'Name': 'Oozie'
        }, {
            'Name': 'Ganglia'
        }, {
            'Name': 'Tez'
        }, {
            'Name': 'Hue'
        }, {
            'Name': 'Spark'
        }],
        "Configurations": [
            {
                "Classification": "emrfs-site",
                "Properties": {
                    "fs.s3.consistent.retryPeriodSeconds": "10",
                    "fs.s3.consistent": "true",
                    "fs.s3.consistent.retryCount": "5",
                    "fs.s3.consistent.metadata.tableName": "EmrFSMetadata"
                },
                "Configurations": [

                ]
            },
            {
                "Classification": "hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class":
                        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                },
                "Configurations": [
                ]
            },
            {
                "Classification": "spark-hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class":
                        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                },
                "Configurations": [
                ]
            },
        ],
        "VisibleToAllUsers":
        True,
        "JobFlowRole":
        EMR_EC2_ROLE,
        "ServiceRole":
        EMR_ROLE,
        "Tags": [
            {
                'Key': 'Role',
                'Value': 'EMR Data Lake'
            },
            {
                'Key': 'Environment',
                'Value': ENVIRONMENT
            },
            {
                'Key': 'Label',
                'Value': label
            },
            {
                'Key': 'Name',
                'Value': label
            }
        ]
    })

    # Create new EMR cluster
    emr_launch_message = 'Launching new EMR cluster: {}'.format(label)
    logger.info(emr_launch_message)
    send_notification(
        sns_arn=SNS_TOPIC_ARN,
        subject='Datalake:{} Create EMR Cluster message'.format(ENVIRONMENT),
        message=emr_launch_message
    )

    try:
        response = emr_client.run_job_flow(**args)
            
        return response
    except Exception as e:
        logger.error("RunJobFlow Exception: {}".format(e))
        send_notification(
            sns_arn=SNS_TOPIC_ARN,
            subject='Datalake:{} Create EMR Cluster Error'.format(ENVIRONMENT),
            message='Lambda Create EMR Cluster Error\nError message: {}'.format(e)
        )
        raise e


def lambda_handler(event, context):
    # check if there are files pending to be processed
    # We expect the Stage Table is a temporary table and as so with a predictable amount of rows
    # Otherwise we advise to avoid the use of scan API (that can lead to slow results)
    if stage_is_empty():
        logger.error('DynamoDB Stage Table {} is empty'.format(DYNAMO_DB_STAGE_TABLE))
        return 'There is no items in the Stage Table to process'

    if cluster_is_running(label, S3_LOG_URI, SNS_TOPIC_ARN, ENVIRONMENT):
        logger.info('There is a cluster running')
        return 'The Data lake cluster is already running'

    create_cluster()
    return "Finished creating the cluster(s) successfully"


if __name__ == '__main__':
    class Context:
        def __init__(self):
            self.function_name = 'mock'
            self.function_version = '1.0'

    mock_event = {}
    mock_context = Context()
    lambda_handler(mock_event, mock_context)
