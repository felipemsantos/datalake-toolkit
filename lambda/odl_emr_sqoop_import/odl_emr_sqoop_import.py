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
# Scheduled job to invoke EMR cluster with sqoop to import data
# Lambda Function for Data Lake Import
# Author: Rafael M. Koike
# Date: 2018-07-05
from __future__ import print_function

import logging
import os

import boto3
from common import cluster_is_running

# label that will uniquely identify this cluster, also used as cluster name e.g. "daily-reporting-emr"
label = os.getenv('CLUSTER_LABEL')

# SNS topic to post email alerts to
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

# EMR Role (If missing we assume EMR_DEfaultRole)
EMR_ROLE = os.getenv('EMR_ROLE', 'EMR_DefaultRole')

# EMR EC2 Role (If missing we assume EMR_DEfaultRole)
EMR_EC2_ROLE = os.getenv('EMR_EC2_ROLE', 'EMR_EC2_DefaultRole')

# Instance type for Master Node
INSTANCE_TYPE_MASTER = os.getenv('INSTANCE_TYPE_MASTER')

# Instance type for Core Node
INSTANCE_TYPE_CORE = os.getenv('INSTANCE_TYPE_CORE')

# Instance type for Task Node
INSTANCE_TYPE_TASK = os.getenv('INSTANCE_TYPE_TASK')

# Amount of Core Nodes
INSTANCE_COUNT_CORE_NODE = os.getenv('INSTANCE_COUNT_CORE_NODE')

# Amount of Task Nodes
INSTANCE_COUNT_TASK_NODE = os.getenv('INSTANCE_COUNT_TASK_NODE')

# Environment (DEV/QA/PROD)
ENVIRONMENT = os.getenv('ENVIRONMENT')

# REGION NAME
REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading Lambda Function {}'.format(__name__))

emr_client = boto3.client('emr')
sns_client = boto3.client('sns')


def build_instace_groups():
    instace_groups = [
        {
            "InstanceRole": "MASTER",
            "InstanceType": str(INSTANCE_TYPE_MASTER),
            "Name": "Master instance group",
            "InstanceCount": 1
        }
    ]
    if int(INSTANCE_COUNT_CORE_NODE) > 0:
        instace_groups += [
            {
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
            }
        ]
    if int(INSTANCE_COUNT_TASK_NODE) > 0:
        instace_groups += [{
            "InstanceRole": "TASK",
            "InstanceType": str(INSTANCE_TYPE_TASK),
            "Name": "Task instance group",
            "InstanceCount": int(INSTANCE_COUNT_TASK_NODE)}
        ]
    return instace_groups


def lambda_handler(event, context):
    # Check if any previous EMR cluster is still running
    if cluster_is_running(label, S3_LOG_URI, SNS_TOPIC_ARN, ENVIRONMENT):
        logger.info('There is a cluster running')
        return 'The Data lake cluster is already running'

    logger.info('There is no Cluster created to execute the job')
    logger.info('We are going to create a new one to run this job.')
    # Create new EMR cluster
    emr_launch_message = 'Launching new EMR cluster: {}'.format(label)
    logger.info(emr_launch_message)
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=emr_launch_message,
        Subject='Datalake Lambda message'
    )

    try:
        response = emr_client.run_job_flow(
            Name=label,
            LogUri=S3_LOG_URI,
            ReleaseLabel=EMR_RELEASE,
            VisibleToAllUsers=True,
            JobFlowRole=EMR_EC2_ROLE,
            ServiceRole=EMR_ROLE,
            Instances={
                'InstanceGroups': build_instace_groups(),
                'Ec2KeyName': EC2_KEYPAIR,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': EC2_SUBNET_ID
            },
            BootstrapActions=[
                {
                    'Name': 'Bootstrap scripts',
                    'ScriptBootstrapAction': {
                        'Path': 's3://{}/sqoop/install_libs.sh'.format(S3_BOOTSTRAP_BUCKET)
                    }
                },
            ],
            Steps=[
                {
                    'Name': 'Run Sqoop import jobs',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['/home/hadoop/run_jobs.sh', '{}'.format(SNS_TOPIC_ARN)]
                    }
                },
                {
                    'Name': 'Terminate Cluster',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['/home/hadoop/terminate_emr.sh']
                    }
                }
            ],
            Applications=[
                {
                    'Name': 'Hadoop'
                }, {
                    'Name': 'Hive'
                }, {
                    'Name': 'Pig'
                }, {
                    'Name': 'Sqoop'
                }, {
                    'Name': 'Hue'
                }, {
                    'Name': 'Spark'
                }
            ],
            Configurations=[
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
                        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    },
                    "Configurations": [
                    ]
                },
                {
                    "Classification": "presto-connector-hive",
                    "Properties": {
                        "hive.metastore.glue.datacatalog.enabled": "true"
                    },
                    "Configurations": [
                    ]
                },

                {
                    "Classification": "spark-hive-site",
                    "Properties": {
                        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    },
                    "Configurations": [
                    ]
                },
            ],
            Tags=[
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
        )
        return response
    except Exception as e:
        logger.error("RunJobFlow Exception: {}".format(e))
        try:
            sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message='Lambda: {lambda_name} version: {lambda_version}\nError: {error}'.format(
                    lambda_name=context.function_name,
                    lambda_version=context.function_version,
                    error=e),
                Subject='Datalake Lambda error: error: Failed to launch new EMR cluster.'
            )
        except Exception as ne:
            logger.error("SNS Exception: {}".format(ne))
        raise e
