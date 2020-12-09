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

import logging
import os
import boto3
import time
from common import es_put
import json

# REGION NAME
REGION = os.environ['AWS_DEFAULT_REGION']

# SNS topic to post email alerts to
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

# DynamoDB table to Datalake Control
DYNAMO_DB_CONTROL = os.environ['DYNAMO_DB_CONTROL']

# S3 bucket athena query results
BUCKET_ATHENA_QUERY_OUTPUT = os.environ['BUCKET_ATHENA_QUERY_OUTPUT']

# S3 key athena query results
KEY_ATHENA_QUERY_OUTPUT = os.environ['KEY_ATHENA_QUERY_OUTPUT']

# Athena Query Results state
STATE = ['RUNNING', 'QUEUED']

# level of recursion of this function.
# 0 = No recursion (slowest process but the recommended way to run locally)
# 1 = Only databases will be invoked recursively
# 2 = Databases and Tables will be invoked recursively
RECURSION = 1

sns_client = boto3.client('sns')
dynamodb_client = boto3.resource('dynamodb', region_name=REGION)
athena_client = boto3.client('athena')
lambda_client = boto3.client('lambda')
# logging.basicConfig(level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading Lambda Function {}'.format(__name__))


def lambda_handler(event, context):
    logger.debug('Event: {}'.format(event))
    recursivity = event.get('Recursivity', RECURSION)
    database = event.get('Database')
    tablename = event.get('Table')
    if database and tablename:
        send_table_to_es(database, tablename)
        return 'Recursive lambda function send_table_to_es() finished'
    elif database:
        query_database(database, context, recursivity)
        return 'Recursive lambda function query_database() finished'

    query = 'show databases;'
    query_id_databases = athena_client.start_query_execution(QueryString=query,
                                                             ResultConfiguration={'OutputLocation': 's3://{}/{}'.format(
                                                                BUCKET_ATHENA_QUERY_OUTPUT,
                                                                KEY_ATHENA_QUERY_OUTPUT)
                                                             })

    logger.debug(query_id_databases)

    resp = query_waiter(query_id_databases['QueryExecutionId'])
    if resp in ['TIMEOUT', 'FAILED', 'CANCELLED']:
        logger.debug('Query result: {}'.format(resp))
        raise Exception('Unable to complete the Athena Query')

    query_databases = resp.get('ResultSet', {}).get('Rows')
    if query_databases:
        logger.info(query_databases)
    else:
        logger.error('GetQueryResults is empty!')
        raise Exception('Failed to execute the Hive Catalog')

    for column_properties in query_databases:
        database = column_properties['Data'][0]['VarCharValue']
        if recursivity:
            # Invoke Lambda function with the database and table name
            try:
                response = lambda_client.invoke(
                    FunctionName=context.function_name,
                    InvocationType='Event',
                    Payload=json.dumps({'Database': database})
                )
                if response['StatusCode'] == 200:
                    logger.info('Invoked recursive lambda with Database: {}'.format(database))
            except Exception as e:
                logger.error('Error invoking recursive lambda - query_database()')
                logger.debug('Error: {}'.format(e))
            # This wait time is to avoid Throttling because Athena has a  soft limit of 5 concurrent queries
            time.sleep(3)
        else:
            query_database(database, context, recursivity)


def query_database(database, context, recursivity):
    logger.info(database)
    logger.debug("### Debug mode enabled ###")
    logger.debug("Database: {}".format(database))
    query = 'show tables;'
    query_id_tables = athena_client.start_query_execution(QueryString=query,
                                                          QueryExecutionContext={'Database': database},
                                                          ResultConfiguration={'OutputLocation': 's3://{}/{}'.format(
                                                               BUCKET_ATHENA_QUERY_OUTPUT,
                                                               KEY_ATHENA_QUERY_OUTPUT)
                                                          })
    resp = query_waiter(query_id_tables['QueryExecutionId'])
    if resp in ['TIMEOUT', 'FAILED', 'CANCELLED']:
        logger.debug('Query result: {}'.format(resp))
        raise Exception('Unable to complete the Athena Query')

    query_tables = resp.get('ResultSet', {}).get('Rows', [])

    for column_data in query_tables:
        tablename = column_data['Data'][0]['VarCharValue']
        if recursivity > 1:
            # Invoke Lambda function with the database and table name
            try:
                response = lambda_client.invoke(
                    FunctionName=context.function_name,
                    InvocationType='Event',
                    Payload=json.dumps({'Database': database, 'Table': tablename})
                )
                if response['StatusCode'] == 200:
                    logger.info('Invoked recursive lambda with Database: {} / Table: {}'.format(database, tablename))
            except Exception as e:
                logger.error('Error invoking recursive lambda - send_table_to_es()')
                logger.debug('Error: {}'.format(e))

            # This wait time is to avoid Throttling because Athena has a  soft limit of 5 concurrent queries
            time.sleep(3)
        else:
            send_table_to_es(database, tablename)


def send_table_to_es(database, tablename):
    table = tablename
    logger.info(table)
    logger.debug("### Debug mode enabled ###")
    logger.debug("Table: {}".format(table))
    query = 'describe ' + str(table) + ';'
    query_id_table = athena_client.start_query_execution(QueryString=query,
                                                         QueryExecutionContext={'Database': database},
                                                         ResultConfiguration={
                                                             'OutputLocation': 's3://{}/{}'.format(
                                                                 BUCKET_ATHENA_QUERY_OUTPUT,
                                                                 KEY_ATHENA_QUERY_OUTPUT)
                                                         })

    resp = query_waiter(query_id_table['QueryExecutionId'])
    if resp in ['TIMEOUT', 'FAILED', 'CANCELLED']:
        logger.debug('Query result: {}'.format(resp))
        raise Exception('Unable to complete the Athena Query')

    query_columns = resp.get('ResultSet', {}).get('Rows', [])

    dict_column_name = dict()
    dict_column_tags = list()
    dict_comment_tags = list()
    for item in query_columns:
        column = item['Data'][0]['VarCharValue']
        column_name, _, column_comment = [x.strip() for x in column.split('\t')]
        if not column_comment:
            column_comment = "empty"
        logger.debug("column name    : {}".format(column_name))
        logger.debug("column comment : {}".format(column_comment))

        if column_name and '#' not in column_name:
            dict_column_tags.append(column_name)
            logger.debug('Sending column_name: {} to ES datalake-tags'.format(column_name))
            resp = es_put(es_index='datalake-tags',
                          es_type='_doc',
                          es_id='{}-{}-{}'.format(database, table, column_name),
                          json={'tag': column_name})
            logger.debug('ES PUT response: {}'.format(resp))
            if resp is not None:
                logger.debug('ES Put response code: {}'.format(resp.status_code))
                logger.debug('ES Put response: {}'.format(resp.text))
                if not 200 <= resp.status_code <= 299:
                    logger.error('Error sending data to ES Catalog')
                    logger.error('Error: {}'.format(resp.text))
            else:
                logger.debug('There is no ES_ENDPOINT configured')
            if column_comment:
                dict_comment_tags.append(column_comment)
                dict_column_name[column_name] = column_comment
        else:
            logger.debug('Not adding this column_name/column_comment because one or both are missing')

    json_data = {
        "database": database,
        "table": table,
        "column_tags": " ".join(dict_column_tags),
        "comment_tags": " ".join(dict_comment_tags),
        "columns": dict_column_name
    }
    logger.info("JSON to catalog on ES: {}".format(json.dumps(json_data)))
    # Send data to Catalog (ElasticSearch)
    try:
        resp = es_put(es_index='datalake-hive',
                      es_type='_doc',
                      es_id='{}-{}'.format(database, table),
                      json=json_data)
    except Exception as e:
        logger.error('Error executing Elastic Search Put')
        logger.error('Error: {}'.format(e))

    logger.info('ES PUT response: {}'.format(resp))
    if resp is not None:
        logger.debug('ES Put response code: {}'.format(resp.status_code))
        logger.debug('ES Put response: {}'.format(resp.text))
        if not 200 <= resp.status_code <= 299:
            logger.error('Error sending data to ES Catalog')
            logger.error('Error: {}'.format(resp.text))
    else:
        logger.debug('There is no ES_ENDPOINT configured')


def query_waiter(execution_id, timeout=60):
    """
    query_waiter receives an Athena execution id and pool the Athena API till you have a SUCCEEDED/FAILED/CANCELLED
    reply or the timeout reached (default is 60 seconds)
    The return is the string: SUCCEEDED/FAILED/CANCELLED/TIMEOUT

    :param execution_id: string
    :param timeout: integer
    :return: string
    """
    start = time.time()
    while True:
        stats = athena_client.get_query_execution(QueryExecutionId=execution_id)
        status = stats['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            if status == 'SUCCEEDED':
                status = athena_client.get_query_results(
                    QueryExecutionId=execution_id)
            return status
        time.sleep(0.2)  # 200ms
        # Exit if the time waiting exceed the timeout seconds
        if time.time() > start + timeout:
            return 'TIMEOUT'


if __name__ == '__main__':
    class MockContext(object):
        def __init__(self):
            self.function_name = 'mock'

    mock_event = {'Recursivity': 0}
    mock_context = MockContext()
    lambda_handler(mock_event, mock_context)
