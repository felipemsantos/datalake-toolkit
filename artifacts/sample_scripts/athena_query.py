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
# Sample Athena Query
# Author: Rafael M. Koike
# Date: 2018-07-06
import time
import boto3


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
    athena_client = boto3.client('athena')
    s3_client = boto3.client('s3')
    # SQL QUERY
    query = 'SHOW DATABASES;'
    query_id_databases = athena_client.start_query_execution(QueryString=query,
                                                             ResultConfiguration={
                                                                 'OutputLocation': 's3://aws-athena-query-results-109881088269-us-east-2'
                                                             })
    resp = query_waiter(query_id_databases['QueryExecutionId'])
    if resp in ['TIMEOUT', 'FAILED', 'CANCELLED']:
        print('Query result: {}'.format(resp))
        raise Exception('Unable to complete the Athena Query')

    # The result is inside the ResultSet but need to extract the values
    print('Query executed successfully!')
    print('Query result: {}'.format(resp.get('ResultSet')))
    for row in resp.get('ResultSet').get('Rows'):
        print(row.get('Data'))
