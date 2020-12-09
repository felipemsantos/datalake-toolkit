from __future__ import print_function
from common import ElasticSearchCatalog

import logging
import os
import json_util as json

SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')

HEADER = os.getenv('HEADER')

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading Lambda Function {}'.format(__name__))


def lambda_handler(event, context):

    ingestion = ElasticSearchCatalog(context, SNS_TOPIC_ARN, HEADER)

    logger.info(event)

    logger.info("Invoked Lambda Function Name : " + context.function_name)
    count = 0
    for record in event['Records']:

        ddb_arn = record['eventSourceARN']
        ddb_table = ddb_arn.split(':')[5].split('/')[1].split('-')[3].lower()

        # Get the primary key for use as the Elasticsearch ID
        es_id = ""
        for item in record.get('dynamodb').get('Keys'):
            es_id += record['dynamodb']['Keys'][item]['S']

        logger.info('ElasticSearch id {}'.format(es_id))

        logger.debug('Sending to Catalog (ElasticSearch)...')

        if 'NewImage' not in record['dynamodb']:
            document = record['dynamodb']
        else:
            document = record['dynamodb']['NewImage']

        my_dict = json.loads(document)

        ingestion.send_to_catalog(es_id, my_dict, 'datalake-' + ddb_table)
        logger.info('Loading document {}'.format(my_dict))
        logger.debug('Finished the Datalake Ingestion process successfully')

        count += 1
    return str(count) + ' records processed.'


def clean_dict(document):
    local_dict = {}
    for item in document.keys():
        local_dict[item] = document.get(item).values()[0]
    return local_dict
