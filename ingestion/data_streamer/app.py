import json
import logging
import os
import urllib

import boto3

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

s3 = boto3.client("s3")
kinesis = boto3.client("kinesis")

# Enrionment Variables
KINESIS_STREAM = os.getenv("KINESIS_STREAM", None)


def clean_extra_data(original_data):
    json_data = json.loads(original_data)
    for info in json_data['ShippingData']['LogisticsInfo']:
        info['slas'] = ''
    for message in json_data['ConversationMessages']:
        message['Body'] = ''
    #json_data = {k:v for k, v in json_data.items() if k in ['Id', 'SellerOrderId', 'Origin', 'OrderGroup', 'Status', 'OrderId', 'CreationDate', 'IsCompleted', 'value', 'Hostname', 'LastChange']}
    return json.dumps(json_data)


def lambda_handler(event, context):
    try:
        records = []
        for sns_event in event['Records']:
            sns_message = json.loads(sns_event['Sns']['Message'])
            for s3_notification in sns_message['Records']:
                s3_event = s3_notification['s3']
                source_bucket = s3_event['bucket']['name']
                file_path_source = s3_event['object']['key']

                source_key = urllib.parse.unquote_plus(file_path_source)
                # get object content
                logger.info("bucket: " + source_bucket + " key: " + source_key)
                obj = s3.get_object(Bucket=source_bucket, Key=source_key)
                object_content_body = obj['Body'].read()

                object_content_body = clean_extra_data(object_content_body)

                record = {
                    'Data': object_content_body,
                    'PartitionKey': 'OMSData'
                }
                records.append(record)

        # sendo to kinesis
        put_records_result = kinesis.put_records(
            StreamName=KINESIS_STREAM,
            Records=records
        )
        return put_records_result
    except Exception as e:
        logger.exception(e)
        raise e
