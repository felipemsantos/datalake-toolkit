import json
import os
from datetime import datetime

import awswrangler as wr
import boto3
import pandas as pd
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

TARGET_BUCKET = os.getenv("TARGET_BUCKET", None)
DATA_SOURCE = os.getenv("DATA_SOURCE", None)
TABLE_NAME = os.getenv("TABLE_NAME", None)
ENVIRONMENT = os.getenv("ENVIRONMENT", None)

tracer = Tracer()
logger = Logger()
metrics = Metrics()
metrics.add_dimension(name="environment", value=ENVIRONMENT)

# Global variables are reused across execution contexts (if available)
AWS_PROFILE = os.getenv("AWS_PROFILE", None)
AWS_REGION = os.getenv("AWS_REGION", None)
boto3.setup_default_session(region_name=AWS_REGION, profile_name=AWS_PROFILE)


@metrics.log_metrics(capture_cold_start_metric=True)
@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """
    It handles S3 notifications delivered by a SNS Topic
    :param event: A SNS Topic Event
    :param context: The Lambda Context
    :return: {
            "incoming_bytes": 0,
            "incoming_records": 0
        }
    """
    try:
        current_timestamp = datetime.utcnow()
        ingestion_date = current_timestamp.strftime("%Y-%m-%d")
        ingestion_time = current_timestamp.strftime("%H:%M:%S")

        result = {
            "incoming_bytes": 0,
            "incoming_records": 0
        }
        # orders-cached/account=mercatto/1050320422689-01
        for sns_event in event["Records"]:
            sns_message = json.loads(sns_event["Sns"]["Message"])
            for s3_notification in sns_message["Records"]:
                s3_event = s3_notification["s3"]
                source_bucket = s3_event["bucket"]["name"]
                file_path_source = s3_event["object"]["key"]
                result["incoming_bytes"] += s3_event["object"]["size"]
                _, _, file_name = file_path_source.split("/")
                source_path = f"s3://{source_bucket}/{file_path_source}"
                logger.info(f"Processing {source_path}")
                data_frame = wr.s3.read_json(path=source_path, **{"orient": "index"})
                target_path = f"s3://{TARGET_BUCKET}/{DATA_SOURCE}/{TABLE_NAME}/ingestion_date={ingestion_date}/ingestion_time={ingestion_time}/{file_name}-{current_timestamp.isoformat()}.json"
                logger.info(f"Storing data at {target_path}")
                wr.s3.to_json(df=data_frame, path=target_path)
                result["incoming_records"] += 1

        metrics.add_metric(name="incoming_bytes", unit=MetricUnit.Bytes, value=result["incoming_bytes"])
        metrics.add_metric(name="incoming_records", unit=MetricUnit.Count, value=result["incoming_records"])
        return result
    except Exception as e:
        logger.exception(e)
        raise e
