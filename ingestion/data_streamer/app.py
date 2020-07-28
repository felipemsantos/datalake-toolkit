import gzip
import json
import os
import urllib

import awswrangler as wr
import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

DATA_SOURCE = os.getenv("DATA_SOURCE", None)
TABLE_NAME = os.getenv("TABLE_NAME", None)
ENVIRONMENT = os.getenv("ENVIRONMENT", None)
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", None)

tracer = Tracer()
logger = Logger()
metrics = Metrics()
metrics.add_dimension(name="Environment", value=ENVIRONMENT)

# Global variables are reused across execution contexts (if available)
AWS_PROFILE = os.getenv("AWS_PROFILE", None)
AWS_REGION = os.getenv("AWS_REGION", None)
boto3.setup_default_session(region_name=AWS_REGION, profile_name=AWS_PROFILE)

kinesis = boto3.client("kinesis")


@metrics.log_metrics(capture_cold_start_metric=True)
@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        result = {
            "StreamedBytes": 0,
            "StreamedRecords": 0
        }
        records = []
        for sns_event in event["Records"]:
            sns_message = json.loads(sns_event["Sns"]["Message"])
            for s3_notification in sns_message["Records"]:
                s3_event = s3_notification["s3"]
                source_bucket = s3_event["bucket"]["name"]
                file_path_source = urllib.parse.unquote_plus(s3_event["object"]["key"])
                result["StreamedBytes"] += int(s3_event["object"]["size"])
                _, _, file_name = file_path_source.split("/")
                source_path = f"s3://{source_bucket}/{file_path_source}"
                logger.info(f"Processing {source_path}")
                data_frame = wr.s3.read_json(path=source_path, **{"orient": "index"})
                compressed_data = gzip.compress(data_frame.to_dict())
                records.append({
                    "Data": compressed_data,
                    "PartitionKey": f"{DATA_SOURCE}.{TABLE_NAME}"
                })
                result["StreamedRecords"] += 1

        ret = kinesis.put_records(Records=records, StreamName=KINESIS_STREAM_NAME)

        metrics.add_metric(name="StreamedBytes", unit=MetricUnit.Bytes, value=result["StreamedBytes"])
        metrics.add_metric(name="StreamedRecords", unit=MetricUnit.Count, value=result["StreamedRecords"])

        if "FailedRecordCount" in ret:
            result["FailedStreamedRecord"] = ret["FailedRecordCount"]
            metrics.add_metric(name="FailedStreamedRecord", unit=MetricUnit.Count, value=result["FailedStreamedRecord"])
            logger.error(ret)

        return result
    except Exception as e:
        metrics.add_metric(name="StreamingError", unit=MetricUnit.Count, value=1)
        logger.exception(e)
        raise e
