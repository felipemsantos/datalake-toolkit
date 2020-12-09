import json
import logging
import os

import boto3

AWS_PROFILE = os.getenv("AWS_PROFILE", None)
AWS_REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", None))
DEV_ENDPOINT_NAME = os.getenv("DEV_ENDPOINT_NAME", "glue-dev-endpoint")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
S3_CONFIG_BUCKET = os.getenv("S3_CONFIG_BUCKET", None)
S3_OBJECT_KEY = os.getenv("S3_OBJECT_KEY", "config/glue/dev_endpoint.json")

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

boto3_session = boto3.Session(region_name=AWS_REGION, profile_name=AWS_PROFILE)

glue = boto3_session.client("glue")
s3 = boto3_session.resource("s3")


def lambda_handler(event, context):
    logger.info(f"Running Lambda Function {context.function_name}")

    try:
        resp_dev_endpoint = glue.get_dev_endpoint(EndpointName=DEV_ENDPOINT_NAME)
        dev_endpoint = resp_dev_endpoint["DevEndpoint"]
        if dev_endpoint["Status"] != "READY":
            logger.warning(f"A Glue Dev Endpoint already exists with this name ({DEV_ENDPOINT_NAME}) but is not ready to use.")
        else:
            logger.warning(f"A Glue Dev Endpoint already exists with this name ({DEV_ENDPOINT_NAME}) and is ready to use.")
        return dev_endpoint
    except glue.exceptions.EntityNotFoundException as e:
        logger.warning(e)
        logger.warning("It means that there is not dev endpoint created")

    try:
        if not S3_CONFIG_BUCKET or S3_CONFIG_BUCKET == "":
            logger.error("The environment variable S3_CONFIG_BUCKET is required")
            raise ValueError("S3_CONFIG_BUCKET")

        logger.info(f"Creating a new Glue Dev Endpoint {DEV_ENDPOINT_NAME}")

        obj = s3.Object(S3_CONFIG_BUCKET, S3_OBJECT_KEY)
        params = json.loads(obj.get()['Body'].read().decode('utf-8'))
        dev_endpoint = glue.create_dev_endpoint(**params)
        return dev_endpoint
    except Exception as e:
        logger.error(e)
        raise e
