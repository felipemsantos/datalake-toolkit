import logging
import os

import boto3

AWS_PROFILE = os.getenv("AWS_PROFILE", None)
AWS_REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", None))
DEV_ENDPOINT_NAME = os.getenv("DEV_ENDPOINT_NAME", "glue-dev-endpoint-test")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

boto3_session = boto3.Session(region_name=AWS_REGION, profile_name=AWS_PROFILE)

glue = boto3_session.client("glue")


def lambda_handler(event, context):
    logger.info(f"Running Lambda Function {context.function_name}")

    try:
        return glue.delete_dev_endpoint(EndpointName=DEV_ENDPOINT_NAME)
    except glue.exceptions.EntityNotFoundException as e:
        logger.error(e)
        raise e
