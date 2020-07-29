import gzip
import json
import os
import urllib
import boto3


# Global variables are reused across execution contexts (if available)
AWS_PROFILE = os.getenv("AWS_PROFILE", None)
AWS_REGION = os.getenv("AWS_REGION", None)
boto3.setup_default_session(region_name=AWS_REGION, profile_name=AWS_PROFILE)


def lambda_handler(event, context):
    try:

    except Exception as e:

        raise e
