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
# Dummy processor

from __future__ import print_function

import logging
import os


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading processor SKIP_FILE')


# This pattern is to skip because it's the historical data
# servicedesk/customer/ca_sdm/tb_call_req/2018-07-02/call_req.csv
REGEX = r"([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+)/(\d{4}-\d{2}-\d{2})/([a-zA-Z0-9_]+.csv)"


def processor(**kwargs):
    bucket = kwargs.get('bucket')
    key = kwargs.get('key')
    logger.info('The file: {}/{} is skipped'.format(bucket, key))
    return
