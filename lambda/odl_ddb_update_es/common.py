# -*- coding: utf-8 -*-
#
# common.py
#
# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#    http://aws.amazon.com/asl/
#
# or in the "license" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# express or implied.
# See the License for the specific language governing permissions and limitations under the License.
#
#
# Common functions to Data Lake Lambda functions
import datetime
import hashlib
import hmac
import json
import logging
import os
import string

from urllib import quote

# http://python-future.org/compatible_idioms.html
# Python 2 and 3: alternative 4
try:
    from urllib.parse import urlparse

except ImportError:
    from urlparse import urlparse

from botocore.vendored import requests

import boto3

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))


# Key derivation functions. See:
# http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-python
def sign(key, msg):
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()


def get_signature_key(key, datetime_stamp, region_name, service_name):
    k_date = sign(('AWS4' + key).encode('utf-8'), datetime_stamp)
    k_region = sign(k_date, region_name)
    k_service = sign(k_region, service_name)
    k_signing = sign(k_service, 'aws4_request')
    return k_signing


def es_put(es_index, es_type, es_id, data):
    """
    Send documents to Elasticsearch
    the ES endpoint is get from Environment Variable ES_ENDPOINT
    This code is supposed to run on Lambda function

    :param es_index: string
    :param es_type: string
    :param es_id: string
    :param data: dict
    :return: object
    """
    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    endpoint = os.getenv('ES_ENDPOINT')
    logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
    if not endpoint:
        return None

    if endpoint[-1:] == '/':
        endpoint = endpoint[:-1]
    host = endpoint.replace('https://', '')
    session = boto3.Session()
    credentials = session.get_credentials()
    auth = AWSRequestsAuth(aws_access_key=credentials.access_key,
                           aws_secret_access_key=credentials.secret_key,
                           aws_token=credentials.token,
                           aws_host=host,
                           aws_region=region,
                           aws_service='es')
    url = '{}/{}/{}/{}'.format(endpoint, es_index, es_type, es_id)
    logger.debug('URL: {}'.format(url))
    response = requests.put(url, auth=auth, json=data)

    return response


# This part of the code is extracted from: https://github.com/DavidMuller/aws-requests-auth
# MIT License
# Copyright (c) David Muller.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
#     1. Redistributions of source code must retain the above copyright notice,
#        this list of conditions and the following disclaimer.
#
#     2. Redistributions in binary form must reproduce the above copyright
#        notice, this list of conditions and the following disclaimer in the
#        documentation and/or other materials provided with the distribution.
#
#     3. The names of its contributors may not be used to endorse or promote
#        products derived from this software without specific prior written
#        permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
class AWSRequestsAuth(requests.auth.AuthBase):
    """
    Auth class that allows us to connect to AWS services
    via Amazon's signature version 4 signing process
    Adapted from https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
    """

    def __init__(self,
                 aws_access_key,
                 aws_secret_access_key,
                 aws_host,
                 aws_region,
                 aws_service,
                 aws_token=None):
        """
        Example usage for talking to an AWS Elasticsearch Service:
        AWSRequestsAuth(aws_access_key='YOURKEY',
                        aws_secret_access_key='YOURSECRET',
                        aws_host='search-service-foobar.us-east-1.es.amazonaws.com',
                        aws_region='us-east-1',
                        aws_service='es',
                        aws_token='...')
        The aws_token is optional and is used only if you are using STS
        temporary credentials.
        """
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_host = aws_host
        self.aws_region = aws_region
        self.service = aws_service
        self.aws_token = aws_token

    def __call__(self, r):
        """
        Adds the authorization headers required by Amazon's signature
        version 4 signing process to the request.
        Adapted from https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
        """
        aws_headers = self.get_aws_request_headers_handler(r)
        r.headers.update(aws_headers)
        return r

    def get_aws_request_headers_handler(self, r):
        """
        Override get_aws_request_headers_handler() if you have a
        subclass that needs to call get_aws_request_headers() with
        an arbitrary set of AWS credentials. The default implementation
        calls get_aws_request_headers() with self.aws_access_key,
        self.aws_secret_access_key, and self.aws_token
        """
        return self.get_aws_request_headers(r=r,
                                            aws_access_key=self.aws_access_key,
                                            aws_secret_access_key=self.aws_secret_access_key,
                                            aws_token=self.aws_token)

    def get_aws_request_headers(self, r, aws_access_key, aws_secret_access_key, aws_token):
        """
        Returns a dictionary containing the necessary headers for Amazon's
        signature version 4 signing process. An example return value might
        look like
            {
                'Authorization': 'AWS4-HMAC-SHA256 Credential=YOURKEY/20160618/us-east-1/es/aws4_request, '
                                 'SignedHeaders=host;x-amz-date, '
                                 'Signature=ca0a856286efce2a4bd96a978ca6c8966057e53184776c0685169d08abd74739',
                'x-amz-date': '20160618T220405Z',
            }
        """
        # Create a date for headers and the credential string
        t = datetime.datetime.utcnow()
        amzdate = t.strftime('%Y%m%dT%H%M%SZ')
        datestamp = t.strftime('%Y%m%d')  # Date w/o time for credential_scope

        canonical_uri = AWSRequestsAuth.get_canonical_path(r)

        canonical_querystring = AWSRequestsAuth.get_canonical_querystring(r)

        # Create the canonical headers and signed headers. Header names
        # and value must be trimmed and lowercase, and sorted in ASCII order.
        # Note that there is a trailing \n.
        canonical_headers = ('host:' + self.aws_host + '\n' +
                             'x-amz-date:' + amzdate + '\n')
        if aws_token:
            canonical_headers += 'x-amz-security-token:' + aws_token + '\n'

        # Create the list of signed headers. This lists the headers
        # in the canonical_headers list, delimited with ";" and in alpha order.
        # Note: The request can include any headers; canonical_headers and
        # signed_headers lists those that you want to be included in the
        # hash of the request. "Host" and "x-amz-date" are always required.
        signed_headers = 'host;x-amz-date'
        if aws_token:
            signed_headers += ';x-amz-security-token'

        # Create payload hash (hash of the request body content). For GET
        # requests, the payload is an empty string ('').
        body = r.body if r.body else bytes()
        try:
            body = body.encode('utf-8')
        except (AttributeError, UnicodeDecodeError):
            # On py2, if unicode characters in present in `body`,
            # encode() throws UnicodeDecodeError, but we can safely
            # pass unencoded `body` to execute hexdigest().
            #
            # For py3, encode() will execute successfully regardless
            # of the presence of unicode data
            body = body

        payload_hash = hashlib.sha256(body).hexdigest()

        # Combine elements to create create canonical request
        canonical_request = (r.method + '\n' + canonical_uri + '\n' +
                             canonical_querystring + '\n' + canonical_headers +
                             '\n' + signed_headers + '\n' + payload_hash)

        # Match the algorithm to the hashing algorithm you use, either SHA-1 or
        # SHA-256 (recommended)
        algorithm = 'AWS4-HMAC-SHA256'
        credential_scope = (datestamp + '/' + self.aws_region + '/' +
                            self.service + '/' + 'aws4_request')
        string_to_sign = (algorithm + '\n' + amzdate + '\n' + credential_scope +
                          '\n' + hashlib.sha256(canonical_request.encode('utf-8')).hexdigest())

        # Create the signing key using the function defined above.
        signing_key = get_signature_key(aws_secret_access_key,
                                        datestamp,
                                        self.aws_region,
                                        self.service)

        # Sign the string_to_sign using the signing_key
        string_to_sign_utf8 = string_to_sign.encode('utf-8')
        signature = hmac.new(signing_key,
                             string_to_sign_utf8,
                             hashlib.sha256).hexdigest()

        # The signing information can be either in a query string value or in
        # a header named Authorization. This code shows how to use a header.
        # Create authorization header and add to request headers
        authorization_header = (algorithm + ' ' + 'Credential=' + aws_access_key +
                                '/' + credential_scope + ', ' + 'SignedHeaders=' +
                                signed_headers + ', ' + 'Signature=' + signature)

        headers = {
            'Authorization': authorization_header,
            'x-amz-date': amzdate,
        }
        if aws_token:
            headers['X-Amz-Security-Token'] = aws_token
        return headers

    @classmethod
    def get_canonical_path(cls, r):
        """
        Create canonical URI--the part of the URI from domain to query
        string (use '/' if no path)
        """
        parsedurl = urlparse(r.url)

        # safe chars adapted from boto's use of urllib.parse.quote
        # https://github.com/boto/boto/blob/d9e5cfe900e1a58717e393c76a6e3580305f217a/boto/auth.py#L393
        return quote(parsedurl.path if parsedurl.path else '/', safe='/-_.~')

    @classmethod
    def get_canonical_querystring(cls, r):
        """
        Create the canonical query string. According to AWS, by the
        end of this function our query string values must
        be URL-encoded (space=%20) and the parameters must be sorted
        by name.
        This method assumes that the query params in `r` are *already*
        url encoded.  If they are not url encoded by the time they make
        it to this function, AWS may complain that the signature for your
        request is incorrect.
        """
        canonical_querystring = ''

        parsedurl = urlparse(r.url)
        querystring_sorted = '&'.join(sorted(parsedurl.query.split('&')))

        for query_param in querystring_sorted.split('&'):
            key_val_split = query_param.split('=', 1)

            key = key_val_split[0]
            if len(key_val_split) > 1:
                val = key_val_split[1]
            else:
                val = ''

            if key:
                if canonical_querystring:
                    canonical_querystring += "&"
                canonical_querystring += u'='.join([key, val])

        return canonical_querystring


class ElasticSearchCatalog(object):
    def __init__(self, context, sns_arn, header):
        """
        ElasticSearchCatalog core Object to send data to ElasticSearch
        :param context: object
        :param sns_arn: string
        :param header: string
        """
        self._sns_client = boto3.client('sns')
        self._s3_client = boto3.client('s3')
        self._s3_resource = boto3.resource('s3')
        self._context = context
        self._sns_arn = sns_arn
        self._header = header
        self._first_line = 'no header'

    def send_to_catalog(self, key, data, indice):
        """
        Send data to Elasticsearch and skip if there is no env var ES_ENDPOINT set
        :param key: filename
        :type key: string
        :param data: dict object to be sent to Elasticsearch
        :type data: dict
        :return:
        """
        logger.debug("JSON to catalog on ES: {}".format(json.dumps(data)))
        # Sent data to Catalog (ElasticSearch)
        try:
            resp = es_put(es_index=indice,
                          es_type='_doc',
                          es_id=hashlib.md5(key).hexdigest(),
                          data=data)
        except Exception as e:
            logger.error('Error executing Elastic Search Put')
            logger.debug('Error: {}'.format(e))
            raise Exception('Error sending data to ES')

        logger.debug('ES PUT response: {}'.format(resp))
        if resp:
            logger.debug('ES Put response code: {}'.format(resp.status_code))
            logger.debug('ES Put response: {}'.format(resp.text))
            if not 200 <= resp.status_code <= 299:
                logger.error('Error sending data to ES Catalog')
                logger.debug('Error: {}'.format(resp.text))
        else:
            logger.debug('There is no ES_ENDPOINT configured')
        return resp


if __name__ == '__main__':
    print('Testing common functions')
    for i in range(10):
        mock_data = {"Name": "Robot{}".format(i),
                     "Address": "Address{}".format(i)}
        mock_resp = es_put(es_index='test', es_type='_doc', es_id=hashlib.md5(str(i)).hexdigest(), data=mock_data)
        print(mock_resp.text)
