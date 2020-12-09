# -*- coding: utf-8 -*-

from __future__ import print_function

import logging
import os
import sys
import time

import click

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading Job module.')
TIMEOUT = 15


@click.command()
@click.option('--timeout', default=TIMEOUT, help='Wait period in seconds. default is {}'.format(TIMEOUT))
def run(**kwargs):
    wait_time = kwargs.pop('timeout')
    logger.info('Waiting {} seconds'.format(wait_time))
    time.sleep(wait_time)
    logger.info('Wait is over! I\'m quiting...')
    sys.exit(0)


if __name__ == '__main__':
    logger.info('This is a dummy function to test the cluster')
    run()
