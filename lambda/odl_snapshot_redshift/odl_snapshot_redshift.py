# -*- coding: utf-8 -*-
#
# odl_snapshot_redshift.py
#
# Snapshot manual of the Redshift


from __future__ import print_function

import os
import logging
from datetime import datetime, timedelta

import boto3

# ACCOUNT
# account = os.environ['account']

# SNS topic to post email alerts to
sns_topic_arn = os.getenv('SNS_TOPIC_ARN')

# ENVIRONMENT
ENVIRONMENT = os.getenv('ENVIRONMENT', 'DEV')
ClusterId = os.getenv('CLUSTER_ID')

# LOGGING
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading Lambda Function {}'.format(__name__))


# Main function Lambda will run at start
def lambda_handler(event, context):
    logger.debug('Event received: {}'.format(event))
    logger.debug('Context: {}'.format(context))
    snapshot_deleted = redshift_snapshot_remover()
    snapshot_success = redshift_manual_snap()
    if snapshot_deleted and snapshot_success:
        return "Completed"
    else:
        return "Failure"


# Function to connect to AWS services
def connect(service):
    print('Connecting to AWS Service: ' + service)
    client = boto3.client(service)
    if client is None:
        msg = 'Connection to {} failed in'.format(service)
        logger.info(msg)
    #        notify_devops('Redshift manual snapshot Lambda function: FAILURE in account: ' + account, msg)
    return client


# Function to take manual snapshot of the latest automated snapshot for each Redshift Cluster
def redshift_manual_snap():
    logger.info('Running function for Redshift manual snapshot copy')
    try:
        client = connect('redshift')
        snap = []
        snapshots = client.describe_cluster_snapshots(SnapshotType='automated', ClusterIdentifier=ClusterId)
        if ENVIRONMENT.upper() != 'DEV':
            snap = snapshots["Snapshots"]

        # Sorting the snap dict (Most recent date first)
        sort_snap = sorted(snap, key=lambda x: x['SnapshotCreateTime'], reverse=True)

        '''
        Looping through the sorted dict and appending "Cluster Identifier" if not existing in "copied" array
         - creating cluster snapshot for each of the ClusterIdentifier added
        '''
        copied = []
        man_snaps = []

        for s in sort_snap:
            if not s['ClusterIdentifier'] in copied:
                logger.info('Found new cluster: ' + s['ClusterIdentifier'] + '. Adding to list.')
                copied.append(s['ClusterIdentifier'])
                logger.info('Taking manual snapshot from automated Snapshot ID: ' + s['SnapshotIdentifier'])
                logger.info('Cluster ID: {}'.format(ClusterId))

                SnapshotName = s['SnapshotIdentifier'][3:]

                logger.info('Snapshot Name: {}'.format(SnapshotName))

                response = client.create_cluster_snapshot(SnapshotIdentifier=s['SnapshotIdentifier'][3:],
                                                          ClusterIdentifier=ClusterId)
                logger.info('RETORNO {}'.format(response))
                if ENVIRONMENT.upper() != 'DEV':
                    logger.info('Adding manual snapshot to the list')
                    man_snaps.append(s['SnapshotIdentifier'][3:])
                    logger.info('Current snapshot list: ' + ', '.join(man_snaps))
        final_snaps = ', '.join(man_snaps)
        logger.info('The following manual snapshots were taken: ' + final_snaps)
        return True
    except Exception as e:
        logger.info(str(e))
        #        notify_devops('Redshift manual snapshot Lambda function: FAILURE in account: ' + account, str(e) +
        #        '. Please check Cloudwatch Logs')
        return False


# Function to remove manual snapshots which are older than specified in retention period variable
def redshift_snapshot_remover():
    print('Running function to remove old snapshots')
    try:
        client = connect('redshift')
        snapshots = client.describe_cluster_snapshots(SnapshotType='manual')
        snap = snapshots["Snapshots"]
        # Number of days to keep manual snapshots
        ret_period = os.getenv('ret_period')
        # Maximum days to look back for manual snapshots to avoid deleting old snapshots still needed
        max_back = os.getenv('max_back')
        del_snapshot = []

        logger.info(del_snapshot)

        removal_date = (datetime.now() - timedelta(days=int(ret_period))).date()
        max_look_back = (datetime.now() - timedelta(days=int(max_back))).date()
        # Looping through snap and removing snapshots which are older than retention period
        for s in snap:
            snap_date = s['SnapshotCreateTime'].date()
            # Condition for date older than retention period and condition to keep manual snapshots created by other
            # person in the past
            if (snap_date < removal_date) and (snap_date > max_look_back):
                logger.info('Found snapshot older than retention period: {}. Adding to the list.'.format(
                    s['SnapshotIdentifier']))
                del_snapshot.append(s['SnapshotIdentifier'])
                logger.info('Removing old snapshot: {}'.format(s['SnapshotIdentifier']))
                client.delete_cluster_snapshot(
                    SnapshotIdentifier=s['SnapshotIdentifier'],
                    SnapshotClusterIdentifier=s['ClusterIdentifier']
                )
        deleted_snapshots = ', '.join(del_snapshot)
        if not deleted_snapshots:
            logger.info('No snapshots were found to be deleted')
        else:
            logger.info('List of deleted snapshots: ' + deleted_snapshots)
        return True
    except Exception as e:
        logger.info(str(e))
        #        notify_devops('Redshift manual snapshot Lambda function: FAILURE in account: ' + account, str(e) + '
        #        . Please check Cloudwatch Logs')
        return False


# Function to notify DevOps team in case of a snapshot failure
def notify_devops(sub, msg):
    logger.info('Notifying DevOps team')
    client = connect('sns_topic_arn')
    pub_msg = client.publish(
        TopicArn=sns_topic_arn,
        Message=msg,
        Subject=sub
    )
    return pub_msg
