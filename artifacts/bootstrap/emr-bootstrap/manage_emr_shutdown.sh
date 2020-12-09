#!/bin/bash -x
#
# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
# http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.


# The EMR EC2 Instances role must have permissions to modify EMR clusters attributes, to terminate EMR clusters, to list EMR clusters and to publish SNS messages. 
# See provided example policy - manage_emr_shutdown_iam_policy.json
# Use the manage_emr_shutdown_install.sh to add this script to crontab on the EMR cluster nodes as bootstrap action. For example:
# line="* * * * * /usr/bin/flock -n /tmp/manage_emr_shutdown.lock bash /home/hadoop/manage_emr_shutdown.sh >> /var/log/manage_emr_shutdown.log 2>&1"
# (sudo crontab -u hadoop -l; echo "$line" ) | sudo crontab -u hadoop -


# Modify the variables below as needed:

# the time for the cluster to sleep after launch before initiation termination
SLEEP_TIME="15m"
# SNS topic to send administrator alert emails to
SNS_TOPIC_ARN=$2
# Bucket for the lock files location on S3 to check if we should sleep for a full interval
BOOTSTRAP_BUCKET=$1
# Do not modify below this point

REGION=$(curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region)
CLUSTER_ID=$(cat /mnt/var/lib/info/job-flow.json | jq -r ".jobFlowId")
LOCK_FILE="s3://${BOOTSTRAP_BUCKET}/bootstrap/${CLUSTER_ID}.lock"
STEPS=0
LOCKED=0

# First sleep for the required sleep time
sleep ${SLEEP_TIME}

while true; 
do
	# the lock file is created by Lambda and let us know that it executed but did not create a new cluster because spark_programs were running on the current cluster
	LOCKED=$(aws s3 ls ${LOCK_FILE} | wc -l)
	if [ "$LOCKED" -ne "0" ]; then
		# just sleep for another whole interval
		MESSAGE="$(date): The lambda has been invoked to start a new cluster and the cluster: ${CLUSTER_ID} is already running..."
        SUBJECT="New cluster request skipped because there is a cluster running"
        echo ${MESSAGE}
        aws sns publish --topic-arn ${SNS_TOPIC_ARN} --message "${MESSAGE}" --subject "${SUBJECT}" --region ${REGION}
		aws s3 rm ${LOCK_FILE}
	fi
    STEPS=$(aws emr list-steps --cluster-id ${CLUSTER_ID} --step-states PENDING RUNNING | jq .Steps[].Status.State | wc -l)
    echo "There are ${STEPS} in the queue"
    if [ "$STEPS" -ne "0" ]; then
        MESSAGE="$(date): Found ${STEPS} running or pending. Sleeping for ${SLEEP}..."
        SUBJECT="EMR Cluster shutdown postponed: ${CLUSTER_ID}"
        echo ${MESSAGE}
#        aws sns publish --topic-arn ${SNS_TOPIC_ARN} --message "${MESSAGE}" --subject "${SUBJECT}" --region ${REGION}
        sleep ${SLEEP_TIME}
    else
        MESSAGE="$(date): Terminating cluster ${CLUSTER_ID}"
        SUBJECT="EMR Cluster shutdown: ${CLUSTER_ID}"
        echo ${MESSAGE}
        aws sns publish --topic-arn ${SNS_TOPIC_ARN} --message "${MESSAGE}" --subject "${SUBJECT}" --region ${REGION}
        aws emr terminate-clusters --cluster-ids ${CLUSTER_ID} --region ${REGION}
        exit 0
    fi

done