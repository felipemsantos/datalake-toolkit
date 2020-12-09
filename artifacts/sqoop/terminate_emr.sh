#!/usr/bin/env bash

SLEEP_TIME=10
echo "Waiting ${SLEEP_TIME} seconds"
sleep ${SLEEP_TIME}
#echo "Removing EmrFSMetadata from DynamoDB"
#emrfs delete-metadata

cluster_id=$(cat /mnt/var/lib/info/job-flow.json | jq -r ".jobFlowId")
echo "Terminating cluster: ${cluster_id}"
aws emr terminate-clusters --cluster-ids ${cluster_id}