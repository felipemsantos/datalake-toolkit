#!/bin/bash
sudo mkdir /home/hadoop/code
sudo aws s3 sync $1 /home/hadoop/code/

jobId=`cat /mnt/var/lib/info/job-flow.json | jq -r ".jobFlowId"`

finished=1
while [ $finished -eq 1 ]; do
    echo "loop"
    finished=`aws emr describe-cluster --cluster-id ${jobId} | grep RESIZING | wc -l`

done