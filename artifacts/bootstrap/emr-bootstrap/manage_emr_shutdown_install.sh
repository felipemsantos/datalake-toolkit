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
#
# Adds the manage_emr_shutdown.sh to crontab

# SCRIPT_LOCATION must be passed as full S3 path e.g. s3://my-bootstrap-bucket/bootstrap/manage_emr_shutdown.sh
SCRIPT_LOCATION=$1
ARTIFACTS_BUCKET=$2
SNS_TOPIC=$3
line="*/5 * * * * . $HOME/.bash_profile; /usr/bin/flock -n /tmp/manage_emr_shutdown.lock /home/hadoop/manage_emr_shutdown.sh ${2}  ${3}>> /var/log/manage_emr_shutdown.log 2>&1 &"
(sudo crontab -u hadoop -l; echo "$line" ) | sudo crontab -u hadoop -
