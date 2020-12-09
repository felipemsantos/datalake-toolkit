#!/usr/bin/env bash
set -x -e

#Install only on Master node
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
    echo "Installing Python Boto3 lib..."
    sudo pip install boto3
    echo "Installing Python Click lib..."
    sudo pip install click
    echo "Finished installing libs."
    echo "Copying bootstrap scripts to local folder"
    aws s3 cp s3://$1/bootstrap/emr-bootstrap/ /home/hadoop --recursive
    chmod +x /home/hadoop/*.sh
    echo "Finished copying bootstrap scripts"
fi