#!/bin/bash
set -x -e

#Install only on Master node
if grep isMaster /mnt/var/lib/info/instance.json | grep true; then
    aws ec2 attach-network-interface --network-interface-id $1 --instance-id $(curl -s http://169.254.169.254/latest/meta-data/instance-id ) --device-index 1
fi
