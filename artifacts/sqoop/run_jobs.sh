#!/usr/bin/env bash
# The run-parts in this server is just a shellscript and not the full program
# The only parameter this program accept is the dir :-(
# TODO: replace this run-parts script with the full run-parts programam or implement a loop

echo "Setting SNS Topic to $1"
export SNS_TOPIC=$1
JOBS_PATH=/home/hadoop/jobs
echo "Executing jobs..."
run-parts ${JOBS_PATH}