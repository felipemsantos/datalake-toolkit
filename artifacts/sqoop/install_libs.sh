#!/usr/bin/env bash

BUCKET="koiker-bigdata-artifacts"
FOLDER="sqoop"
FILES="run_jobs.sh terminate_emr.sh"

echo "Copying jdbc sqlserver libraries from s3..."
sudo aws s3 cp "s3://${BUCKET}/${FOLDER}/libs/" /usr/lib/sqoop/lib --recursive

echo "Creating /var/lib/accumulo path to avoid sqoop warnings"
sudo mkdir /var/lib/accumulo

echo "Copying jobs from s3..."
mkdir /home/hadoop/jobs
aws s3 cp "s3://${BUCKET}/${FOLDER}/jobs/" /home/hadoop/jobs --recursive
chmod +x -R /home/hadoop/jobs/*

for f in ${FILES}
do
    echo "Copying ${f} to local folder"
    aws s3 cp "s3://${BUCKET}/${FOLDER}/${f}" /home/hadoop
    sudo chmod +x "/home/hadoop/${f}"
done

echo "Changing files owner"
sudo chown hadoop:hadoop -R /home/hadoop
