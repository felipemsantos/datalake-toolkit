#!/usr/bin/env bash

host='sampledb.cmenuboj2lju.us-east-2.rds.amazonaws.com'
domain='ACS'
database='employees'
table='employees'
output_filename='sampledb.csv'
dest_bucket='koiker-bigdata-raw'
dest_path='service/customer/db_sample/tb_sample'
hdfs_host=$(hdfs getconf -namenodes)
echo 'Getting USER/PASS from Parameter Store'
user_pass=$(aws ssm get-parameter --name dev-acsdomain-credentials --with-decryption | jq .Parameter.Value)

[[ "$user_pass" =~ \"(.*):(.*)\" ]] &&
	user=${BASH_REMATCH[1]}
	pass=${BASH_REMATCH[2]}
export ACCUMULO_HOME=/var/lib/accumulo
export HADOOP_LOG_DIR=/home/hadoop
export HADOOP_CONF_DIR=/etc/hadoop/conf
# Set the log level, output
# Other options: 'INFO,NullAppender', 'WARN, console'
export HADOOP_ROOT_LOGGER='INFO,console'
echo "Connecting to server: $host"
sqoop list-databases \
    --connect "jdbc:mysql://$host/$database" \
    --username ${user} \
    --password ${pass}

# echo "Listing tables from database: $database"
# sqoop list-tables --connect "jdbc:jtds:sqlserver://$host/$database;useNTLMv2=true;domain=$domain" --username $user --password $pass 

echo "Importing table $table from database: $database"
sqoop   import \
        --table ${table} \
        --delete-target-dir \
        --target-dir "hdfs://$hdfs_host/service/customer/db_sample/tb_sample" \
        --fields-terminated-by '\001' \
        --hive-delims-replacement '\\n' \
        --connect "jdbc:mysql://$host/$database" \
        --username ${user} \
        --password ${pass}

echo "Merging part files into $output_filename"
hadoop fs -getmerge "hdfs://$hdfs_host/service/customer/db_sample/tb_sample" $output_filename

date=$(date +%Y-%m-%d)
echo "Copying local file to S3"
aws s3 cp "$output_filename" "s3://$dest_bucket/$dest_path/$date/$output_filename"
# Copying from S3 to S3 is faster than uploading the file again
aws s3 cp "s3://$dest_bucket/$dest_path/$date/$output_filename" "s3://$dest_bucket/$dest_path/latest/$output_filename"


echo "Deleting local file"
rm ${output_filename}
hadoop fs -rm -R "/servicedesk"

echo "Finished import job"

