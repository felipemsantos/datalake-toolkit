# create lambda function

aws lambda create-function --region us-east-1 --function-name odl_create_emr_cluster --s3-bucket it.centauro --s3-key lambda-function/odl_create_emr_cluster/odl_create_emr_cluster.zip --role arn:aws:iam::758458753684:role/lambda_basic_execution --environment "Variables={REGION=us-east-1,SNS_TOPIC_ARN=arn:arn:aws:sns:us-east-1:758458753684:odl-datalake,INSTANCE_TYPE_TASK=m4.xlarge,SUBNET_ID_FOR_CLUSTER=subnet-990fadb6,INSTANCE_COUNT_TASK_NODE=1,CLUSTER_LABEL=centauro-emr-datalake,EMR_RELEASE=emr-5.10.0,MY_LOG_BUCKET=s3://it.centauro/odl-emr-logs/,S3_BOOTSTRAP_BUCKET=it.centauro,INSTANCE_TYPE_CORE=m4.xlarge,EC2_KEYPAIR_NAME=odl-emr-keypair,INSTANCE_TYPE_MASTER=m4.xlarge,REGION=us-east-1,DYNAMO_DB_STAGE_TABLE=odl_stage_control}" --handler odl_create_emr_cluster.lambda_handler --runtime python2.7 --profile default --version $LASTEST

#Lambda Deploy 

-- subir uma instancia ec2 t2.micro e conecte-se via ssh
export LC_CTYPE=en_US.UTF-8
sudo su

sudo echo "LANG=en_US.utf-8" >> /etc/environment
sudo echo "LC_ALL=en_US.utf-8" >> /etc/environment
sudo echo "LC_CTYPE=en_US.UTF-8" >>  /etc/environment

exit

sudo pip install virtualenvwrapper

echo "export WORKON_HOME=$HOME/.virtualenvs" >> .bashrc
echo "export PROJECT_HOME=$HOME/Devel" >> .bashrc
echo "source /usr/local/bin/virtualenvwrapper.sh" >> .bashrc

source .bashrc

workon

mkdir Devel

mkproject odl_create_emr_cluster

workon odl_create_emr_cluster

sudo pip install boto3

cd .virtualenvs/odl_create_emr_cluster

zip -9 ~/odl_create_emr_cluster.zip 

echo $VIRTUAL_ENV

cd $VIRTUAL_ENV/lib/python2.7/site-packages

zip -r9 ~/odl_create_emr_cluster.zip *

cd $VIRTUAL_ENV/lib64/python2.7/site-packages

zip -r9 ~/odl_create_emr_cluster.zip *

cd $VIRTUAL_ENV

aws s3 cp s3://it.centauro/lambda-function/odl_create_emr_cluster/odl_create_emr_cluster.py .

zip -g ~/odl_create_emr_cluster.zip odl_create_emr_cluster.py 

aws s3 cp ~/odl_create_emr_cluster.zip s3://it.centauro/lambda-function/odl_create_emr_cluster/

aws lambda update-function-code --function-name odl_create_emr_cluster  --s3-bucket it.centauro --s3-key lambda-function/odl_create_emr_cluster/odl_create_emr_cluster.zip

aws lambda update-function-configuration --function-name odl_create_emr_cluster --region us-east-1 --environment "Variables={REGION=us-east-1,SNS_TOPIC_ARN=arn:arn:aws:sns:us-east-1:758458753684:odl-datalake,INSTANCE_TYPE_TASK=m4.xlarge,SUBNET_ID_FOR_CLUSTER=subnet-990fadb6,INSTANCE_COUNT_TASK_NODE=1,CLUSTER_LABEL=centauro-emr-datalake,EMR_RELEASE=emr-5.10.0,MY_LOG_BUCKET=s3://it.centauro/odl-emr-logs/,S3_BOOTSTRAP_BUCKET=it.centauro,INSTANCE_TYPE_CORE=m4.xlarge,EC2_KEYPAIR_NAME=odl-emr-keypair,INSTANCE_TYPE_MASTER=m4.xlarge,REGION=us-east-1,DYNAMO_DB_STAGE_TABLE=odl_stage_control}" --profile default --handler odl_create_emr_cluster.lambda_handler --timeout 300



