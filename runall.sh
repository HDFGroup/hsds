#!/bin/bash

# script to startup hsds docker containers
if [ $# -eq 1 ] && ([ $1 == "-h" ] || [ $1 == "--help" ]); then
   echo "Usage: runall.sh [count] [s3]"
   exit 1
fi


count=4
if [ $# -gt 0 ]; then 
  count=$1
fi


s3=0
if [ $# -gt 1 ]; then 
  s3=$2
fi

[ -z ${BUCKET_NAME} ] && echo "Need to set BUCKET_NAME" && exit 1
[ -z ${AWS_S3_GATEWAY} ] && echo "Need to set AWS_S3_GATEWAY" && exit 1
[ -z ${AWS_REGION} ] && echo "Need to AWS_REGION"  && exit 1
if [ -z $s3 ]; then
  # if not using s3 and EC2 IAM roles, need to define AWS access keys
  [ -z ${AWS_ACCESS_KEY_ID} ] && echo "Need to set AWS_ACCESS_KEY_ID" && exit 1
  [ -z ${AWS_SECRET_ACCESS_KEY} ] && echo "Need to set AWS_SECRET_ACCESS_KEY" && exit 1
fi

if [ $(docker ps -aq -f status=exited | wc -l) -gt 0 ]; then
   echo "clean stopped containers"
   docker rm -v $(docker ps -aq -f status=exited) 
fi

echo "count: " $count
echo "s3:" $s3
if [ $s3 == "s3" ]; then
  echo "using s3"
  echo "starting headnode"
  ./run.sh head $count
  echo "starting asyncnode"
  ./run.sh an
  echo "starting datanodes"
  ./run.sh dn $count
  echo "starting service nodes"
  ./run.sh sn $count
else
  echo "using minio"
  echo "starting headnode"
  ./run_minio.sh head $count
  echo "starting asyncnode"
  ./run_minio.sh an
  echo "starting datanodes"
  ./run_minio.sh dn $count
  echo "starting service nodes"
  ./run_minio.sh sn $count
fi

docker ps
   
 


 

 
