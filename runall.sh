#!/bin/bash

# script to startup hsds service
if [ $# -eq 1 ] && ([ $1 == "-h" ] || [ $1 == "--help" ]); then
   echo "Usage: runall.sh [count]"
   exit 1
fi

[ -z ${AWS_S3_GATEWAY}  ] && echo "Need to set AWS_S3_GATEWAY" && exit 1

[ -z ${BUCKET_NAME} ] && echo "Need to set BUCKET_NAME" && exit 1

[ -z ${SYS_BUCKET_NAME} ] && export SYS_BUCKET_NAME=${BUCKET_NAME}

[ -z ${HSDS_ENDPOINT} ] && echo "Need to set HSDS_ENDPOINT" && exit 1
if [[ ${HSDS_ENDPOINT} == "https://"* ]] ; then
   export PUBLIC_DNS=${HSDS_ENDPOINT:8}
elif [[ ${HSDS_ENDPOINT} == "http://"* ]] ; then
   export PUBLIC_DNS=${HSDS_ENDPOINT:7}
else
   echo "Invalid HSDS_ENDPOINT: ${HSDS_ENDPOINT}"  && exit 1 
fi 

if [ ${AWS_S3_GATEWAY} == "http://minio:9000" ] || [ -z $AWS_IAM_ROLE ] ; then
  # if not using s3 or S3 without EC2 IAM roles, need to define AWS access keys
  [ -z ${AWS_ACCESS_KEY_ID} ] && echo "Need to set AWS_ACCESS_KEY_ID" && exit 1
  [ -z ${AWS_SECRET_ACCESS_KEY} ] && echo "Need to set AWS_SECRET_ACCESS_KEY" && exit 1
fi

if [ $# -gt 0 ]; then 
  export CORES=$1
elif [ -z ${CORES} ] ; then
  export CORES=1
fi
 
echo "AWS_S3_GATEWAY:" $AWS_S3_GATEWAY
echo "AWS_ACCESS_KEY_ID:" $AWS_ACCESS_KEY_ID
echo "AWS_SECRET_ACCESS_KEY: ******" 
echo "BUCKET_NAME:"  $BUCKET_NAME
echo "SYS_BUCKET_NAME:" $SYS_BUCKET_NAME
echo "CORES:" $CORES
echo "HSDS_ENDPOINT:" $HSDS_ENDPOINT
echo "PUBLIC_DNS:" $PUBLIC_DNS

if [ ${AWS_S3_GATEWAY} == "http://minio:9000" ] ; then
   echo "docker-compose.local"
   docker-compose -f docker-compose.local.yml up -d --scale sn=${CORES} --scale dn=${CORES}
elif [[ ${HSDS_ENDPOINT} == "https"* ]] ; then
   echo "docker-compose.secure"
   docker-compose -f docker-compose.secure.yml up -d --scale sn=${CORES} --scale dn=${CORES}
else
   echo "docker-compose"
   docker-compose up -d --scale sn=${CORES} --scale dn=${CORES}
fi


   
 


 

 
