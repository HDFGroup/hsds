#!/bin/bash

 
if [ $# -eq 0 ] || [ $1 == "-h" ] || [ $1 == "--help" ]; then
   echo "Usage: run.sh [head|dn|sn|stopdn|stopsn|clean] [count]"
   exit 1
fi

count=1
if [ $# -eq 2 ]; then 
  count=$2
fi
  
 
#
# Define common variables
#
NODE_TYPE="head_node"
HEAD_PORT=5100
AN_PORT=6100
SN_PORT=5101
DN_PORT=6101
AN_RAM=1g  # AN needs to store the entire object dictionary in memory
SN_RAM=1g
DN_RAM=3g  # should be comfortably larger than CHUNK CACHE
HEAD_RAM=512m
# set chunk cache size to 2GB
CHUNK_MEM_CACHE_SIZE=2g
# set max chunk size to 8MB
MAX_CHUNK_SIZE=8m
# set the log level  
LOG_LEVEL=INFO
# Restart policy: no, on-failure, always, unless-stopped (see docker run reference)
RESTART_POLICY=on-failure

# the following is returned when /about is invoked
SERVER_NAME=${SERVER_NAME:='Highly Scalable Data Service (HSDS)'}

# Set ANONYMOUS_TTL to 0 to disable GC, default to 10 minutes
#ANONYMOUS_TTL=${ANONYMOUS_TTL:=600}
ANONYMOUS_TTL=${ANONYMOUS_TTL:=0}


#
# run container given in arguments
#
if [ $1 == "head" ]; then
  echo "run head_node - ${HEAD_PORT}"
  docker run -d -p ${HEAD_PORT}:${HEAD_PORT} --restart ${RESTART_POLICY} --name hsds_head \
  --memory=${HEAD_RAM} \
  --env TARGET_SN_COUNT=${count} \
  --env TARGET_DN_COUNT=${count} \
  --env HEAD_PORT=${HEAD_PORT} \
  --env HEAD_HOST="hsds_head" \
  --env NODE_TYPE="head_node"  \
  --env AWS_S3_GATEWAY=${AWS_S3_GATEWAY} \
  --env AWS_REGION=${AWS_REGION} \
  --env AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
  --env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
  --env BUCKET_NAME=${BUCKET_NAME} \
  --env SYS_BUCKET_NAME=${SYS_BUCKET_NAME} \
  --env LOG_LEVEL=${LOG_LEVEL} \
  hdfgroup/hsds  
elif [ $1 == "an" ]; then
  echo "run async_node - ${AN_PORT}"
  docker run -d -p ${AN_PORT}:${AN_PORT} --restart ${RESTART_POLICY} --name hsds_async \
  --memory=${AN_RAM} \
  --env AN_PORT=${AN_PORT} \
  --env NODE_TYPE="an"  \
  --env AWS_S3_GATEWAY=${AWS_S3_GATEWAY} \
  --env AWS_REGION=${AWS_REGION} \
  --env AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
  --env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
  --env BUCKET_NAME=${BUCKET_NAME} \
  --env LOG_LEVEL=${LOG_LEVEL} \
  --env ANONYMOUS_TTL=${ANONYMOUS_TTL} \
  --link hsds_head:hsds_head \
  hdfgroup/hsds
elif [ $1 == "dn" ]; then
  echo "run dn"
  
  for i in $(seq 1 $count);
    do    
      NAME="hsds_dn_"$(($i))
      docker run -d -p ${DN_PORT}:${DN_PORT} --restart ${RESTART_POLICY} --name $NAME \
        --memory=${DN_RAM} \
        --env DN_PORT=${DN_PORT} \
        --env NODE_TYPE="dn"  \
        --env AWS_S3_GATEWAY=${AWS_S3_GATEWAY} \
        --env AWS_REGION=${AWS_REGION} \
        --env AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
        --env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
        --env BUCKET_NAME=${BUCKET_NAME} \
        --env LOG_LEVEL=${LOG_LEVEL} \
        --env CHUNK_MEM_CACHE_SIZE=${CHUNK_MEM_CACHE_SIZE} \
        --env MAX_CHUNK_SIZE=${MAX_CHUNK_SIZE} \
        --link hsds_head:hsds_head \
        hdfgroup/hsds
      DN_PORT=$(($DN_PORT+1))
    done
elif [ $1 == "sn" ]; then
  echo "run sn"
  for i in $(seq 1 $count);
    do    
      NAME="hsds_sn_"$(($i))
      docker run -d -p ${SN_PORT}:${SN_PORT} --restart ${RESTART_POLICY} --name $NAME \
        --memory=${SN_RAM} \
        --env SN_PORT=${SN_PORT} \
        --env NODE_TYPE="sn"  \
        --env AWS_S3_GATEWAY=${AWS_S3_GATEWAY} \
        --env AWS_REGION=${AWS_REGION} \
        --env AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
        --env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
        --env BUCKET_NAME=${BUCKET_NAME} \
        --env LOG_LEVEL=${LOG_LEVEL} \
        --env SERVER_NAME="${SERVER_NAME}" \
        --env CHUNK_MEM_CACHE_SIZE=${CHUNK_MEM_CACHE_SIZE} \
        --env MAX_CHUNK_SIZE=${MAX_CHUNK_SIZE} \
        --link hsds_head:hsds_head \
        hdfgroup/hsds
      SN_PORT=$(($SN_PORT+1))
    done    
elif [ $1 == "stopdn" ]; then
   for i in $(seq 1 $count);
     do    
        DN_NAME="hsds_dn_"$(($i))   
        docker stop $DN_NAME &
     done
elif [ $1 == "stopsn" ]; then
   for i in $(seq 1 $count);
     do    
        SN_NAME="hsds_sn_"$(($i))
        docker stop $SN_NAME &
     done
elif [ $1 == "clean" ]; then
   echo "run_clean"
   docker rm -v $(docker ps -aq -f status=exited) 
else
  echo "Argument not recognized" 
fi
 


 

 
