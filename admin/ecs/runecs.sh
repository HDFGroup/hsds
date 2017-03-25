#!/bin/bash

#
# A simple run hsds example on the aws container service 
# Note, this is as of Mar,24 2017
#
# You will need to login from the build host, e.g. 
# $ aws ecr get-login --region us-west-2
# which will print something like:
# docker login -u AWS -p <big-b64-string> -e none https://158023651469.dkr.ecr.us-west-2.amazonaws.com
#
# Note, you will need to use docker version >= 1.9 for this.
#

HEAD_PORT=5100
DN_PORT=5101
SN_PORT=5102
CONT_NETWORK=hsdsnet

CNT=
CLUST=
HOSTIP=

if [ -z "$1" ] || [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
   echo "runecs.sh <count> [cluster]"
   echo ""
   echo "This script expects the following environ variables to be set:"
   echo "AWS_S3_GATEWAY, AWS_REGION, AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID, BUCKET_NAME, REPURI, REPNAME, HSDS_ADMIN, ROOTDOMAIN"
   echo "== Example =="
   echo "export AWS_S3_GATEWAY=https://s3-us-west-2.amazonaws.com" 
   echo "export AWS_REGION=us-west-2"
   echo "export AWS_SECRET_ACCESS_KEY=1E1IVEleao4omF3l3mCQ77vT6nfqfWfFBp/CVM56"
   echo "export AWS_ACCESS_KEY_ID=BKIBIN4H4DG2B8RK23BB"
   echo "export BUCKET_NAME=abcdehsds"
   echo "export REPURI=158023651469.dkr.ecr.us-west-2.amazonaws.com"
   echo "export REPNAME=hsds"
   echo "export HSDS_ADMIN=adminuser"
   echo "export ROOTDOMAIN=/data"
   exit 1
else
   CNT=$1
fi

if [ ! -z "$2" ]; then
   CLUST="yes"
fi


#---------------------------------------------------------------------------
# Note, this depends on being able to reach the internet 
function mylocalip {
   # use ames internet root name server ns.nasa.gov for ip route test
   HOSTIP=$(ip route get 192.203.230.10 | awk '/192.203.230.10/ {print $NF}')
}

#---------------------------------------------------------------------------
# Makes a local container network so nodes can reach each other.
# Note, the --link is now deprecated...
function make_local_container_network {
   isavail=`docker network ls --filter name=$CONT_NETWORK | wc -l`
   if [ "$isavail" != "2" ]; then
      echo "Initializing docker network..." 
      docker network create --driver bridge $CONT_NETWORK
   fi
   echo "== Current docker networks =="
   docker network ls
   echo "== The $CONT_NETWORK docker network =="
   docker network inspect $CONT_NETWORK
   sleep 4
}

#---------------------------------------------------------------------------
# Routine must be called like "run_head N", where N is a number fomr 1 to M
function run_head {
   count=$1
   echo "starting the head on listening on port ${HEAD_PORT}"
   docker run -d -p ${HEAD_PORT}:${HEAD_PORT} --name hsds_head \
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
      --network=$CONT_NETWORK \
        ${REPURI}/${REPNAME}:latest
}

#-------------------------------------------------------------------------------
# Routine must be called like "run_datanode  N", where N is a number fomr 1 to M
function run_datanode {
   count=$1
   for i in $(seq 1 $count); do    
      NAME="hsds_dn_"$(($i-1))
      docker run -d -p ${DN_PORT}:${DN_PORT} --name $NAME \
         --env DN_PORT=${DN_PORT} \
         --env NODE_TYPE="dn"  \
         --env AWS_S3_GATEWAY=${AWS_S3_GATEWAY} \
         --env AWS_REGION=${AWS_REGION} \
         --env AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
         --env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
         --env BUCKET_NAME=${BUCKET_NAME} \
         --network=$CONT_NETWORK \
         ${REPURI}/${REPNAME}:latest
      DN_PORT=$(($DN_PORT+2))
   done
}

#-------------------------------------------------------------------------------
# Routine must be called like "run_servicenode  N", where N is a number fomr 1 to M
function run_servicenode  {
   count=$1
   for i in $(seq 1 $count); do    
      NAME="hsds_sn_"$(($i-1))
      docker run -d -p ${SN_PORT}:${SN_PORT} --name $NAME \
         --env SN_PORT=${SN_PORT} \
         --env NODE_TYPE="sn"  \
         --env AWS_S3_GATEWAY=${AWS_S3_GATEWAY} \
         --env AWS_REGION=${AWS_REGION} \
         --env AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
         --env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
         --env BUCKET_NAME=${BUCKET_NAME} \
         --network=$CONT_NETWORK \
         ${REPURI}/${REPNAME}:latest
      SN_PORT=$(($SN_PORT+2))
   done    
}

#---------------------------------------------------------------------------
# The hsds service requires a root domain to be specified before you
# can add/manipulate any other domain; here by domain we're refering to
# the hdf5 REST definition, not a network def'n.
function init_h5_root_domain_local {
   mylocalip 
   ison=
   for i in `seq 1 10`; do
      ISREADY=`curl -s http://$HOSTIP:$HEAD_PORT/info | grep READY`
      if  [ ! -z "$ISREADY" ]; then
         ison="1"
         break
      fi
      echo "waiting for local hsds to be ready..."
      sleep 2
   done

   if [ ! -z  "$ison" ]; then
      docker exec hsds_head \
      python3 /usr/local/src/hsds/create_toplevel_domain_json.py --user=$HSDS_ADMIN  --domain=$ROOT_DOMAIN
   else
      echo "local hsds failed to get to ready state..."
      exit 1
   fi
}

if [ "$CLUST" == "yes" ]; then
   echo "Running with ecs docker cluster..."
   echo "ECS cluster example not implemented yet..."
   exit 1
else
   echo "Running with local docker..."
   make_local_container_network 
   run_head $CNT
   run_datanode $CNT
   run_servicenode $CNT
   init_h5_root_domain_local 
fi


