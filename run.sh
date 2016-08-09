#!/bin/bash

 
if [ $# -eq 0 ] || [ $1 == "-h" ] || [ $1 == "--help" ]; then
   echo "Usage: run.sh [head|dn|sn|clean]"
   exit 1
fi
 
#
# Define common variables
#
NODE_TYPE="head_node"
HEAD_PORT=6000
DN_PORT=6001
SN_PORT=6002
 

#
# run container given in arguments
#
if [ $1 == "head" ]; then
  echo "run head_node - ${HEAD_PORT}"
  docker run -d -p ${HEAD_PORT}:${HEAD_PORT} --name hsds_head \
  --env HEAD_PORT=${HEAD_PORT} \
  --env NODE_TYPE="head_node"  \
  hdfgroup/hsds  
elif [ $1 == "dn" ]; then
  echo "run dn"
  docker run -d -p ${DN_PORT}:${DN_PORT} --name hsds_dn \
  --env DN_PORT=${DN_PORT} \
  --env NODE_TYPE="dn"  \
    hdfgroup/hsds
elif [ $1 == "sn" ]; then
  echo "run sn"
  docker run -d -p ${SN_PORT}:${SN_PORT} --name hsds_sn \
  --env SN_PORT=${SN_PORT} \
  --env NODE_TYPE="sn"  \
  hdfgroup/hsds    
elif [ $1 == "clean" ]; then
   echo "run_clean"
   docker rm -v $(docker ps -aq -f status=exited) 
fi
 


 

 
