#!/bin/bash
if [ $# -eq 0 ] || [ $1 == "--help" ]; then
   echo "Usage: run_client.sh <name> [cmd] [sn_host] [head_host] [count]"
   exit 1
fi 

CMD=/bin/bash
SHELL_FLAGS="--rm -it"

CONTAINER_NAME=$1

if [ $# -gt  1 ]; then
    CMD=$2
    echo "set cmd:" $CMD
    SHELL_FLAGS="-d"  # run in detached mode
fi

if [ $# -gt 2 ]; then
    SN_HOST=$3
fi

if [ $# -gt  3 ]; then
    HEAD_HOST=$4
fi

count=4
if [ $# -gt 4 ]; then 
  count=$5
fi

LINK_ARG=""
for ((i=0; i<${count}; i++)) do
    LINK_ARG=${LINK_ARG}"--link hsds_sn_${i}:hsds_sn_${i} "
done
 

if [ -z ${HEAD_HOST} ] || [ -z ${SN_HOST} ]; then
    echo "running cmd: " ${CMD} 
    #echo "shell flags: " ${SHELL_FLAGS}
    HEAD_HOST=${SN_HOST}
    HSDS_ENDPOINT='http://hsds_sn_0:5102'
    HEAD_ENDPOINT='http://hsds_head:5100'
    echo "hsds_endpoint: " $HSDS_ENDPOINT
    echo "head_endpoint: " $HEAD_ENDPOINT
    echo "run container"
    docker run  \
       -v ${PWD}:/hsds \
       --env HSDS_ENDPOINT=${HSDS_ENDPOINT} \
       --env HEAD_ENDPOINT=${HEAD_ENDPOINT} \
       --link hsds_head:hsds_head \
       --name ${CONTAINER_NAME} \
       ${LINK_ARG} \
       ${SHELL_FLAGS} \
       hdfgroup/hsds /bin/sh -c "$CMD"
else
    echo "running cmd: " ${CMD}
    HSDS_ENDPOINT='http://'${SN_HOST}':5102'
    HEAD_ENDPOINT='http://'${HEAD_HOST}':5100'
    echo "hsds_endpoint: " $HSDS_ENDPOINT
    echo "head_endpoint: " $HEAD_ENDPOINT
    echo "run container"
    docker run  \
       -v ${PWD}:/hsds \
       --env HSDS_ENDPOINT=${HSDS_ENDPOINT} \
       --env HEAD_ENDPOINT=${HEAD_ENDPOINT} \
       ${SHELL_FLAGS} \
       hdfgroup/hsds /bin/sh -c "$CMD"
fi
 



 
