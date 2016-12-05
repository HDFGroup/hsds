#!/bin/bash
if [ $# -eq 1 ] && [ $1 == "--help" ]; then
   echo "Usage: run_client.sh [sn_host] [head_host]"
   exit 1
fi 

if [ $# -gt  0 ]; then
    SN_HOST=$1
fi

if [ $# -gt  1 ]; then
    HEAD_HOST=$2
fi

if [ -z ${HEAD_HOST} ] || [ -z ${SN_HOST} ]; then
     
    HEAD_HOST=${SN_HOST}
    HSDS_ENDPOINT='http://hsds_sn_0:5102'
    HEAD_ENDPOINT='http://hsds_head:5100'
    echo "hsds_endpoint: " $HSDS_ENDPOINT
    echo "head_endpoint: " $HEAD_ENDPOINT
    echo "run container"
    docker run --rm  --name hsds_client \
       --env HSDS_ENDPOINT=${HSDS_ENDPOINT} \
       --env HEAD_ENDPOINT=${HEAD_ENDPOINT} \
       --link hsds_head:hsds_head \
       --link hsds_sn_0:hsds_sn_0 \
       -it hdfgroup/hsds   
else
    HSDS_ENDPOINT='http://'${SN_HOST}':5102'
    HEAD_ENDPOINT='http://'${HEAD_HOST}':5100'
    echo "hsds_endpoint: " $HSDS_ENDPOINT
    echo "head_endpoint: " $HEAD_ENDPOINT
    echo "run container"
    docker run --rm  --name hsds_client \
       --env HSDS_ENDPOINT=${HSDS_ENDPOINT} \
       --env HEAD_ENDPOINT=${HEAD_ENDPOINT} \
       -it hdfgroup/hsds  /bin/bash
fi
 



 
