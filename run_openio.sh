#!/bin/bash

# script to startup hsds SN/DN node with OpenIO
USAGE="Usage: run_openio.sh [sn|dn] [port]"

if [ $# -eq 0 ] || ([ $1 == "-h" ] || [ $1 == "--help" ]); then
   echo $USAGE
   exit 1
fi

unset AWS_ACCESS_KEY_ID
unset AWS_SECRET_ACCESS_KEY
unset AWS_S3_GATEWAY

# for lakesamm
export AWS_ACCESS_KEY_ID=demo:demo
export AWS_SECRET_ACCESS_KEY=DEMO_PASS
export BUCKET_NAME=hsds.lakesamm
export AWS_REGION=us-east-1
export AWS_S3_GATEWAY="[http://192.168.1.121:6007,  http://192.168.1.122:6007, http://192.168.1.123:6007]"
export HSDS_ENDPOINT=http://hsds.lakesamm.test
export LOG_LEVEL=DEBUG
export OIO_PROXY=http://192.168.1.121:6006
export OIO_NS=OPENIO
export HOST_IP=192.168.1.100
export PASSWORD_FILE=""

if [ $# -eq 2 ]; then
    export SN_PORT=$2
    export DN_PORT=$2
else
    export SN_PORT=8888
    export DN_PORT=8889
fi

if [ $1 == "sn" ]; then
    python hsds/servicenode.py
elif [ $1 == "dn" ]; then
    echo "run dn"
    python hsds/datanode.py
else
    echo $USAGE
fi