#!/bin/bash

# script to run SN/DN nodes as a OpenIO service

[ -z ${AWS_S3_GATEWAY}  ] && echo "Need to set AWS_S3_GATEWAY" && exit 1

[ -z ${HSDS_ENDPOINT} ] && echo "Need to set HSDS_ENDPOINT" && exit 1

[ -z ${OIO_PROXY} ] && echo "Need to se OIO_PROXY" && exit 1

[ ${NODE_TYPE} == "sn" ] && [ -z ${SN_PORT} ] && echo "Need to set SN_PORT" && exit 1

[ ${NODE_TYPE} == "dn" ] && [ -z ${DN_PORT} ] && echo "Need to set DN_PORT" && exit 1

[ -z ${HOST_IP} ] && echo "Need to set HOST_IP" && exit 1

PASSWORD_FILE=${PWD}/admin/config/passwd.txt

if [[ -z ${PUBLIC_DNS} ]] ; then
  if [[ ${HSDS_ENDPOINT} == "https://"* ]] ; then
     export PUBLIC_DNS=${HSDS_ENDPOINT:8}
  elif [[ ${HSDS_ENDPOINT} == "http://"* ]] ; then
     export PUBLIC_DNS=${HSDS_ENDPOINT:7}
  else
    echo "Invalid HSDS_ENDPOINT: ${HSDS_ENDPOINT}"  && exit 1
  fi
fi



echo "AWS_S3_GATEWAY:" $AWS_S3_GATEWAY
echo "AWS_ACCESS_KEY_ID:" $AWS_ACCESS_KEY_ID
echo "AWS_SECRET_ACCESS_KEY: ******"
echo "BUCKET_NAME:"  $BUCKET_NAME
echo "HSDS_ENDPOINT:" $HSDS_ENDPOINT
echo "PUBLIC_DNS:" $PUBLIC_DNS
echo "OIO_PROXY:" $OIO_PROXY

python hsds/servicenode.py
