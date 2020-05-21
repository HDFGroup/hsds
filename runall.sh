#!/bin/bash

# script to startup hsds service
if [[ $# -eq 1 ]] && ([[ $1 == "-h" ]] || [[ $1 == "--help" ]]); then
   echo "Usage: runall.sh [[count]]"
   exit 1
fi

if [[ ${AWS_S3_GATEWAY} ]]; then
  COMPOSE_FILE="admin/docker/docker-compose.aws.yml"
  echo "AWS_S3_GATEWAY set, using ${COMPOSE_FILE}"
elif [[ ${AZURE_CONNECTION_STRING} ]]; then
  COMPOSE_FILE="admin/docker/docker-compose.azure.yml"
  echo "AZURE_CONNECTION_STRING set, using ${COMPOSE_FILE}"
elif [[ ${ROOT_DIR} ]]; then
  COMPOSE_FILE="admin/docker/docker-compose.posix.yml"
  echo "ROOT_DIR set, using ${COMPOSE_FILE}"
else
  COMPOSE_FILE="admin/docker/docker-compose.openio.yml"
  echo "no persistent storage configured, using OPENIO ephemeral storage, ${COMPOSE_FILE}"
  export AWS_S3_GATEWAY="http://openio:6007"
  if [[ -z ${AWS_ACCESS_KEY_ID} ]]; then
      # use default access keys and region for openio demo container
      export AWS_ACCESS_KEY_ID=demo:demo
      export AWS_SECRET_ACCESS_KEY=DEMO_PASS
      export AWS_REGION=us-east-1
  fi
  [[ -z ${BUCKET_NAME} ]]  && export BUCKET_NAME="hsds.test"
  [[ -z ${HSDS_ENDPOINT} ]] && export HSDS_ENDPOINT=http://localhost
fi
 
[[ -z ${BUCKET_NAME} ]] && echo "No default bucket set - did you mean to export BUCKET_NAME?"

[[ -z ${HSDS_ENDPOINT} ]] && echo "HSDS_ENDPOINT is not set" && exit 1

if [[ -z ${PUBLIC_DNS} ]] ; then
  if [[ ${HSDS_ENDPOINT} == "https://"* ]] ; then
     export PUBLIC_DNS=${HSDS_ENDPOINT:8}
  elif [[ ${HSDS_ENDPOINT} == "http://"* ]] ; then
     export PUBLIC_DNS=${HSDS_ENDPOINT:7}
  else
    echo "Invalid HSDS_ENDPOINT: ${HSDS_ENDPOINT}"  && exit 1
  fi
fi

if [[ -z $AWS_IAM_ROLE ]] && [[ $AWS_S3_GATEWAY ]]; then
  # if not using s3 or S3 without EC2 IAM roles, need to define AWS access keys
  [[ -z ${AWS_ACCESS_KEY_ID} ]] && echo "Need to set AWS_ACCESS_KEY_ID" && exit 1
  [[ -z ${AWS_SECRET_ACCESS_KEY} ]] && echo "Need to set AWS_SECRET_ACCESS_KEY" && exit 1
fi

if [[ $# -gt 0 ]]; then
  export DN_CORES=$1
  export SN_CORES=$1
elif [[ ${CORES} ]] ; then
  export DN_CORES=${DN_CORES}
  export SN_CORES=${SN_CORES}
else
  export DN_CORES=1
  export SN_CORES=1
fi

echo "dn cores:" $DN_CORES

if [[ $AWS_S3_GATEWAY ]]; then
  echo "AWS_S3_GATEWAY:" $AWS_S3_GATEWAY
  echo "AWS_ACCESS_KEY_ID:" $AWS_ACCESS_KEY_ID
  echo "AWS_SECRET_ACCESS_KEY: ******"
elif [[ $AZURE_CONNECTION_STRING ]]; then
  echo "AZURE_CONNECTION_STRING: *****"
else
  echo "ROOT_DIR:" $ROOT_DIR
fi
echo "BUCKET_NAME:"  $BUCKET_NAME
echo "CORES: ${SN_CORES}/${DN_CORES}"
echo "HSDS_ENDPOINT:" $HSDS_ENDPOINT
echo "PUBLIC_DNS:" $PUBLIC_DNS

grep -q -c "^  proxy" ${COMPOSE_FILE}
if [[ $? -gt 0 ]]; then
  echo "no load balancer"
  export SN_CORES=1
  if [[ -z ${SN_PORT} ]]; then
    echo "setting sn_port to 80"
    export SN_PORT=80  # default to port 80 if the SN is fronting requests
  else
    echo "SN_PORT is set to: [${SN_PORT}]"
  fi
  docker-compose -f ${COMPOSE_FILE} up -d --scale dn=${DN_CORES}
else
  docker-compose -f ${COMPOSE_FILE} up -d --scale sn=${SN_CORES} --scale dn=${DN_CORES}
  echo "load balancer"
fi

if [[ ${AWS_S3_GATEWAY} == "http://openio:6007" ]]; then
  # install awscli if not setup already
  pip freeze | grep -q awscli  || pip install awscli
  # if we've just launched the openio demo container, create a test bucket
  echo "make bucket ${BUCKET_NAME} (may need some retries)"
  sleep 5  # let the openio container spin up first

  for ((var = 1; var <= 10; var++)); do
    # call may fail the first few times as the openio container is spinning up
    aws --endpoint-url http://127.0.0.1:6007 --no-verify-ssl s3 mb s3://${BUCKET_NAME} && break
    sleep 2
  done
  if aws --endpoint-url http://127.0.0.1:6007 --no-verify-ssl s3 ls s3://${BUCKET_NAME}
  then
     echo "bucket ${BUCKET_NAME} created"
  else
     echo "failed to create bucket ${BUCKET_NAME}"
     exit 1
  fi
fi

# wait for the server to be ready
for i in {1..120}
do
  STATUS_CODE=`curl -s -o /dev/null -w "%{http_code}" ${HSDS_ENDPOINT}/about`
  if [[ $STATUS_CODE == "200" ]]; then
    echo "service ready!"
    break
  else
    echo "${i}: waiting for server startup (status: ${STATUS_CODE}) "
    sleep 1
  fi
done

if [[ $STATUS_CODE != "200" ]]; then
  echo "service failed to start"
  echo "SN_1 logs:"
  docker logs --tail 100 hsds_sn_1
  exit 1
fi
