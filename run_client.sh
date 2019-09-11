#!/bin/bash
if [ $# -eq 0 ] || [ $1 == "--help" ]; then
   echo "Usage: run_client.sh <name> [sn_host] [cmd] [count]"
   exit 1
fi

#SHELL_FLAGS="--rm -it"
SHELL_FLAGS="--rm -i --tty --image-pull-policy Never"
CONTAINER_NAME=$1
APP=kubectl
#APP=docker

SN_HOST=""
CMD=""

if [ $# -gt 1 ]; then
    SN_HOST=$2
fi

if [ $# -gt  2 ]; then
    CMD=$3
    SHELL_FLAGS="-d"  # run in detached mode
fi

count=4
if [ $# -gt 4 ]; then
  count=$5
fi

LINK_CMD=""
LINK_ARG=""
IMAGE=""

if [ -z ${HS_ENDPOINT} ]; then
  if [ -z ${SN_HOST} ]; then
    HS_ENDPOINT='http://hsds_sn_1:5101'  # only works with docker
  else
    HS_ENPOINT='http://${SN_HOST}:5101'
  fi
fi

if [ -z ${HS_USERNAME} ]; then
  HS_USERNAME=test_user1
fi
if [ -z ${HS_PASSWORD} ]; then
  HS_PASSWORD=test
fi
  

if [ ${APP} == "docker" ]; then
  for ((i=1; i<${count}+1; i++)) do
      LINK_ARG=${LINK_ARG}"--link hsds_sn_${i}:hsds_sn_${i} "
  done
  LINK_CMD="--link --name ${CONTAINER_NAME}"
  IMAGE="hdfgroup/hsds_client"
else
  IMAGE="--image hdfgroup/hsds_client"
fi


echo "link args: ${LINK_ARG} "
echo "shell flags: ${SHELL_FLAGS}"
echo "image: ${IMAGE}"
echo "hs_endpoint: ${HS_ENDPOINT}"
echo "hs_username: ${HS_USERNAME}"
echo "hs_password: ${HS_PASSWORD}"
   
${APP} run   \
    --env HS_ENDPOINT=${HS_ENDPOINT} \
    --env HS_USERNAME=${HS_USERNAME} \
    --env HS_PASSWORD=${HS_PASSWORD} \
    ${LINK_CMD} ${LINK_ARG} \
    ${SHELL_FLAGS} \
    ${IMAGE} ${CONTAINER_NAME}  