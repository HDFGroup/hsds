#!/bin/bash

config_value() {
  # For given key return env variable, override.yml value, config.yml value in that order
  # Not a proper yaml parser, but works for simple config we have
  key=$1
  yaml_key=`echo $key | awk '{ print tolower($0) }'`

  # check for environment variable
  rv="${!key}"
  
  if [[ -z $rv ]] && [[ -f $OVERRIDE_FILE ]] ; then
    # check override yaml
    rv=`grep "^${yaml_key}" $OVERRIDE_FILE | awk '{ print $2 }'`
  fi
  if [[ -z $rv ]] ; then
    # check config yaml
    rv=`grep "^${yaml_key}" $CONFIG_FILE | awk '{ print $2 }'`
  fi
  if [ "$rv" = "null" ]; then
    rv=  # treat yml null as empty string
  fi
  if [[ ${PRINT_CONFIG} ]]; then
    echo "${key}=${rv}"
  fi
 
  [[ -z $rv ]] && return 1 || return 0
}

# script to startup hsds service
if [[ $# -eq 1 ]] && ([[ $1 == "-h" ]] || [[ $1 == "--help" ]]); then
   echo "Usage: runall.sh [--no-docker] [--stop] [--config] [count] "
   exit 1
fi

if [[ $# -gt 0 ]] ; then
  if [[ $1 == "--no-docker" ]] ; then
    [[ -z $LOG_DIR ]] && echo "need to set LOG_DIR environment variable"  && exit 1
    export NO_DOCKER=1

    if [[ $# -gt 1 ]] ; then
      export CORES=$2
    fi
  elif [[ $1 == "--stop" ]]; then
     echo "stopping"
  elif [[ $1 == "--config" ]]; then
     PRINT_CONFIG=1
  else
    export CORES=$1
  fi
fi

if [[ ${CORES} ]] ; then
  export DN_CORES=${CORES}
else
  export DN_CORES=1
fi

if [[ -z $SN_CORES ]] ; then
  # Use 1 SN_CORE unless there's an environment variable set
  export SN_CORES=1
fi

CONFIG_FILE="admin/config/config.yml"
OVERRIDE_FILE="admin/config/override.yml"

# get config values
config_value "LOG_LEVEL" && export LOG_LEVEL=$rv
config_value "AWS_S3_GATEWAY" && export AWS_S3_GATEWAY=$rv
config_value "AWS_IAM_ROLE" && export AWS_IAM_ROLE=$rv
config_value "AWS_ACCESS_KEY_ID" && export AWS_ACCESS_KEY_ID=$rv
config_value "AWS_SECRET_ACCESS_KEY" && export AWS_SECRET_ACCESS_KEY=$rv
config_value "AWS_REGION" && export AWS_REGION=$rv
config_value "AZURE_CONNECTION_STRING" && export AZURE_CONNECTION_STRING=$rv
config_value "ROOT_DIR" && export ROOT_DIR=$rv
config_value "BUCKET_NAME" && export BUCKET_NAME=$rv
config_value "HSDS_ENDPOINT" && export HSDS_ENDPOINT=$rv
config_value "RESTART_POLICY" && export RESTART_POLICY=$rv
config_value "PUBLIC_DNS" && export PUBLIC_DNS=$rv
config_value "HEAD_PORT" && export HEAD_PORT=$rv
config_value "HEAD_RAM" && export HEAD_RAM=$rv
config_value "DN_PORT" && export DN_PORT=$rv
config_value "DN_RAM" && export DN_RAM=$rv
config_value "SN_PORT" && export SN_PORT=$rv
config_value "SN_RAM" && export SN_RAM=$rv
config_value "RANGEGET_PORT" && export RANGEGET_PORT=$rv
config_value "RANGEGET_RAM" && export RANGEGET_RAM=$rv

if [[ ${NO_DOCKER} ]]; then
   # setup extra envs needed when not using docker
    export TARGET_SN_COUNT=$SN_CORES
    export TARGET_DN_COUNT=$DN_CORES
    export HEAD_ENDPOINT=http://localhost:5100
    export PYTHONUNBUFFERED="1"
    [[ -z ${SN_PORT} ]] && export SN_PORT=80
    [[ -z ${PASSWORD_FILE} ]] && export PASSWORD_FILE=${PWD}/admin/config/passwd.txt
    [[ -z ${CONFIG_DIR} ]] && export CONFIG_DIR=${PWD}/admin/config/
    # TBD - this script needs updating to run multiple SN, DN nodes
    export SN_CORES=1
    export DN_CORES=1
else
    # check that docker-compose is available
    docker-compose --version >/dev/null || exit 1
    export COMPOSE_PROJECT_NAME=hsds  # use "hsds_" as prefix for container names
fi


[[ -z ${BUCKET_NAME} ]] && echo "No default bucket set - did you mean to export BUCKET_NAME?"

[[ -z ${HSDS_ENDPOINT} ]] && echo "HSDS_ENDPOINT is not set" && exit 1

if [[ ${AWS_S3_GATEWAY} ]]; then
  COMPOSE_FILE="admin/docker/docker-compose.aws.yml"
  echo "AWS_S3_GATEWAY set, using ${COMPOSE_FILE}"
elif [[ ${AZURE_CONNECTION_STRING} ]]; then
  COMPOSE_FILE="admin/docker/docker-compose.azure.yml"
  echo "AZURE_CONNECTION_STRING set, using ${COMPOSE_FILE}"
else 
  COMPOSE_FILE="admin/docker/docker-compose.posix.yml"
  echo "no AWS or AZURE env set, using ${COMPOSE_FILE}"
  if [[ -z ${ROOT_DIR} ]]; then
    export ROOT_DIR=$PWD/data
    echo "no ROOT_DIR env set, using $ROOT_DIR directory for storage"
  fi
  if [[ ! -d ${ROOT_DIR} ]]; then
      echo "creating directory ${ROOT_DIR}"
      mkdir ${ROOT_DIR}
  fi
  if [[ ! -d ${ROOT_DIR}/${BUCKET_NAME} ]]; then
      echo "creating directory ${ROOT_DIR}/${BUCKET_NAME}"
      mkdir ${ROOT_DIR}/${BUCKET_NAME}
  fi
fi
 

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
  [[ -z ${AWS_ACCESS_KEY_ID} ]] && echo "Need to set AWS_ACCESS_KEY_ID or AWS_IAM_ROLE" && exit 1
  [[ -z ${AWS_SECRET_ACCESS_KEY} ]] && echo "Need to set AWS_SECRET_ACCESS_KEY" && exit 1
fi

if [[ ${PRINT_CONFIG} ]]; then
   echo "use $0 without --config option to actually start/stop service"
   exit 0
fi

if [[ $NO_DOCKER ]] ; then
  echo "no docker startup"
  echo "starting head node"
  hsds-headnode >${LOG_DIR}/head.log 2>&1 &
  sleep 1
  echo "starting service node"
  hsds-servicenode >${LOG_DIR}/sn.log 2>&1 &
  sleep 1
  echo "starting data node"
  hsds-datanode >${LOG_DIR}/dn.log 2>&1 &
else
  if [[ $# -eq 1 ]] && [[ $1 == "--stop" ]]; then
    # use the compose file to shutdown the sevice
    echo "Running docker-compose -f ${COMPOSE_FILE} down"
    docker-compose -f ${COMPOSE_FILE} down 
    exit 0  # can quit now
  else
    echo "Running docker-compose -f ${COMPOSE_FILE} up"
    docker-compose -f ${COMPOSE_FILE} up -d --scale sn=${SN_CORES} --scale dn=${DN_CORES}
  fi
fi

# wait for the server to be ready
for i in {1..120}
do
  STATUS_CODE=`curl -s -o /dev/null -w "%{http_code}" http://localhost:${SN_PORT}/about`
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
