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
   echo "Usage: runall.sh [--no-docker] [--no-docker-tcp] [--stop] [--config] [dn_count] [sn_count]"
   echo "  --no-docker: run server as set of processes rather than Docker containers (using unix sockets)"
   echo "  --no-docker-tcp: run server as set of processes rather than Docker containers (using tcp)"
   echo "  --stop: shutdown the server (Docker only)"
   echo "  --config: view config options"
   echo "  count: set number of DN processes/containers (default is 4)"
   exit 1
fi


DOCKER_CMD="up"

while [[ $# -gt 0 ]]; do
  if [[ $1 == "--no-docker" ]]; then
    export NO_DOCKER=1
  elif [[ $1 == "--no-docker-tcp" ]]; then
    export NO_DOCKER=1
    export USE_TCP=1
  elif [[ $1 == "--stop" ]]; then
    echo "stopping"
    export DOCKER_CMD="down"
  elif [[ $1 == "--config" ]]; then
    PRINT_CONFIG=1
  elif  [[ -z ${DN_CORES} ]]; then
    export DN_CORES=$1
  else
    export SN_CORES=$1
  fi
  shift
done


if [[ -z $CONFIG_DIR ]]; then
  export CONFIG_DIR="admin/config"
fi

CONFIG_FILE="${CONFIG_DIR}/config.yml"
OVERRIDE_FILE="${CONFIG_DIR}/override.yml"

# get config values
if [[ ${PRINT_CONFIG} ]]; then
   echo "Config values.."
   echo "  Modify by setting corresponding environment variable or setting in admin/config/override.yml"
fi

config_value "LOG_LEVEL" && export LOG_LEVEL=$rv
config_value "AWS_S3_GATEWAY" && export AWS_S3_GATEWAY=$rv
config_value "AWS_IAM_ROLE" && export AWS_IAM_ROLE=$rv
config_value "AWS_ACCESS_KEY_ID" && export AWS_ACCESS_KEY_ID=$rv
config_value "AWS_SECRET_ACCESS_KEY" && export AWS_SECRET_ACCESS_KEY=$rv
config_value "AWS_REGION" && export AWS_REGION=$rv
config_value "AZURE_CONNECTION_STRING" && export AZURE_CONNECTION_STRING=$rv
config_value "SOCKET_DIR" && export SOCKET_DIR=$rv
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


if [[ -z ${DN_CORES} ]]; then
  export DN_CORES=4
fi

if [[ -z $SN_CORES ]]; then
  # Use 1 SN_CORE by default
  export SN_CORES=1
  export SN_PORT_RANGE=$SN_PORT
else
  export SN_PORT_RANGE=$SN_PORT-$((SN_PORT + SN_CORES - 1))
fi


if [[ ${NO_DOCKER} ]]; then
  # setup extra envs needed when not using docker
  export PYTHONUNBUFFERED="1"
  if [[ $USE_TCP ]] ; then
    echo "use tcp with no-docker option"
  else
    if [[ -z ${SOCKET_DIR} ]]; then
      # this is the directory that will be used for socket file and log files
      export SOCKET_DIR=/tmp/hs
    fi
    if [[ ! -d ${SOCKET_DIR} ]]; then
      echo "creating directory ${SOCKET_DIR}"
      mkdir ${SOCKET_DIR}
    fi
    echo "--no_docker option specified - using directory: ${SOCKET_DIR} for socket and log files"
  fi
  if [[ -f "admin/config/passwd.txt" ]]; then
     export PASSWORD_FILE="admin/config/passwd.txt"
  else
     export PASSWORD_FILE="admin/config/passwd.default"
  fi
  echo "using password file: ${PASSWORD_FILE}"
     
else
    # check that docker-compose is available
    docker-compose --version >/dev/null || exit 1
    if [[ -z ${COMPOSE_PROJECT_NAME} ]]; then
      export COMPOSE_PROJECT_NAME=hsds  # use "hsds_" as prefix for container names
    fi
fi

[[ -z ${BUCKET_NAME} ]] && echo "No default bucket set - did you mean to export BUCKET_NAME?"

[[ -z ${HSDS_ENDPOINT} ]] && echo "HSDS_ENDPOINT is not set" && exit 1

if [[ ${ROOT_DIR} ]]; then 
  COMPOSE_FILE="admin/docker/docker-compose.posix.yml"
  echo "ROOT_DIR set, using POSIX storage"
  if [[ ! -d ${ROOT_DIR} ]]; then
      echo "creating directory ${ROOT_DIR}"
      mkdir ${ROOT_DIR}
  fi
  if [[ ! -d ${ROOT_DIR}/${BUCKET_NAME} ]]; then
      echo "creating directory ${ROOT_DIR}/${BUCKET_NAME}"
      mkdir ${ROOT_DIR}/${BUCKET_NAME}
  fi
elif [[ ${AWS_S3_GATEWAY} ]]; then
  COMPOSE_FILE="admin/docker/docker-compose.aws.yml"
  echo "AWS_S3_GATEWAY set, using ${BUCKET_NAME} S3 Bucket (verify that this bucket exists)"
elif [[ ${AZURE_CONNECTION_STRING} ]]; then
  COMPOSE_FILE="admin/docker/docker-compose.azure.yml"
  echo "AZURE_CONNECTION_STRING set, using ${BUCKET_NAME} Azure Storage Container (verify that the container exsits)"
else
  echo "no storage setting defined (set at least one of ROOT_DIR, AWS_S3_GATEWAY or AZURE_CONNECTION_STRING)"
  exit 1
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
  if [[ $AWS_S3_GATEWAY ]] || [[ $AZURE_CONNECTION_STRING ]]; then
    if [[ $AWS_S3_GATEWAY ]]; then
      echo "Using S3 Gateway"
    else
      echo "Using Azure connection string"
    fi
    if [[ $USE_TCP ]]; then
      hsds --bucket_name ${BUCKET_NAME} --password_file ${PASSWORD_FILE} --logfile hs.log --loglevel ${LOG_LEVEL} --host localhost --port ${SN_PORT} --config_dir=${CONFIG_DIR} --count=${DN_CORES}
    else
      hsds --bucket_name ${BUCKET_NAME} --password_file ${PASSWORD_FILE} --logfile hs.log  --loglevel ${LOG_LEVEL} --socket_dir ${SOCKET_DIR} --config_dir=${CONFIG_DIR} --count=${DN_CORES}
    fi
  else
    echo "Using posix storage: ${ROOT_DIR}"
    if [[ $USE_TCP ]]; then
      hsds --root_dir ${ROOT_DIR} --password_file ${PASSWORD_FILE} --logfile hs.log  --loglevel ${LOG_LEVEL} --host localhost --port ${SN_PORT} --config_dir=${CONFIG_DIR} --count=${DN_CORES}
    else
      hsds --root_dir ${ROOT_DIR} --password_file ${PASSWORD_FILE} --logfile hs.log  --loglevel ${LOG_LEVEL} --socket_dir ${SOCKET_DIR}  --config_dir=${CONFIG_DIR} --count=${DN_CORES}
    fi
  fi
  # this will run until server is killed by ^C
else
  if [[ $DOCKER_CMD == "down" ]]; then
    # use the compose file to shutdown the sevice
    echo "Running docker-compose -f ${COMPOSE_FILE} down"
    docker-compose -f ${COMPOSE_FILE} down
    exit 0  # can quit now
  else
    echo "Running docker-compose -f ${COMPOSE_FILE} up -d --scale sn=${SN_CORES} --scale dn=${DN_CORES}"
    docker-compose -f ${COMPOSE_FILE} up -d --scale sn=${SN_CORES} --scale dn=${DN_CORES}
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
fi
