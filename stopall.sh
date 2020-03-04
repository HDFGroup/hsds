#!/bin/bash

#
# Shutdown HSDS with "docker-compose down" using the appropriate compose file
#
if [[ -z ${AWS_S3_GATEWAY}  ]] && [[ -z ${AZURE_CONNECTION_STRING}  ]]; then
  if [[ -z ${ROOT_DIR} ]]; then
    COMPOSE_FILE="docker-compose.openio.yml"
    [ -z ${HSDS_ENDPOINT} ] && export HSDS_ENDPOINT=http://localhost:5101
   else
     COMPOSE_FILE="docker-compose.posix.yml"
   fi
elif [[ ${HSDS_USE_HTTPS} ]]; then
   COMPOSE_FILE="docker-compose.secure.yml"
else
   COMPOSE_FILE="docker-compose.yml"
fi

echo "Running docker-compose -f ${COMPOSE_FILE} down"

docker-compose -f ${COMPOSE_FILE} down
