#!/bin/bash

#
# Shutdown HSDS with "docker-compose down" using the appropriate compose file
#
if [[ -z ${AWS_S3_GATEWAY}  ]]
then
  COMPOSE_FILE="docker-compose.openio.yml"
elif [[ ${HSDS_USE_HTTPS} ]]
then
   COMPOSE_FILE="docker-compose.secure.yml"
else
   COMPOSE_FILE="docker-compose.yml"
fi

docker-compose -f ${COMPOSE_FILE} down
