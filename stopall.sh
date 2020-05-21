#!/bin/bash

#
# Shutdown HSDS with "docker-compose down" using the appropriate compose file
#
if [[ ${AWS_S3_GATEWAY} ]]; then
  COMPOSE_FILE="admin/docker/docker-compose.aws.yml"
elif [[ ${AZURE_CONNECTION_STRING} ]]; then
  COMPOSE_FILE="admin/docker/docker-compose.azure.yml"
elif [[ ${ROOT_DIR} ]]; then
  COMPOSE_FILE="admin/docker/docker-compose.posix.yml"
else
  COMPOSE_FILE="admin/docker/docker-compose.openio.yml"
fi
 
echo "Running docker-compose -f ${COMPOSE_FILE} down"

docker-compose -f ${COMPOSE_FILE} down
