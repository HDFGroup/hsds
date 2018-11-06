#!/bin/bash

#
# Shutdown HSDS with "docker-compose down" using the appropriate compose file
#

if [[ ${HSDS_ENDPOINT} == "https"* ]] ; then
   docker-compose -f docker-compose.secure.yml down
else
   docker-compose down  
fi
