#!/bin/bash

#
# Shutdown HSDS with "docker-compose down" using the appropriate compose file
#
if [ ${AWS_S3_GATEWAY} == "http://minio:9000" ] ; then
   docker-compose -f docker-compose.local.yml down  
elif [[ ${HSDS_ENDPOINT} == "https"* ]] ; then
   docker-compose -f docker-compose.secure.yml down
else
   docker-compose down  
fi

 


 

 
