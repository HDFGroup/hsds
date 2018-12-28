#!/bin/bash

if [ -z ${HS_ENDPOINT} ] ; then
  HS_ENDPOINT="http://hsdshdflab.hdfgroup.org"
fi

[ -z ${HS_USERNAME}  ] && echo "Need to set HS_USERNAME" && exit 1
[ -z ${HS_PASSWORD}  ] && echo "Need to set HS_PASSWORD" && exit 1
[ -z ${DOMAIN}] ] && echo "Need to set DOMAIN" && exit 1

docker run \
   -e "HS_ENDPOINT=$HS_ENDPOINT" \
   -e "HS_USERNAME=$HS_USERNAME" \
   -e "HS_PASSWORD=$HS_PASSWORD" \
   -e "DOMAIN=$DOMAIN" \
   -e "RUN_COUNT=$RUN_COUNT" \
   -it hsclient/load-kubeptwrite
