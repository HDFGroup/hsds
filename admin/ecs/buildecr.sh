#!/bin/bash

#
# A simple example building the hsds image and pushing it to the aws container service 
# Note, this is as of Mar,24 2017
#
# You will need to login from the build host, e.g. 
# aws ecr get-login --region us-west-2
#
# which will print something like:
# docker login -u AWS -p <big-b64-string> -e none https://158023651469.dkr.ecr.us-west-2.amazonaws.com
# 

HSDS_ADMIN=adminuser
HSDS_PASS=1foobar2

# The name of your repository 
# e.g. REPNAME=hsds
REPNAME=hsds

# The repo base uri 
# e.g. REPURI=158023651469.dkr.ecr.us-west-2.amazonaws.com
REPURI=

# Your tag. For now leave the tag as latest unless you know what you're doing.
# e.g. TAG=latest
TAG=latest

# The name of your target S3 bucket should be in your environment, if it is 
# not, set BUCKET_NAME to the appropriate value e.g. export BUCKET_NAME=nexdsp
export BUCKET_NAME=nexdsp

# create the credential file that hsds will use. 
if [ ! -d ./admin/config/ ]; then
   echo "The ./admin/config/ directory does not exists. Are you running the script from the correct location?"
   exit 1
fi
echo "$HSDS_ADMIN:$HSDS_PASS" > ./admin/config/passwd.txt

# Build the hsds container...
docker build -t $REPNAME .

# Add the appropriate tag
docker tag $REPNAME:$TAG $REPURI/$REPNAME:$TAG 

# And the final push
docker push $REPURI/$REPNAME:$TAG

