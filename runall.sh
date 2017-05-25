#!/bin/bash

# script to startup hsds docker containers
if [ $# -eq 1 ] && ([ $1 == "-h" ] || [ $1 == "--help" ]); then
   echo "Usage: runall.sh [count] [s3]"
   exit 1
fi


count=4
if [ $# -gt 0 ]; then 
  count=$1
fi
s3=0
if [ $# -gt 1 ]; then 
  s3=$2
fi


if [ $(docker ps -aq -f status=exited | wc -l) -gt 0 ]; then
   echo "clean stopped containers"
   docker rm -v $(docker ps -aq -f status=exited) 
fi

echo "count: " $count
echo "s3:" $s3
if [ $s3 == "s3" ]; then
  echo "using s3"
  echo "starting headnode"
  ./run.sh head $count
  echo "starting asyncnode"
  ./run_minio.sh an
  echo "starting datanodes"
  ./run.sh dn $count
  echo "starting service nodes"
  ./run.sh sn $count
else
  echo "using minio"
  echo "starting headnode"
  ./run_minio.sh head $count
  echo "starting asyncnode"
  ./run_minio.sh an
  echo "starting datanodes"
  ./run_minio.sh dn $count
  echo "starting service nodes"
  ./run_minio.sh sn $count
fi

docker ps
   
 


 

 
