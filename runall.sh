#!/bin/bash

# script to startup hsds docker containers
if [ $# -eq 1 ] && ([ $1 == "-h" ] || [ $1 == "--help" ]); then
   echo "Usage: runall.sh [count]"
   exit 1
fi


count=4
if [ $# -eq 1 ]; then 
  count=$1
fi

if [ $(docker ps -aq -f status=exited | wc -l) -gt 0 ]; then
   echo "clean stopped containers"
   docker rm -v $(docker ps -aq -f status=exited) 
fi

echo "starting headnode"
./run.sh head $count

echo "starting datanodes"
./run.sh dn $count

echo "starting service nodes"
./run.sh sn $count

docker ps
   
 


 

 
