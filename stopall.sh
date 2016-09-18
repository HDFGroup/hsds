#!/bin/bash

# script to stop hsds docker containers
if [ $# -eq 1 ] && ([ $1 == "-h" ] || [ $1 == "--help" ]); then
   echo "Usage: runall.sh [count]"
   exit 1
fi


count=4
if [ $# -eq 1 ]; then 
  count=$1
fi

echo "stopping headnode"
docker stop hsds_head &

echo "stopping datanodes"
./run.sh stopdn $count &

echo "stopping service nodes"
./run.sh stopsn $count &

sleep 10  # allow some time for containers to shutdown
docker ps
sleep 10
docker ps
   
 


 

 
