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
HSDS_HEAD_PID=$!

echo "stoping asyncnode"
docker stop hsds_async &
HSDS_ASYNC_PID=$!

echo "stopping datanodes"
./run.sh stopdn $count &
HSDS_DN_PID=$!


echo "stopping service nodes"
./run.sh stopsn $count &
HSDS_SN_PID=$!

wait $HSDS_{HEAD,ASYNC,DN,SN}_PID 

for job in `jobs | awk '{print $1}' | sed -e "s/\[//" |sed -e "s/\]//" | sed -e "s/[+-]//"`; do wait %$job 2>/dev/null; done

docker ps
   
 


 

 
