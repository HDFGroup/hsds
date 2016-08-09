#!/bin/bash
cd /usr/local/src/hsds
if [ -z "$NODE_TYPE" ]; then
  NODE_TYPE="dn" 
fi 

if [ $NODE_TYPE == "dn" ]; then
  python datanode.py 
elif [ $NODE_TYPE == "sn" ]; then
  python servicenode.py 
else
  python headnode.py 
fi
