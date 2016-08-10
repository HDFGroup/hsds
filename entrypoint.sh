#!/bin/bash
cd /usr/local/src/hsds
if [ -z "$NODE_TYPE" ]; then
  NODE_TYPE="dn" 
fi 

if [ $NODE_TYPE == "dn" ]; then
  echo "running datanode.py"
  python -u datanode.py 
elif [ $NODE_TYPE == "sn" ]; then
  echo "running servicenode.py"
  python -u servicenode.py 
else
  echo "running headnode.py"
  python -u headnode.py 
fi
