#!/bin/bash
cd /usr/local/src/hsds

if [ -z $NODE_TYPE ]; then
  # run client
  cd /usr/local/src/
  /bin/bash
elif [ $NODE_TYPE == "dn" ]; then
  echo "running datanode.py"
  python -u datanode.py 
elif [ $NODE_TYPE == "sn" ]; then
  echo "running servicenode.py"
  python -u servicenode.py 
elif [ $NODE_TYPE == "head_node" ]; then
  echo "running headnode.py"
  python -u headnode.py 
else
  echo "Unknown NODE_TYPE: " $NODE_TYPE
fi
