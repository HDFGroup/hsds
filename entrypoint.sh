#!/bin/bash
cd /usr/local/src/hsds

if [ -z $NODE_TYPE ]; then
  # run client
  cd /usr/local/src/
  if [ -z "$RUN_COMMAND" ]; then
    /bin/bash
  else
    /bin/bash -c "$RUN_COMMAND"
  fi
elif [ $NODE_TYPE == "dn" ]; then
  echo "running datanode.py"
  python -u datanode.py 
elif [ $NODE_TYPE == "sn" ]; then
  echo "running servicenode.py"
  python -u servicenode.py 
elif [ $NODE_TYPE == "an" ]; then
  if [ -f /data/bucket.db ]; then
    echo "Using /data/bucket.db for sqlite"
  else
    echo "Missing sqlite db -- rebuilding"
    python -u rebuild_db.py
  fi
  echo "running asyncnode.py"
  python -u asyncnode.py
elif [ $NODE_TYPE == "head_node" ]; then
  echo "running headnode.py"
  python -u headnode.py 
else
  echo "Unknown NODE_TYPE: " $NODE_TYPE
fi
