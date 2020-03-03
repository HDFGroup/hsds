#!/bin/bash
echo "hsds entrypoint"
echo "node type: " $NODE_TYPE
cd /usr/local/src/


if [ -z $NODE_TYPE ]; then
  # run client
  if [ -z "$RUN_COMMAND" ]; then
    /bin/bash
  else
    /bin/bash -c "$RUN_COMMAND"
  fi
elif [ $NODE_TYPE == "dn" ]; then
  echo "running hsds-datanode"
  python -u -m hsds.datanode
elif [ $NODE_TYPE == "sn" ]; then
  echo "running hsds-servicenode"
  python -u -m hsds.servicenode
elif [ $NODE_TYPE == "head_node" ]; then
  echo "running hsds-headnode"
  python -u -m hsds.headnode
else
  echo "Unknown NODE_TYPE: " $NODE_TYPE
fi
