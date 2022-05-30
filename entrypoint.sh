#!/bin/bash
echo "hsds entrypoint"
echo "node type: " $NODE_TYPE


if [ -z $NODE_TYPE ]; then
  # run client
  if [ -z "$RUN_COMMAND" ]; then
    /bin/bash
  else
    /bin/bash -c "$RUN_COMMAND"
  fi
elif [ $NODE_TYPE == "dn" ]; then
  echo "running hsds-datanode"
  export PYTHONUNBUFFERED="1"
  pipenv run hsds-datanode
elif [ $NODE_TYPE == "sn" ]; then
  echo "running hsds-servicenode"
  export PYTHONUNBUFFERED="1"
  pipenv run hsds-servicenode
elif [ $NODE_TYPE == "head_node" ]; then
  echo "running hsds-headnode"
  export PYTHONUNBUFFERED="1"
  pipenv run hsds-headnode
elif [ $NODE_TYPE == "rangeget" ]; then
  echo "running hsds-rangeget"
  export PYTHONUNBUFFERED="1"
  pipenv run hsds-rangeget
else
  echo "Unknown NODE_TYPE: " $NODE_TYPE
fi
