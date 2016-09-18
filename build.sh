#!/bin/bash

echo "running pyflakes on hsds"
if [ $(./pyflakes.sh -count hsds) -ge 1 ]; then
   echo "pyflakes errors in hsds..."
   ./pyflakes.sh hsds
   exit 1
fi
echo "running pyflakes on hsds/util"
if [ $(./pyflakes.sh -count hsds/util) -ge 1 ]; then
   echo "pyflakes errors in hsds/util..."
   ./pyflakes.sh hsds/util
   exit 1
fi

echo "clean stopped containers"
docker rm -v $(docker ps -aq -f status=exited) 
echo "removing old hsds image"
docker rmi $(docker images -q hdfgroup/hsds:latest)


echo "building docker image"
docker build -t hdfgroup/hsds .
