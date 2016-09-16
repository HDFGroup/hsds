#!/bin/bash


echo "clean stopped containers"
docker rm -v $(docker ps -aq -f status=exited) 
echo "removing old hsds image"
docker rmi $(docker images -q hdfgroup/hsds:latest)
echo "building docker image"
docker build -t hdfgroup/hsds .
