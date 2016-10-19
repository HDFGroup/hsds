#!/bin/bash
run_pyflakes=1
if [ $# -gt 0 ]; then
    if [ $1 == "-h" ] || [ $1 == "--help" ]; then
        echo "Usage: build.sh [--nolint]"
        exit 1
    fi
    if [ $1 == "--nolint" ]; then
        echo "no pyflakes"
        run_pyflakes=
    fi
fi 
  
if [ $run_pyflakes ]; then
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
fi

echo "clean stopped containers"
docker rm -v $(docker ps -aq -f status=exited) 
echo "removing old hsds image"
docker rmi $(docker images -q hdfgroup/hsds:latest)


echo "building docker image"
docker build -t hdfgroup/hsds .
