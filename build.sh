#!/bin/bash
run_pyflakes=1
run_docker=1
if [ $# -gt 0 ]; then
    if [ $1 == "-h" ] || [ $1 == "--help" ]; then
        echo "Usage: build.sh [--no-lint | --no-docker]"
        exit 1
    fi
    if [ $1 == "--no-lint" ]; then
        echo "no pyflakes"
        run_pyflakes=
    fi
    if [ $1 == "--no-docker" ]; then
        echo "no docker"
        run_docker=
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

pip install --upgrade build

echo "running build"
python -m build
pip install -v .
 
if [ $run_docker ]; then
    command -v docker
    echo "clean stopped containers"
    docker rm -v $(docker ps -aq -f status=exited)

    echo "building docker image"
    docker build -t hdfgroup/hsds .
fi
