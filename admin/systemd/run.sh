#!/bin/sh
#
# Setup environment variables for AMCE environment and start 
# requested servicenode
#
if [ $# -eq 0 ] || [ $1 == "-h" ] || [ $1 == "--help" ]; then
   echo "Usage: run.sh [head|dn|sn]"
   exit 1
fi
export AWS_S3_GATEWAY="https://s3-us-west-2.amazonaws.com"
export BUCKET_NAME="nasa.hsdsdev"
export AWS_REGION='us-west-2'
export PATH="${HOME}/anaconda3/bin:$PATH"
#source activate py35
PYTHON_VERSION=$(python --version)
echo $PYTHON_VERSION
export HEAD_PORT=5100
export DN_PORT=5101
export SN_PORT=5102
cd ${HOME}/hsds/hsds
echo "cwd:" ${PWD}
if [ $1 == "head" ]; then
    # magic url which returns local ip when run on EC2 host
    HEAD_HOST=$(curl http://169.254.169.254/latest/meta-data/local-ipv4)
    echo "running headnode.py"
    python -u headnode.py &
elif [ $1 == "dn" ]; then
    echo "running datanode.py"
    python -u datanode.py &
elif [ $1 == "sn" ]; then
    echo "running servicenode.py"
    python -u servicenode.py &
else
    echo "Argument not recognized"
    exit 1
fi
