#!/bin/bash
echo "hsperf entrypoint"
export PYTHONUNBUFFERED="1"
cd /app
python read_shots.py 0
echo "unexpected exit of read_shots.py" 
