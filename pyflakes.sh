#!/bin/bash
#
# script to run pyflakes over set of files in directory
#
if [ $# -eq 1 ] && ([ $1 == "-h" ] || [ $1 == "--help" ]); then
   echo "Usage: pyflakes.sh [-count] [<dir>]"
   exit 1
fi

SRC_DIR=${PWD}
GETCOUNT=0
COUNT=0
if [ $# -ge 1 ] && [ $1 == "-count" ]; then
  GETCOUNT=1
fi

if [ $# -ge 1 ] && [ ${!#} != "-count" ]; then
   SRC_DIR=${!#}
fi

for f in ${SRC_DIR}/*.py
do
  if [ $GETCOUNT -ge 1 ]; then
     COUNT=$(($COUNT + $(pyflakes $f 2>&1 | wc -l)))
  else
     pyflakes $f
  fi
done

if [ $GETCOUNT -ge 1 ]; then
  echo $COUNT 
fi
