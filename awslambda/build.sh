#!/bin/bash
#
# create zip file for deployment to AWS Lambda
#

ZIPFILE="function.zip"
if [ -f ${ZIPFILE} ]; then
   rm ${ZIPFILE}
fi

run_pyflakes () {
   SRC=$1
   pyflakes="../pyflakes.sh"
   echo "running pyflakes on $SRC files"
   if [ $(${pyflakes} -count ${SRC}) -ge 1 ]; then
      echo "pyflakes errors in ${SRC}..."
      ${pyflakes} $SRC
      exit 1
   fi
}

dolint=1
if [ $# -gt 0 ]; then
    if [ $1 == "-h" ] || [ $1 == "--help" ]; then
        echo "Usage: build.sh [--nolint]"
        exit 1
    fi
    if [ $1 == "--nolint" ]; then
        echo "no pyflakes"
        dolint=0
    fi
fi

if [ $dolint ]; then
    echo "dolint"
    run_pyflakes "chunkread"
    run_pyflakes "chunkread/hsds"
    run_pyflakes "chunkread/hsds/util"
fi


zip ${ZIPFILE} chunkread/lambda_function.py
zip ${ZIPFILE} chunkread/__init__.py
zip ${ZIPFILE} chunkread/hsds/*.py
zip ${ZIPFILE} chunkread/hsds/util/*.py

pip install --target ./package numpy
#pip install --target ./package numba  # this will make the image too large...

cd package
zip -r9 ${OLDPWD}/function.zip .

cd -
