#!/bin/bash
#
# create zip file for deployment to AWS Lambda
#

ZIPFILE="function.zip"
if [ -f ${ZIPFILE} ]; then
   rm ${ZIPFILE}
fi
zip ${ZIPFILE} lambda_function.py
zip ${ZIPFILE} __init__.py
zip ${ZIPFILE} hsds/*.py
zip ${ZIPFILE} hsds/util/*.py

pip install --target ./package numpy
pip install --target ./package aiobotocore
pip install --target ./package aiohttp
#pip install --target ./package numba

cd package
zip -r9 ${OLDPWD}/function.zip .

cd -