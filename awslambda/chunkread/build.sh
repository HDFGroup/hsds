#
SRCDIR=../../hsds
zip function.zip *.py
cd $SRCDIR
zip -g ${OLDPWD}/function.zip util/arrayUtil.py
zip -g ${OLDPWD}/function.zip util/arrayUtil.py
zip -g ${OLDPWD}/function.zip util/chunkUtil.py
zip -g ${OLDPWD}/function.zip util/dsetUtil.py
zip -g ${OLDPWD}/function.zip util/domainUtil.py
zip -g ${OLDPWD}/function.zip util/hdf5dtype.py
zip -g ${OLDPWD}/function.zip util/idUtil.py
zip -g ${OLDPWD}/function.zip util/s3Client.py
zip -g ${OLDPWD}/function.zip util/storUtil.py


pip install --target ./package numpy
pip install --target ./package aiobotocore
pip install --target ./package aiohttp
pip install --target ./package numba

cd package
zip -r9 ${OLDPWD}/function.zip .

cd -

aws lambda update-function-code --function-name chunk_read --zip-file fileb://function.zip
