#!/bin/bash
echo "hswrite test  entrypoint"
export PYTHONUNBUFFERED="1"
echo "HS_ENDPOINT: ${HS_ENDPOINT}"
echo "HS_WRITE_TEST_DOMAIN: ${HS_WRITE_TEST_DOMAIN}"
python write_hdf.py ${HS_WRITE_TEST_DOMAIN}
