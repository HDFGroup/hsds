#!/bin/bash
echo "hswrite test  entrypoint"
export PYTHONUNBUFFERED="1"
echo "HS_ENDPOINT: ${HS_ENDPOINT}"
echo "HS_WRITE_TEST_DOMAIN: ${HS_WRITE_TEST_DOMAIN}"
python hs_write.py ${HS_WRITE_TEST_DOMAIN}
