if [[ -z $HS_ENDPOINT ]] ; then
  echo "HS_ENDPOINT not set" 
  exit 1
fi
if [[ -z $HS_USERNAME ]] ; then
  echo "HS_USERNAME not set" 
  exit 1
fi
if [[ -z $HS_PASSWORD ]] ; then
  echo "HS_PASWORD not set" 
  exit 1
fi
if [[ -z $HS_WRITE_TEST_DOMAIN ]] ; then
  echo "HS_WRITE_TEST_DOMAIN not set" 
  exit 1
fi
if [[ -z $HS_BUCKET ]]; then
  export HS_BUCKET=
fi

docker run  \
  -e HS_ENDPOINT=${HS_ENDPOINT} \
  -e HS_USERNAME=${HS_USERNAME} \
  -e HS_PASSWORD=${HS_PASSWORD} \
  -e HS_BUCKET=${HS_BUCKET} \
  -e HS_WRITE_TEST_DOMAIN=${HS_WRITE_TEST_DOMAIN} \
  -d hdfgroup/hswritetest
