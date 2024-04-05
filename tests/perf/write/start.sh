export HS_ENDPOINT=http://host.docker.internal:5101
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

if [[ $# -gt 0 ]] ; then
  if [[ $1 == "--help" ]] ; then
    echo "./run.sh [count]" 
    exit 0
  fi
  count=$1
else
  count=1
fi

echo "count $count"

docker compose -f docker-compose.yml up -d --scale hswritetest=$count
