if [[ $# -eq 0 ]] || ([[ $1 == "-h" ]] || [[ $1 == "--help" ]]); then
   echo "Usage: scale_hsds.sh <pod_count>"
   exit 1
fi

kubectl -n hsperf scale deployment/hsperf-test --replicas=$1
