# Create Kubernetes secrets for AWS authentication based on environment variables
if [[ -z ${HS_USERNAME} ]]; then
   echo "HS_USERNAME not set"
   exit 1
fi
if [[ -z ${HS_PASSWORD} ]]; then
   echo "HS_PASSWORD not set"
   exit 1
fi
echo -n ${HS_USERNAME} > /tmp/hs_username
echo -n ${HS_PASSWORD} > /tmp/hs_password

if [[ -z ${K8S_NAMESPACE} ]]; then
   echo "K8S_NAMESPACE not set using default namespace"
   K8S_NAMESPACE="default"
fi

# create the secret
kubectl --namespace ${K8S_NAMESPACE} create secret generic h5user-cred   \
                                            --from-file=/tmp/hs_username \
                                            --from-file=/tmp/hs_password  


# delete the temp files
rm /tmp/hs_username
rm /tmp/hs_password