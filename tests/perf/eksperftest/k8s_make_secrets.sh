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
echo -n "${HS_USERNAME}:${HS_PASSWORD}" > /tmp/user-password.txt

# create the secret
kubectl create secret generic hs-perftest-keys   \
                                            --from-file=/tmp/hs_username \
                                            --from-file=/tmp/hs_password \
                                            --from-file=/tmp/user-password.txt


# delete the temp files
rm /tmp/hs_username
rm /tmp/hs_password
rm /tmp/user-password.txt