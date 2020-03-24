# Create Kubernetes secrets for Azure authentication based on environment variables
[ -z ${AZURE_CONNECTION_STRING} ] &&  echo "Need to set AZURE_CONNECTION_STRING" && exit 1

echo -n ${AZURE_CONNECTION_STRING} > /tmp/az_conn_str

# create the secret
kubectl create secret generic azure-conn-str --from-file=/tmp/az_conn_str

# delete the temp files
rm /tmp/az_conn_str

# create secrets for AAD if needed
if [ ${AZURE_APP_ID} ] && [ ${AZURE_RESOURCE_ID} ]; then
  echo -n ${AZURE_APP_ID} > /tmp/az_app_id
  echo -n ${AZURE_RESOURCE_ID} > /tmp/az_resource_id
  kubectl create secret generic azure-ad-ids --from-file=/tmp/az_app_id --from-file=/tmp/az_resource_id
  # delete tmp files
  rm /tmp/az_app_id
  rm /tmp/az_resource_id
fi
