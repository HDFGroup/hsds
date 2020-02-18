# Create Kubernetes secrets for Azure authentication based on environment variables
[ -z ${AZURE_CONNECTION_STRING} ] &&  echo "Need to set AZURE_CONNECTION_STRING" && exit 1

echo -n ${AZURE_CONNECTION_STRING} > /tmp/az_conn_str

# create the secret
kubectl create secret generic azure-conn-str --from-file=/tmp/az_conn_str

# delete the temp files
rm /tmp/az_conn_str
