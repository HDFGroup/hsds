# Create Kubernetes secrets for AWS authentication based on environment variables
if [ ${AWS_ACCESS_KEY_ID} ] && [ ${AWS_SECRET_ACCESS_KEY} ]; then
  echo -n ${AWS_ACCESS_KEY_ID} > /tmp/aws_access_key_id
  echo -n ${AWS_SECRET_ACCESS_KEY} > /tmp/aws_secret_access_key

  # create the secret
  kubectl create secret generic aws-auth-keys --from-file=/tmp/aws_access_key_id --from-file=/tmp/aws_secret_access_key

  # delete the temp files
  rm /tmp/aws_access_key_id
  rm /tmp/aws_secret_access_key
fi

# Create Kubernetes secrets for Azure authentication based on environment variables
if [ ${AZURE_CONNECTION_STRING} ]; then
  echo -n ${AZURE_CONNECTION_STRING} > /tmp/az_conn_str

  # create the secret
  kubectl create secret generic azure-conn-str --from-file=/tmp/az_conn_str

  # delete the temp files
  rm /tmp/az_conn_str
fi

# create secrets for AAD if needed
if [ ${AZURE_APP_ID} ] && [ ${AZURE_RESOURCE_ID} ]; then
  echo -n ${AZURE_APP_ID} > /tmp/az_app_id
  echo -n ${AZURE_RESOURCE_ID} > /tmp/az_resource_id
  kubectl create secret generic azure-ad-ids --from-file=/tmp/az_app_id --from-file=/tmp/az_resource_id
  # delete tmp files
  rm /tmp/az_app_id
  rm /tmp/az_resource_id
fi

# make password secret if password.txt is present
if [ -f "admin/config/passwd.txt" ]; then
  kubectl create secret generic user-password --from-file=admin/config/passwd.txt
fi
