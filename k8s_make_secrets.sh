# Create Kubernetes secrets for AWS authentication based on environment variables
[ -z ${AWS_ACCESS_KEY_ID} ] &&  echo "Need to set AWS_ACCESS_KEY_ID" && exit 1
[ -z ${AWS_SECRET_ACCESS_KEY} ] && echo "Need to set AWS_SECRET_ACCESS_KEY" && exit 1

echo -n ${AWS_ACCESS_KEY_ID} > /tmp/aws_access_key_id
echo -n ${AWS_SECRET_ACCESS_KEY} > /tmp/aws_secret_access_key

# create the secret
kubectl create secret generic aws-auth-keys --from-file=/tmp/aws_access_key_id --from-file=/tmp/aws_secret_access_key

# delete the temp files
rm /tmp/aws_access_key_id
rm /tmp/aws_secret_access_key