# Deploy lambda function
aws lambda update-function-code --region ${AWS_REGION} --function-name chunk_read --zip-file fileb://function.zip
