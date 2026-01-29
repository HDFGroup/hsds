if [ "$#" -ne 1 ]; then
    echo "usage: ./update_image.sh <tag>"
fi

# get the ecr login password
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 891377354611.dkr.ecr.us-east-1.amazonaws.com

# tag to the path we need for ECR
docker tag psfc/hsperftest:latest 891377354611.dkr.ecr.us-east-1.amazonaws.com/hsperftest:${1}

# push to ECR 
docker push 891377354611.dkr.ecr.us-east-1.amazonaws.com/hsperftest:${1}
