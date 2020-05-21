#!/bin/bash

admin_passwd=$(echo admin |md5sum | awk '{print $1}')

#echo 'admin':$admin_passwd >> /home/ubuntu/hsds/admin/config/passwd.txt

aws dynamodb put-item --table-name $2 --item '{"username": {"S": "admin"}, "password": {"S": "'$admin_passwd'"}}' --region $3

# aws ssm put-parameter --name "admin" --value admin_passwd --type String --region $3

for username in $1

do

passwd=$(echo $username |md5sum | awk '{print $1}')

# Store into textfile. NOTE: to be deprecated
#echo $username:$passwd >> /home/ubuntu/hsds/admin/config/passwd.txt

# Store credentials in dynamoDB
aws dynamodb put-item --table-name $2 --item '{"username": {"S": "'$username'"}, "password": {"S": "'$passwd'"}}' --region $3

# Store credentials in SSM parameter store
# aws ssm put-parameter --name $username --value $username:$passwd --type String --region $3

done
