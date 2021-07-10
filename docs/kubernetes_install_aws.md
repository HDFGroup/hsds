Installation with Kubernetes on AWS
===================================

Prerequisites
-------------

Clone the hsds repository in a local folder: `git clone https://github.com/HDFGroup/hsds`.  You will be modifying
the yaml files in hsds/admin/kubernetes and hsds/admin/config to customize the install for your deployment.

Setup your AWS EKS Cluster
--------------------------

Here we will create a Kubernetes cluster and S3 bucket

1. Setup Kubernetes cluster with AWS EKS
2. Install and configure kubectl on the machine being used for the installation
3. Run `kubectl cluster-info` to verify connection to the cluster
4. Create a bucket for HSDS, using AWS cli tools or AWS Management console (make sure it's in the same region as the cluster)
5. If you are using a VPC, verify an endpoint for S3 is setup (see: <https://docs.aws.amazon.com/vpc/latest/userguide/vpc-endpoints-s3.html>).  This is important to avoid having to pay for egress charges between S3 and the Kubernetes cluster

Create Kubernetes Secrets
-------------------------

Kubernetes secrets are used in AWS to make sensitive information available to the service.
HSDS on AWS utilizes the following secret: user-password: username/password list

HSDS accounts are set by creating the user-password secret (alternatively authentication using OpenID or Azure Active Directory can be used, but are not covered in this docuemnt).

To create the user-password secret, first create a text file with the desired usernames and passwords as follows:

1. Go to admin/config directory: `cd hsds/admin/config`
2. Copy the file "passwd.default" to "passwd.txt".
3. Add/change usernames/passwords that you want to use. **Note**: Do not keep the original example credentials.
4. Go back to the hsds root directory: `cd ../..`

Next, verify that you have set the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.

Run the make_secrets script: `admin/kubernetes/k8s_make_secrets.sh`

Run: `kubectl get secrets` to verify the secrets have been created.

Create Kubernetes ConfigMaps
----------------------------

Kubernetes ConfigMaps are used to store settings that are specific to your HSDS deployment.

Review the contents of **admin/config/config.yml** and create the file **admin/config/override.yml** for any keys where you don't 
wish to use the default value.  Values that you will most certainly want to override are:

* bucket_name # set to the name of the bucket you will be using (must be globally unique)
* aws_region  # set to the aws region you will be deploying to (e.g. us-west-2)
* hsds_endpoint # use the external IP endpoint or DNS name that maps to the IP
* aws_s3_gateway # Use AWS endpoint for the region you will be deploying to

Run the make_config map script to store the yaml settings as Kubernetes ConfigMaps: `admin/kubernetes/k8s_make_configmap.sh`

Run: `kubectl describe configmaps hsds-config` to verify the configmap entries.

Create EKS Node IAM Role
------------------------

Create an IAM role as described in [Amazon EKS node IAM role](https://docs.aws.amazon.com/eks/latest/userguide/create-node-role.html).

Add policies for AmazonEKSWorkerNodePolicy, AmazonEC2ContainerRegisteryReadOnly (if ACR will be used for images), 
AmazonEKS_CNI_Policy, Amazon_EBS_CSI_Driver, and a policy for S3 access.  The AmazonS3FullAcces policy can be used,
or (for greater security) a cosutom policy can be created that only allows access to the configured S3 Bucket.

Modify the deployment yaml to include the role ARN.

Apply the role to each Node Group in your cluster.

Deploy HSDS to K8s
------------------

If you need to build and deploy a custom HSDS image (e.g. you have made changes to the HSDS code), first build and deploy the code to ECR as described in section "Building a docker image and deploying to ECR" below.  Otherwise, the standard image from docker hub (<https://hub.docker.com/repository/docker/hdfgroup/hsds>) will be deployed.

1. Create RBAC roles: `kubectl create -f admin/kubernetes/k8s_rbac.yml`
2. Create HSDS service: `kubectl apply -f admin/kubernetes/k8s_service_lb.yml`
3. This will create an external load balancer with an http endpoint with a public-ip.
   Use kubectl to get the public-ip of the hsds service: `kubectl get service`
   You should see an entry similar to:

       NAME    TYPE           CLUSTER-IP     EXTERNAL-IP      PORT(S)        AGE
       hsds    LoadBalancer   10.0.242.109   20.36.17.252     80:30326/TCP   23

   Note the public-ip (EXTERNAL-IP). This is where you can access the HSDS service externally. It may take some time for the EXTERNAL-IP to show up after the service deployment.
4. Now we will deploy the HSDS containers. In ***k8s_deployment_aws.yml***, modify the image value if a custom build is being used.  E.g:
    * image: '1234567.dkr.ecr.us-east-1.amazonaws.com/hsds:v1' to reflect the ecr repository for deployment
5. Apply the deployment: `kubectl apply -f k8s_deployment_aws.yml`
6. Verify that the HSDS pod is running: `kubectl get pods`  a pod with a name starting with hsds should be displayed with status as "Running".
7. Additional verification: Run (`kubectl describe pod hsds-xxxx`) and make sure everything looks OK
8. To locally test that HSDS functioning
    * Create a forwarding port to the Kubernetes service `sudo kubectl port-forward hsds-1234 5101:5101` where 'hsds-1234' is the name of one of the HSDS pods. 
    * From a browser hit: <http://127.0.0.1:5101/about> and verify that "cluster_state" is "READY"

Test the Deployment using Integration Test and Test Data
--------------------------------------------------------

Perform post install configuration.   See: [Post Install Configuration](post_install.md)

Cluster Scaling
---------------

To scale up or down the number of HSDS pods, run:
`kubectl scale --replicas=n deployment/hsds` where n is the number of pods desired.

Building a docker image and deploying to ECR
--------------------------------------------

This step is only needed if a custom image of HSDS needs to be deployed.

1. From hsds directory, build docker image: `bash build.sh`
2. Using AWS CLI or the AWS Mangement console, crete an ECR repository, 'hsds' in the region you will be deploying to
3. Tag the docker image using the ECR scheme: `docker tag 1234 56789.dkr.ecr.us-east-1.amazonaws.com/hsds:v1` where 1234 is the docker image id and 56780 is the account being deployed to, and v1 is the version (update this every time you will be deploying a new version of HSDS).
4. Login to the AWS container registry (ECR): `aws ecr get-login --no-include-email`, run the command that was printed
5. Push the image to ECR: `docker push 56789.dkr.ecr.us-east-1.amazonaws.com/hsds:v1`
6. Update the ***k8s_deployment_aws.yml*** file to use the ECR image path (note there are multiple references to the image)
