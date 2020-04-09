Installation with Kubernetes on AWS
===================================

**Note:** These instructions assume you are using a Linux based system (or OS X). If you are using Windows please see the special notes at the end.

To begin, export environment variables as shown in "Sample .bashrc" below.

These environment variables will be used to access AWS resources.

    export AWS_ACCESS_KEY_ID=1234567890            # user your AWS account access key if using S3 (Not needed if running on EC2 and AWS_IAM_ROLEis defined)
    export AWS_SECRET_ACCESS_KEY=ABCDEFGHIJKL      # use your AWS account access secret key if using S3  (Not needed if running on EC2 and AWS_IAM_ROLE is defined)
    export ADMIN_USER=admin                        # The username for the HSDS admin acount
    export BUCKET_NAME=hsds.test                   # set to the name of the bucket you will be using (must be globally unique)
    export AWS_REGION=us-east-1                    # for boto compatibility - for S3 set to the region the bucket is in
    export AWS_S3_GATEWAY=http://s3.amazonaws.com  # Use AWS endpoint for S3, see note below
    export HSDS_ENDPOINT=http://hsds.hdf.test      # use https protocol if SSL is desired

For S3, set AWS_S3_GATEWAY to endpoint for the region the bucket is in.  E.g.: <http://s3.amazonaws.com>. See <http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region> for list of endpoints.

Prerequisites
-------------

Setup Pip and Python 3 on your local machine if not already installed (e.g. with Miniconda <https://docs.conda.io/en/latest/miniconda.html>).

Clone the hsds repository in a local folder: `git clone https://github.com/HDFGroup/hsds`.

Setup your AWS  Kubernetes
---------------------------

Here we will create a Kubernetes cluster and S3 bucket

1. Setup Kubernetes cluster either manually or with AWS EKS
2. Install and configure kubectl on the machine being used for the installation
3. Run `kubectl cluster-info` to verify connection to the cluster
4. Create a bucket for HSDS, using AWS cli tools or AWS Management console (make sure it's in the same region as the cluster)
5. If you are using a VPC, veryify an endpoint for S3 is setup (see: <https://docs.aws.amazon.com/vpc/latest/userguide/vpc-endpoints-s3.html>).  This is important to avoid having to pay for egress charges between S3 and the Kubernetes cluster

Create Kubernetes secrets
-------------------------

Kubernetes secrets are used in AWS to make sensitive information available to the service.
HSDS on AWS utilizes the following secrets:

1. user-password: username/password list
2. aws-auth-keys: the AWS access key and secret key

HSDS accounts are set by creating the user-password secret (support for authentication using OpenID is pending).

To create the user-password secret, first create a text file with the desired usernames and passwords as follows:

1. Go to admin/config directory: `cd hsds/admin/config`
2. Copy the file "passwd.default" to "passwd.txt".
3. Add/change usernames/passwords that you want to use. **Note**: Do not keep the original example credentials.
4. Go back to the hsds root directory: `cd ../..`

Next, verify that you have set the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.

Run the make_secrets script: `./make_secrets.sh`

Run: `kubectl get secrets` to verify the secrets have been created.

Deploy HSDS to K8s
------------------

If you need to build and deploy a custom HSDS image (e.g. you have made changes to the HSDS code), first build and deploy the code to ECR as described in section "Building a docker image and deploying to ECR" below.  Otherwise, the standard image from docker hub (<https://hub.docker.com/repository/docker/hdfgroup/hsds>) will be deployed.

1. Create RBAC roles: `kubectl create -f k8s_rbac.yml`
2. Create HSDS service: `$ kubectl apply -f k8s_service_lb.yml`
3. This will create an external load balancer with an http endpoint with a public-ip.
   Use kubectl to get the public-ip of the hsds service: `$kubectl get service`
   You should see an entry similar to:

       NAME    TYPE           CLUSTER-IP     EXTERNAL-IP      PORT(S)        AGE
       hsds    LoadBalancer   10.0.242.109   20.36.17.252     80:30326/TCP   23

   Note the public-ip (EXTERNAL-IP). This is where you can access the HSDS service externally. It may take some time for the EXTERNAL-IP to show up after the service deployment.
4. Now we will deploy the HSDS containers. In ***k8s_deployment_aws.yml***, customize the values for:
   env sections:
    * HSDS_ENDPOINT (change to `http://public-ip` where pubic-ip is the EXTERNAL-IP from step 3 above)
    * BUCKET_NAME (this is the name of the blob container created earlier)
   containers sections
    * image: '1234567.dkr.ecr.us-east-1.amazonaws.com/hsds:v1' to reflect the ecr repository for deployment (for custom builds only).
5. Apply the deployment: `$ kubectl apply -f k8s_deployment_aws.yml`
6. Verify that the HSDS pod is running: `$ kubectl get pods`  a pod with a name starting with hsds should be displayed with status as "Running".
7. Additional verification: Run (`$ kubectl describe pod hsds-xxxx`) and make sure everything looks OK
8. To locally test that HSDS functioning
    * Create a forwarding port to the Kubernetes service `$ sudo kubectl port-forward hsds-1234 8080:5101` (use another port if 8080 is unavailable)
    * From a browser hit: <http://127.0.0.1:8080/about> and verify that "cluster_state" is "READY"

Test the Deployment using Integration Test and Test Data
--------------------------------------------------------

1. Install h5pyd: `pip install h5pyd`
2. Run: `hsconfigure` and set:
    * hs endpoint: e.g. <http://EXTERNAL-IP>)
    * admin username/password (added to passwd.txt earlier)
    * Ignore API Key
3. Run: `hsinfo`.  Server state should be "`READY`".  Ignore the "Not Found" error for the admin home folder
4. Create "/home" folder: `$ hstouch /home/`.  Note: trailing slash is important!
5. For each username in the passwd file (or desired AD usernames), create a top-level domain: `hstouch -u <username> -p <passwd> /home/<username>/test/`
6. Run the integration test: `python testall.py --skip_unit`
7. Download the following file: `wget https://s3.amazonaws.com/hdfgroup/data/hdf5test/tall.h5`
8. Create a test folder: `hstouch -u test_user1 -p <passwd> /home/test_user1/test/`
9. Import into hsds: `hsload -v -u test_user1 -p <passwd> tall.h5 /home/test_user1/test/`
10. Verify upload: `hsls -r -u test_user1 -p <passwd> /home/test_user1/test/tall.h5`

Cluster Scaling
---------------

To scale up or down the number of HSDS pods, run:
`$kubectl scale --replicas=n deployment/hsds` where n is the number of pods desired.

Building a docker image and deploying to ECR
--------------------------------------------

This step is only needed if a custom image of HSDS needs to be deployed.

1. From hsds directory, build docker image: `bash build.sh`
2. Tag the docker image using the ECR scheme: `docker tag 1234 56789.dkr.ecr.us-east-1.amazonaws.com/hsds:v1` where 1234 is the docker image id and 56780 is the account being deployed to, and v1 is the version (update this every time you will be deploying a new version of HSDS).
3. Login to the AWS container registry (ECR): `aws ecr get-login --no-include-email`, run the command that was printed
4. Push the image to ECR: `docker push 56789.dkr.ecr.us-east-1.amazonaws.com/hsds:v1`
5. Update the ***k8s_deployment_aws.yml*** file to use the ECR image path (note there are multiple references to the image)

Notes for Installation from a Windows Machine
---------------------------------------------

Follow the instructions above with the following modifications in the respective sections

1. Before you start make sure that you have docker installed on your system by running: `docker --version` otherwise install docker desktop: <https://docs.docker.com/docker-for-windows/>
2. Sample .bashrc will not work on Windows - instead run the bashrc commands on the console (or include them in a batch file and run the batch file)
3. For commands in all sections replace the unix environment variable notation (SVAR) with Windows notation (%VAR%).  For example instead of `$ACRNAME` use `%ACRNAME%`
