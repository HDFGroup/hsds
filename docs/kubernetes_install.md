Installation with Kubernetes
============================

Export environment variables as shown in "Sample .bashrc" below.

1. Setup Kubernetes cluster either manually or with AWS EKS
2. Install and configure kubectl on the machine being used for the installation
3. Create a bucket for HSDS, using AWS cli tools or AWS Management console
4. Get project source code: `$ git clone https://github.com/HDFGroup/hsds`
5. Apply k8s_rbac.yml: `$ kubectl apply -f k8s_rbac.yml`.  This allows pods running in Kubernetes to list other pods running in the cluster
6. Set the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to the values for the AWS account to be used
7. Run k8s_make_secrets.sh: `$  ./k8s_make_secrets.sh`. This stores the AWS access keys as a Kubernetes secret
8. For HSDS to be used only within the cluster apply: `$ kubectl apply -f k8s_service.yml`.  Or for HSDS to be available externally, customize k8s_service_lb.yml with an ssl cert identifier and apply: `$ kubectl apply -f k8s_service_lb.yml`
9. Go to admin/config directory: `$ cd hsds/admin/config`
10. Copy the file "passwd.default" to "passwd.txt".  Add any usernames/passwords you wish
11. From hsds directory, build docker image:  `$ docker build -t hdfgroup/hsds .`
12. Tag the docker image using the AWS ECR scheme: `$ docker tag 82126fcb0658 12345678.dkr.ecr.us-west-2.amazonaws.com/hsds:v1`  where 82126fcb0658 is the docker image id, 12345678, is the AWS account id, us-west-2 is replaced by the region you will be installing to, and v1 is the version (update this everytime you will be deploying a new version of HSDS)
13. Obtain the credentials to login to the AWS container registry: `$ aws ecr get-login --no-include-email`.  This will print a command starting with `docker login -u AWS ...`, run it
14. Push the image to AWS ECR: `$ docker push 1234567.dkr.ecr.us-west-2.amazonaws.com/hsds:v1`
15. In k8s_deployment.yml, customize the values for AWS_S3_GATEWAY, AWS_REGION, BUCKET_NAME, LOG_LEVEL, SERVER_NAME, HSDS_ENDPOINT, GREETING, and ABOUT based on the AWS region you will be deploying to and values desired for your installation
16. Apply the deployment: `$ kubectl apply -f k8s_deployment.yml`
17. Verify that the HSDS pod is running: `$ kubectl get pods`.  A pod with a name starting with hsds should be displayed with status as "Running".
18. Tail the pod logs (`$ kubectl logs -f hsds-1234 sn`) till you see the line: `All nodes healthy, changing cluster state to READY` (requires log level be set to INFO or lower)
19. Create a forwarding port to the Kubernetes service: `$ sudo kubectl port-forward hsds-1234 80:5101`
20. Add the DNS for the service to the /etc/hosts file.  E.g. `127.0.0.1  hsds.hdf.test` (use the DNS name given in k8s_deployment.yml)
21. Go to <http://hsds.hdf.test/about> and verify that "cluster_state" is "READY"
22. Install Anaconda: <https://conda.io/docs/user-guide/install/linux.html>  (install for python 3.6)
23. Install h5pyd: `$ pip install h5pyd`
24. Run: `$ hsconfigure`.  Set hs endpoint with DNS name (e.g. <http://hsds.hdf.test>) and admin username/password.  Ignore API Key.
25. Run: `$ hsinfo`.  Server state should be "`READY`".  Ignore the "Not Found" error for the admin home folder
26. Create "/home" folder: `$ hstouch /home/`.  Note: trailing slash is important!
27. For each username in the passwd file, create a top-level domain: `$ hstouch -o <username> /home/<username>/`
28. Run the integration test: `$ python testall.py --skip_unit`
29. The test suite will emit some warnings due to test domains not being loaded.  To address see test_data_setup below.
30. To scale up or down the number of HSDS pods, run: `$ kubectl scale --replicas=n deployment/hsds` where n is the number of pods desired.
31. If enabling external access to the service, create a DNS record for the HSDS endpoint to the DNS name of the AWS ELB load balancer


Test Data Setup
---------------

Using the following procedure to import test files into hsds

1. Install h5py: `$ pip install h5py`
2. Install h5pyd (Python client SDK): `$ pip install h5pyd`
3. Download the following file: `$ wget https://s3.amazonaws.com/hdfgroup/data/hdf5test/tall.h5`
4. In the following steps use the password that was setup for the test_user1 account in place of \<passwd\>
5. Create a test folder on HSDS: `$ hstouch -u test_user1 -p <passwd> /home/test_user1/test/` 
6. Import into hsds: `$ hsload -v -u test_user1 -p <passwd> tall.h5 /home/test_user1/test/`
7. Verify upload: `$ hsls -r -u test_user1 -p <passwd> /home/test_user1/test/tall.h5
