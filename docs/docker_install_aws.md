Installation using Docker with AWS S3 storage
=============================================

Export environment variables as shown in "Sample .bashrc" below.

1. Install Python 3 (e.g. with Miniconda <https://docs.conda.io/en/latest/miniconda.html>)
4. Install Docker and docker-compose if necessary. See [Docker Setup](setup_docker.md) 
5. Create a bucket for HSDS, using aws cli tools or aws management console
6. Get project source code: `$ git clone https://github.com/HDFGroup/hsds`
6. Go to the hsds directory and run: `$ python setup.py install`
7. For a custom build (hsds source code has been changed), build docker image:  `$ docker build -t hdfgroup/hsds .`
8. Go to admin/config directory: `$ cd hsds/admin/config`
9. Copy the file "passwd.default" to "passwd.txt".  Add any usernames/passwords you wish.  Modify existing passwords (for admin, test_user1, test_user2) for security.
10. If group-level permissions are desired (See [Authorization](authorization.md)), copy the file "groups.default" to "groups.txt".  Modify existing groups as needed
11. Create environment variables as in "Sample .bashrc" below
12. Setup Lambda if desired.  See [AWS Lambda Setup](aws_lambda_setup.md)
13. Create the file **admin/config/override.yml** for deployment specific settings (see "Sample override.yml")
14. Start the service `$./runall.sh <n>` where n is the number of containers desired (defaults to 1)
15. Run `$ docker ps` and verify that the containers are running: hsds_head, hsds_sn_[1-n], hsds_dn_[1-n]
16. Go to <http://hsds.hdf.test/about> and verify that "cluster_state" is "READY" 
17. Perform post install configuration.   See: [Post Install Configuration](post_install.md)


Sample .bashrc
--------------

These environment variables will be passed to the Docker containers on start up.

    export AWS_ACCESS_KEY_ID=1234567890            # user your AWS account access key if using S3 (Not needed if running on EC2 and AWS_IAM_ROLE is defined)
    export AWS_SECRET_ACCESS_KEY=ABCDEFGHIJKL      # use your AWS account access secret key if using S3  (Not needed if running on EC2 and AWS_IAM_ROLE is defined)
    export BUCKET_NAME=hsds.test                   # set to the name of the bucket you will be using
    export AWS_REGION=us-east-1                    # The AWS region the instance/bucket is running in
    export AWS_S3_GATEWAY=http://s3.amazonaws.com  # Use AWS endpoint for region where bucket is
    export HSDS_ENDPOINT=http://hsds.hdf.test      # The DNS name of the instance (use https protocol if SSL is desired)
    export LOG_LEVEL=INFO                          # Verbosity of server logs (DEBUG, INFO, WARN, or ERROR)
    # For S3, set AWS_S3_GATEWAY to endpoint for the region the bucket is in.  E.g.: http://s3.amazonaws.com.
    # See http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region for list of endpoints.

Sample override.yml
-------------------

Review the contents of **admin/config/config.yml** and create the file **admin/config/override.yml** for any keys where you don't 
wish to use the default value.  Values that you will most certainly want to override are:

* aws_iam_)role # set to the name of an iam_role that allows read/write to the S3 bucket


Installing Software Updates
---------------------------

To get the latest codes changes from the HSDS repo do the following:

1. Shutdown the service: `$ stopall.sh`
2. Get code changes: `$ git pull`
3. Build a new Docker image: `$ docker-compose build`
4. Start the service: `$ ./runall.sh`
