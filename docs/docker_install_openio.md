Installation with Docker with OpenIO (on prem deployment)
==========================================================

Export environment variables as shown in "Sample .bashrc" below.

1. Install Python 3 (e.g. with Miniconda <https://docs.conda.io/en/latest/miniconda.html>)
2. Install awscli Python packages (`$pip install awscli`)
3. Install aiohttp Python package (`$pip install aiohttp`)
4. Install Docker and docker-compose if necessary.   See [Docker Setup](setup_docker.md) 
5. If setting up an multi-node OpenIO cluster, follow instruction here: <https://docs.openio.io/latest/source/sandbox-guide/multi_nodes_install.html> and set AWS_S3_GATEWAY environment variable appropriately. For using OpenIO in a local container (for testing only), don't set the AWS_S3_GATEWAY environment variable.
6. If using a multi-node OpenIO cluster, create a bucket for HSDS, using aws cli tools: `$aws --endpoint-url $AWS_S3_GATEWAY --no-verify-ssl s3 mb s3://hsds.test`
7. Get project source code: `$ git clone https://github.com/HDFGroup/hsds`
8. Go to admin/config directory: `$ cd hsds/admin/config`
9. Copy the file "passwd.default" to "passwd.txt".  Add any usernames/passwords you wish.  Modify existing passwords (for admin, test_user1, test_user2) for security.
9. Create environment variables as in "Sample .bashrc" below
10. From hsds directory, build docker image:  `$ docker build -t hdfgroup/hsds .`
11. Start the service `$./runall.sh <n>` where n is the number of containers desired (defaults to 1)
12. Run `$ docker ps` and verify that the containers are running: hsds_head, hsds_sn_[1-n], hsds_dn_[1-n]
13. Go to <http://hsds.hdf.test/about> and verify that "cluster_state" is "READY" (might need to give it a minute or two)14. Perform post install configuration.   See: [Post Install Configuration](post_install.md)

Sample .bashrc
--------------

Use these environment variables for running with a multi node OpenIO cluster

    export AWS_ACCESS_KEY_ID=1234567890            # use the OpenIO AWS_ACCESS_KEY
    export AWS_SECRET_ACCESS_KEY=ABCDEFGHIJKL      # use the OpenIO AWS_SECRET_ACCESS_KEY
    export BUCKET_NAME=hsds.test                   # set to the name of the bucket you will be using
    export AWS_REGION=us-east-1                    # for boto compatibility - for S3 set to the region the bucket is in
    export AWS_S3_GATEWAY=http://host1.mynetwork.com:6007  # Set to the S3 port for one of the OpenIO machine in the cluster
    export HSDS_ENDPOINT=http://hsds.hdf.test    # The DNS name of the machine running docker, or a name defined in /etc/hosts.  Use https protocol if SSL is desired


Installing Software Updates
---------------------------

To get the latest codes changes from the HSDS repo do the following:

1. Shutdown the service: `$ stopall.sh`
2. Get code changes: `$ git pull`
3. Build a new Docker image: `$ docker-compose build`
4. Start the service: `$ ./runall.sh`
