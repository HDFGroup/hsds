 

Installation with Docker
--------------------------

1. Install Docker if necessary (see "Docker Setup" below)
2. Export environment variables as shown in "Sample .bashrc" below.
3. If using Minio rather than S3, setup Minio (see "Minio Setup" below)
4. Create a bucket for HSDS, using aws cli tools, aws management console, or minio webapp
5. Get project source code: `$ git clone https://github.com/HDFGroup/hsds`
6. Go to admin/config directory: `$ cd hsds/admin/config`
7. Copy the file "passwd.default" to "passwd.txt".  Add any usernames/passwords you wish 
8. Create an enviroment variable to pass the bucket name: `$ export BUCKET_NAME=<your_bucket>`
9. Build docker image:  `$ ./build.sh --nolint` 
10. Run the docker containers: `$ ./runall.sh 4 s3`  For Minio, just: `$ ./runall.sh` 
11. Run `$ docker ps` and verify that 10 containers are running: hsds_head, hsds_async, hsds_sn[0-4], hsds_dn[0-4]
12. Get the external IP for the docker containers (just 127.0.0.1 if running directly on host, otherwise run: `$docker-machine ip`
13. Go to http://127.0.0.1:5100 (or http://<docker_ip>:5100/ for docker machine) and verify that "cluster_state" is "READY" (might need to give it a minute or two)
14. Exec into the head_node_container: `$ docker exec -it hsds_head /bin/bash`
15. In the head_node cd to the hsds source directory: `# cd /usr/local/src/hsds`
16. Create "home" folder: `# python create_toplevel_domain_json --user=admin --domain=/home`
17. For each other username in the passwd file, create a top-level domain:  `# python create_toplevel_domain_json.py --user=<username>`
18. Exit out of the container: `# exit`
19. Run the client container interactively: `$ ./run_client.sh hsds_test`
20. In the container, run the test suite: `# python testall.py`
21. The test suite will emit some warnings due to test domains not being loaded.  To address see test_data_setup below.
 
Sample .bashrc
--------------
These environment variables will be passed to the Docker containers on start up.

::

    export MINIO_DATA=${HOME}/minio_dat            # only needed for minio installs
    export MINIO_CONFIG=${HOME}/minio_confi        # only needed for minio installs
    export AWS_ACCESS_KEY_ID=1234567890            # user your AWS account access key if using S3
    export AWS_SECRET_ACCESS_KEY=ABCDEFGHIJKL      # use your AWS account access secret key if using S3
    export BUCKET_NAME=hsds                        # set to the name of the bucket you will be usings
    export AWS_REGION=us-east-1                    # for boto compatibility
    export AWS_S3_GATEWAY="http://127,0.0.1:9000"  # if running docker machine set to machine ip


Minio Setup
-----------

Minio is a docker container you can run on your desktop rather than using S3.  To HSDS treats the Minio just like
it would the real S3 endpoint.

To setup minio:

1. Create directories for minio data and config files based what the MINIO environemnt varaibles were set to - these can be on any local drive
2. Start Minio container.  From the hsds install directory: `$ run_minio.sh minio`
3. Run: `$ docker logs minio` and verify that minio started correctly.  It shold list the AWS keys specified in the enviornment variables.
4. Go to the minio web app: http://127.0.0.1:9000 or (http://<docker-machine ip>:9000 for docker machine) and sign in using the AWS access keys


Docker Setup
------------

The following are instructions for installing docker on Linux/CentOS.  Details for other Linux distros
may vary.  For OS X, see: https://docs.docker.com/engine/installation/. 

Run the following commands to install Docker on Linux/CentOS:

::
    $ sudo yum install docker
    $ sudo service docker start
    $ sudo chkconfig --level 300 docker on
    $ sudo usermod -aG docker $(whoami)
    $ docker ps  # verify


Test Data Setup
---------------

Using the following procedure to import test files into hsds

1. Get project source code: `$ git clone https://github.com/HDFGroup/h5pyd`
2. Go to install directory: `$ cd h5pyd`
3. Build and install: `$ python setup.py install`
4. Go to apps dir: `$ cd apps`
5. Download the following file: `$ wget https://s3.amazonaws.com/hdfgroup/data/hdf5test/tall.h5`
6. In the following steps use the password that was setup for the test_user1 account in place of <passwd>
7. Create a test folder on HSDS: `$ python hstouch.py -u test_user1 -p <passwd> /home/test_user1/test/` 
8. Import into hsds: `$ python hsload.py -v -u test_user1 -p <passwd> tall.h5 /home/test_user1/test/tall.h5`
9. Verify upload: `$ python hsls.py -r -u test_user1 -p <passwd> /home/test_user1/test/tall.h5
 

