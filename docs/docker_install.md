

Installation with Docker
========================

Export environment variables as shown in "Sample .bashrc" below.

1. Install Docker and docker-compose if necessary (see "Docker Setup" below)
2. Create a bucket for HSDS, using aws cli tools or aws management console
3. Get project source code: `$ git clone https://github.com/HDFGroup/hsds`
4. Go to admin/config directory: `$ cd hsds/admin/config`
5. Copy the file "passwd.default" to "passwd.txt".  Add any usernames/passwords you wish
6. Add the DNS for the service to the /etc/hosts file.  E.g. `127.0.0.1  hsds.hdf.test` (can use any valid DNS name) if you running containers directly on the host, or `192.168.99.100  hsds.hdf.test` if using docker machine (use `docker-machine ip` to get the IP address)
7. Create environment variables as in "Sample .bashrc" below
8. From hsds directory, build docker image:  `$ docker build -t hdfgroup/hsds .`
9. Start the service `$./runall.sh <n>` where n is the number of containers desired (defaults to 1)
10. Run `$ docker ps` and verify that the containers are running: hsds_head, hsds_sn_[1-n], hsds_dn_[1-n]
11. Go to <http://hsds.hdf.test/about> and verify that "cluster_state" is "READY" (might need to give it a minute or two)
12. Install Anaconda: <https://conda.io/docs/user-guide/install/linux.html>  (install for python 3.6)
13. Install h5pyd: `$ pip install h5pyd`
14. Run: `$ hsconfigure`.  Set hs endpoint with DNS name (e.g. <http://hsds.hdf.test>) and admin username/password.  Ignore API Key.
15. Run: `$ hsinfo`.  Server state should be "`READY`".  Ignore the "Not Found" error for the admin home folder
16. Create "/home" folder: `$ hstouch /home/`.  Note: trailing slash is important!
17. For each username in the passwd file, create a top-level domain: `$ hstouch -o <username> /home/<username>/`
18. Run the integration test: `$ python testall.py --skip_unit`
19. The test suite will emit some warnings due to test domains not being loaded.  To address see test_data_setup below.

Sample .bashrc
--------------

These environment variables will be passed to the Docker containers on start up.

::

    export AWS_ACCESS_KEY_ID=1234567890            # user your AWS account access key if using S3 (Not needed if running on EC2 and AWS_IAM_ROLE is defined)
    export AWS_SECRET_ACCESS_KEY=ABCDEFGHIJKL      # use your AWS account access secret key if using S3  (Not needed if running on EC2 and AWS_IAM_ROLE is defined)
    export BUCKET_NAME=hsds.test                   # set to the name of the bucket you will be using
    export AWS_REGION=us-east-1                    # for boto compatibility - for S3 set to the region the bucket is in
    export AWS_S3_GATEWAY=http://s3.amazonaws.com  # Use AWS endpoint for region where bucket is
    export HSDS_ENDPOINT=http://hsds.hdf.test    # use https protocal if SSL is desired
    # For S3, set AWS_S3_GATEWAY to endpoint for the region the bucket is in.  E.g.: http://s3.amazonaws.com.
    # See http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region for list of endpoints.


Docker Setup
------------

The following are instructions for installing docker on Linux/CentOS.  Details for other Linux distros
may vary.  For OS X, see: <https://docs.docker.com/engine/installation/>.

Run the following commands to install Docker on Linux/CentOS:

    ```
    \$ sudo yum install docker
    \$ sudo service docker start
    \$ sudo chkconfig --level 300 docker on
    \$ sudo groupadd docker # if group docker doesn't exist already
    \$ sudo gpasswd -a $USER docker
    # log out and back in again (may also need to stop/start docker service)
    \$ docker ps  # verify
    ```

Install docker-compose.  See: <https://docs.docker.com/compose/install/>


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

Installing Software Updates
---------------------------

To get the latest codes changes from the HSDS repo do the following:

1. Shutdown the service: `$ stopall.sh`
2. Get code changes: `$ git pull`
3. Build a new Docker image: `$ docker-compose build
4. Start the service: `$ ./runall.sh`
