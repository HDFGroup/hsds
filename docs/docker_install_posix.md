Installation using Docker with POSIX storage
============================================

Export environment variables as shown in "Sample .bashrc" below.

1. Install Python 3 (e.g. with Miniconda <https://docs.conda.io/en/latest/miniconda.html>)
2. Create a directory for storage files and set the ROOT_DIR environment variable to point to it
3. Create a subdirectory under ROOT_DIR that will be the default location when "bucket" is not defined.
4. Set the environment variable BUCKET_NAME to the name of the subdirectory
5. Install Docker and docker-compose if necessary.  See [Docker Setup](setup_docker.md)
6. Get project source code: `$ git clone https://github.com/HDFGroup/hsds`
7. Go to admin/config directory: `$ cd hsds/admin/config`
8. Copy the file "passwd.default" to "passwd.txt".  Add any usernames/passwords you wish.  Modify existing passwords (for admin, test_user1, test_user2) for security.
9. If a non-valid DNS name is used for the HSDS_ENDPOINT (e.g. "hsds.hdf.test"), create a /etc/hosts entry for the DNS name
10. Verify the environment variables as in "Sample .bashrc" below
11. From hsds directory, build docker image:  `$ docker build -t hdfgroup/hsds .`
11. From hsds directory, build docker image:  `$ docker build -t hdfgroup/hsds .`
12. Start the service `$./runall.sh <n>` where n is the number of containers desired (defaults to 1)
13. Run `$ docker ps` and verify that the containers are running: hsds_head, hsds_sn_[1-n], hsds_dn_[1-n]
14. Go to <http://hsds.hdf.test/about> and verify that "cluster_state" is "READY" (might need to give it a minute or two)
15. Export the admin password as ADMIN_PASSWORD  (only needed the first time integration tests are run)
16. Run the integration test: `$ python testall.py --skip_unit`
17. The test suite will emit some warnings due to test domains not being loaded.  To address see "Post Install configuration" below.

Sample .bashrc
--------------

These environment variables will be passed to the Docker containers on start up.

    export BUCKET_NAME=hsds.test                 # set to the name of the bucket you will be using (should be subdir of TOP_DIR)
    export HSDS_ENDPOINT=http://hsds.hdf.test    # Use the machines DNS name or create virtual name in /etc/hosts
    unset AWS_S3_GATEWAY                         # To avoid use of S3 storage
    unset AZURE_CONNECTION_STRING                # to avoid use of Azure Blob storage


Post Install Configuration
--------------------------

See [Post Install Configuration](post_install.md)

Installing Software Updates
---------------------------

To get the latest codes changes from the HSDS repo do the following:

1. Shutdown the service: `$ stopall.sh`
2. Get code changes: `$ git pull`
3. Build a new Docker image: `$ docker-compose build`
4. Start the service: `$ ./runall.sh`
