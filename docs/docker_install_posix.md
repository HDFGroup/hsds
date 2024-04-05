Installation using Docker with POSIX storage
============================================

HSDS can be used with POSIX storage (i.e. an ordinary file system) rather than object storage.  While not recommended for use with AWS or Azure (e.g. if the disk crashes, all data will be lost), using POSIX is convienent for testing or trial installations on a desktop, or do utilize existing HDF5 file archives.

Export environment variables as shown in "Sample .bashrc" below.

1. Install Python 3 (e.g. with Miniconda <https://docs.conda.io/en/latest/miniconda.html>)
2. Create a directory for storage files and set the ROOT_DIR environment variable to point to it
3. Create a subdirectory under ROOT_DIR that will be the default location when "bucket" is not defined.
4. Set the environment variable BUCKET_NAME to the name of the subdirectory
5. Install Docker and docker compose if necessary.  See [Docker Setup](setup_docker.md)
6. Get project source code: `$ git clone https://github.com/HDFGroup/hsds`
7. Go to admin/config directory: `$ cd hsds/admin/config`
8. Copy the file "passwd.default" to "passwd.txt".  Add any usernames/passwords you wish.  Modify existing passwords (for admin, test_user1, test_user2) for security
9. If group-level permissions are desired (See [Authorization](authorization.md)), copy the file "groups.default" to "groups.txt".  Modify existing groups as needed
10. If a non-valid DNS name is used for the HSDS_ENDPOINT (e.g. "hsds.hdf.test"), create a /etc/hosts entry for the DNS name
11. Verify the environment variables as in "Sample .bashrc" below
12. Build the docker image: `$ ./build.sh --nolint`
13. Create the file **admin/config/override.yml** for deployment specific settings (see "Sample override.yml")
14. Start the service `$./runall.sh <n>` where n is the number of containers desired (defaults to 1)
15. Run `$ docker ps` and verify that the containers are running: hsds_head, hsds_sn_[1-n], hsds_dn_[1-n]
16. Go to <http://hsds.hdf.test:5101/about> and verify that "state" is "READY" (might need to give it a minute or two)
17. Perform post install configuration.   See: [Post Install Configuration](post_install.md)


Sample .bashrc
--------------

These environment variables will be passed to the Docker containers on start up.

    export ROOT_DIR=/mnt/data                    # directory that will be the parent of all buckets
    export BUCKET_NAME=hsds.test                 # set to the name of the bucket you will be using (should be subdir of TOP_DIR)
    export SN_PORT=5101                          # port to use
    export HSDS_ENDPOINT=http://hsds.hdf.test:{SN_PORT}    # Use the machines DNS name or create virtual name in /etc/hosts
    unset AWS_S3_GATEWAY                         # To avoid use of S3 storage
    unset AZURE_CONNECTION_STRING                # to avoid use of Azure Blob storage

Sample override.yml
-------------------

Review the contents of **admin/config/config.yml** and create the file **admin/config/override.yml** for any keys where you don't 
wish to use the default value.  Example:

    greeting: This is my custom greeting  # this get display in the /about method
    log_level: DEBUG    # for verbose logging


Installing Software Updates
---------------------------

To get the latest codes changes from the HSDS repo do the following:

1. Shutdown the service: `$ stopall.sh`
2. Get code changes: `$ git pull`
3. Build a new Docker image: `$ docker compose build`
4. Start the service: `$ ./runall.sh`
