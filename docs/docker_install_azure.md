Installation with Docker on Azure
=================================

Export environment variables as shown in "Sample .bashrc" below.

1. Create a Ubuntu-based VM, with port 80 open.  Ssh to the VM
2. Install Python 3 (e.g. with Miniconda <https://docs.conda.io/en/latest/miniconda.html>)
3. Install azure-storage-blob Python packages (`$pip install azure-storage-blob`)
4. Install aiohttp Python package (`$pip install aiohttp`)
5. Install Docker and docker-compose if necessary (see "Docker Setup" below)
6. Create a container for HSDS, using Azure Portal
7. Get project source code: `$ git clone https://github.com/HDFGroup/hsds`
8. Go to admin/config directory: `$ cd hsds/admin/config`
9. Copy the file "passwd.default" to "passwd.txt".  Add any usernames/passwords you wish.  Modify existing passwords (for admin, test_user1, test_user2) for security.
10. Create environment variables as in "Sample .bashrc" below
11. From hsds directory, build docker image:  `$ docker build -t hdfgroup/hsds .`
12. Start the service `$./runall.sh <n>` where n is the number of containers desired (defaults to 1)
13. Run `$ docker ps` and verify that the containers are running: hsds_head, hsds_sn_[1-n], hsds_dn_[1-n]
14. Go to <http://hsds.hdf.test/about> and verify that "cluster_state" is "READY" (might need to give it a minute or two)
15. Run the integration test: `$ python testall.py --skip_unit`


Sample .bashrc
--------------

These environment variables will be passed to the Docker containers on start up.

    export AZURE_CONNECTION_STRING=1234567890      # use the connection string for your Azure account 
    export BUCKET_NAME=hsds.test                   # set to the name of the container you will be using
    export HSDS_ENDPOINT=http://hsds.hdf.test      # Set to the DNS name of the VM.  Use https protocal if SSL is desired
    export HDF5_SAMPLE_BUCKET=""    


Docker Setup
------------

The following are instructions for installing docker on Linux/CentOS.  Details for other Linux distros
may vary.  For OS X, see: <https://docs.docker.com/engine/installation/>.

Run the following commands to install Docker on Linux/CentOS:

    $ sudo yum install docker
    $ sudo service docker start
    $ sudo chkconfig --level 300 docker on
    $ sudo groupadd docker # if group docker doesn't exist already
    $ sudo gpasswd -a $USER docker
    # log out and back in again (may also need to stop/start docker service)
    $ docker ps  # verify

Install docker-compose.  See: <https://docs.docker.com/compose/install/>

Post Install Configuration
--------------------------

The following is some optional configuration steps to create test files and configure
user home folders.

1. Install h5py: `$ pip install h5py`
2. Install h5pyd (Python client SDK): `$ pip install h5pyd`
3. Download the following file: `$ wget https://s3.amazonaws.com/hdfgroup/data/hdf5test/tall.h5`
4. In the following steps use the password that was setup for the test_user1 account in place of \<passwd\>
5. Create a test folder on HSDS: `$ hstouch -u test_user1 -p <passwd> /home/test_user1/test/` 
6. Import into hsds: `$ hsload -v -u test_user1 -p <passwd> tall.h5 /home/test_user1/test/`
7. Verify upload: `$ hsls -r -u test_user1 -p <passwd> /home/test_user1/test/tall.h5
8. To setup home folders, for each username in the passwd file (other than admin and test_user1), create a top-level domain: `$ hstouch -o <username> /home/<username>/`

Installing Software Updates
---------------------------

To get the latest codes changes from the HSDS repo do the following:

1. Shutdown the service: `$ stopall.sh`
2. Get code changes: `$ git pull`
3. Build a new Docker image: `$ docker-compose build
4. Start the service: `$ ./runall.sh`
