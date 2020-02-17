Installation with Docker on Azure
=================================

To begin, export environment variables as shown in "Sample .bashrc" below.

Sample .bashrc
--------------

These environment variables will be used to create Azure resources.

    export RESOURCEGROUP=myresouregroup
    export LOCATION=westus
    export VMNAME=myvmname
    export ADMINUSER=azureuser
    export STORAGEACCTNAME=mystorageaccount

    # the following will be the same as the variables exported on the VM below
    export AZURE_CONNECTION_STRING="1234567890"      # use the connection string for your Azure account.                                                     # Note the quotation marks around the string
    export BUCKET_NAME=hsdstest                   # set to the name of the container you will be using

Prerequisites
-------------

Setup Pip and Python 3 on your local machine if not already installed (e.g. with Miniconda <https://docs.conda.io/en/latest/miniconda.html>)


Set up your Azure environment
-----------------------------

1. Install az-cli: `curl -L https://aka.ms/InstallAzureCli | bash`
2. Validate runtime version az-cli is at least 2.0.80: `az version`
3. Login to Azure Subscription using AZ-Cli. `$az login`
4. After successful login, the list of avaialble subscriptions will be displayed. If you have access to more than one subscription, set the proper subscription to be used: `az account set --subscription [name]`
5. Run the following commands to create Azure Resource Group `az group create --name $RESOURCEGROUP --location $LOCATION`
6. Create an Ubuntu Virtual Machine: `az vm create
  --resource-group $RESOURCEGROUP
  --name $VMNAME
  --image UbuntuLTS
  --admin-username $ADMINUSER
  --generate-ssh-keys`
The `--generate-ssh-keys` parameter is used to automatically generate an SSH key, and put it in the default key location (~/.ssh). To use a specific set of keys instead, use the `--ssh-key-value` option.
7. The above command will output values after the successful creation of the VM.  Keep the publicIpAddress for use below.
8. Open port 80 to web traffic: `az vm open-port --port 80 --resource-group $RESOURCEGROUP --name $VMNAME`
9. Create storage account if one does not exist: `az storage account create -n $STORAGEACCTNAME -g $RESOURCEGROUP -l $LOCATION --sku Standard_LRS`
10. Create a container for HSDS in the storage account: `az storage container create --name $BUCKET_NAME --connection-string $AZURE_CONNECTION_STRING`

Note: The connection string for the storage account can be found in the portal under Settings > Access keys on the storage account or via this cli command: `az storage account show-connection-string -n $STORAGEACCTNAME -g $RESOURCEGROUP`

Set up HSDS on VM
-----------------

On the VM, export environment variables as shown in "Sample .bashrc" below. **IMPORTANT:** If you are not adding these variables into your .bashrc, they must be exported in step 11 below, after Miniconda and Docker are installed.

Sample .bashrc for VM
---------------------

These environment variables will be passed to the Docker containers on startup.

    export AZURE_CONNECTION_STRING="1234567890"      # use the connection string for your Azure account. Note the quotation marks around the string
    export BUCKET_NAME=hsdstest                   # set to the name of the container you will be using
    export HSDS_ENDPOINT=http://0.0.0.0      # Set to the public IP of the VM (or DNS name if DNS  configured).  Use https protocol if SSL is desired and configured

1. SSH to the VM created above.  Replace [publicIpAddress] with the public IP dispayed in the ouput of your VM creation command above: `ssh azureuser@[publicIpAddress]`
2. Install Python 3 (e.g. with Miniconda <https://docs.conda.io/en/latest/miniconda.html>)
3. Install pip: `sudo apt-get update && sudo apt-get -y upgrade`
`sudo apt-get install python-pip`
4. Install azure-storage-blob Python packages: `pip install azure-storage-blob`
5. Install aiohttp Python package: `pip install aiohttp`
6. Install Docker and docker-compose if necessary (see "Docker Setup" below)
7. Ensure the proper container for HSDS is created (Step 10 in "Set up your Azure environment")
8. Get project source code: `git clone https://github.com/HDFGroup/hsds`
9. Go to admin/config directory: `cd hsds/admin/config`
10. Copy the file "passwd.default" to "passwd.txt".  Add any usernames/passwords you wish.  Modify existing passwords (for admin, test_user1, test_user2) for security.
11. Create environment variables as in "Sample .bashrc" above
12. From hsds directory, build docker image:  `docker build -t hdfgroup/hsds .`
13. Start the service `./runall.sh <n>` where n is the number of containers desired (defaults to 1)
14. Run `docker ps` and verify that the containers are running: hsds_head, hsds_sn_[1-n], hsds_dn_[1-n]
15. Go to <http://hsds.hdf.test/about> and verify that "cluster_state" is "READY" (might need to give it a minute or two)

Docker Setup
------------

The following are instructions for installing Docker on Linux/Ubuntu.  Details for other Linux distros
may vary.

Run the following commands to install Docker on Linux/Ubuntu:

1. `sudo apt install docker.io`
2. `sudo systemctl start docker`
3. `sudo systemctl enable docker`
4. `sudo groupadd docker` if group docker doesn't exist already
5. `sudo gpasswd -a $USER docker`
6. Log out and back in again (you may also need to stop/start docker service)
7. `docker ps` to verify that Docker is running.

Install docker-compose.

1. See: <https://docs.docker.com/compose/install/>

Post Install Configuration and Testing
--------------------------------------

The following is some optional configuration steps to create test files and configure
user home folders. **Important:** trailing slashes are essential here.  These steps can be run
on the server VM or on your client.

1. Set an environment variable: ADMIN_PASSWORD with the value used in the password.txt file.  E.g.: `export ADMIN_PASSWORD=admin`
2. Set an environment varaible: USER_PASSWORD with the password for test_user1 in the password.txt file.  E.g.: `export USER_PASSWORD=test`
3. In the hsds directory, run the integration test: `python testall.py --skip_unit`. Ignore `WARNING: is test data setup?` messages for now
4. Install h5py: `pip install h5py`
5. Install h5pyd (Python client SDK): `pip install h5pyd`
6. Configure h5pyd: `hsconfigure`
Server endpoint: $HSDS_ENDPOINT enviornment variable
Username: from hsds/admin/config/passwd.txt file above
Password: from hsds/admin/config/passwd.txt file above
7. To setup test data, download the following file: `wget https://s3.amazonaws.com/hdfgroup/data/hdf5test/tall.h5`
8. Import into hsds: `hsload -v -u test_user1 -p $USER_PASSWORD tall.h5 /home/test_user1/test/`
9. Verify upload: `hsls -r -u test_user1 -p $USER_PASWORD /home/test_user1/test/tall.h5`
10. Rerun the integration test: `python testall.py --skip_unit`.  You should not see any WARNING messages now

Installing Software Updates
---------------------------

To get the latest codes changes from the HSDS repo do the following:

1. Shutdown the service: `./stopall.sh`
2. Get code changes: `git pull`
3. Rebuild the Docker image: `./build.sh`
4. Start the service: `./runall.sh`

Updating passwords
------------------

To change passwords or add new user accounts do the following:

1. Shutdown the service: `./stopall.sh`
2. Add new username/passwords to the hsds/admin/config/passwd.txt file
3. Rebuild the Docker image: `./build.sh`
4. Start the service: `./runall.sh`
