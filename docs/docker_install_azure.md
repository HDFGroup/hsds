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

1. Install azure-cli: `pip install azure-cli`
2. Validate runtime version az-cli is at least 2.0.80: `az version`
3. Login to Azure Subscription using AZ-Cli. `az login`
4. After successful login, the list of avaialble subscriptions will be displayed. If you have access to more than one subscription, set the proper subscription to be used: `az account set --subscription [name]`
5. Run the following commands to create Azure Resource Group `az group create --name $RESOURCEGROUP --location $LOCATION`


Virtual Machine Setup
---------------------

1. Create an Ubuntu Virtual Machine: `az vm create
  --resource-group $RESOURCEGROUP
  --name $VMNAME
  --image UbuntuLTS
  --admin-username $ADMINUSER
  --public-ip-address-dns-name $VMNAME
  --generate-ssh-keys`<br/>
The `--generate-ssh-keys` parameter is used to automatically generate an SSH key, and put it in the default key location (~/.ssh). To use a specific set of keys instead, use the `--ssh-key-value` option.<br/>**Note:**: To use $VMNAME as your public DNS name, it will need to be unique across the $LOCATION the VM is located.
2. The above command will output values after the successful creation of the VM.  Keep the publicIpAddress for use below.
3. Open port 80 to web traffic: `az vm open-port --port 80 --resource-group $RESOURCEGROUP --name $VMNAME`
4. Create storage account if one does not exist: `az storage account create -n $STORAGEACCTNAME -g $RESOURCEGROUP -l $LOCATION --sku Standard_LRS`
5. Create a container for HSDS in the storage account: `az storage container create --name $BUCKET_NAME --connection-string $AZURE_CONNECTION_STRING`

Note: The connection string for the storage account can be found in the portal under Settings > Access keys on the storage account or via this cli command: `az storage account show-connection-string -n $STORAGEACCTNAME -g $RESOURCEGROUP`

Install HSDS on Virtual Machine
-------------------------------

On the VM, export environment variables as shown in "Sample .bashrc" below. **IMPORTANT:** If you are not adding these variables into your .bashrc, they must be exported in step 7 below, after Docker is installed.

These environment variables will be passed to the Docker containers on startup.

    export AZURE_CONNECTION_STRING="1234567890"      # use the connection string for your Azure account. Note the quotation marks around the string
    export BUCKET_NAME=hsdstest                   # set to the name of the container you will be using
    export HSDS_ENDPOINT=http://myvmname.westus.cloudapp.azure.com      # Set to the public DNS name of the VM.  Use https protocol if SSL is desired and configured

Follow the following steps to setup HSDS:

1. SSH to the VM created above.  Replace [publicIpAddress] with the public IP dispayed in the ouput of your VM creation command above: `ssh azureuser@[publicIpAddress]`
2. Install Docker and docker-compose if necessary (see "Docker Setup" below)
3. Ensure the proper container for HSDS is created (Step 10 in "Set up your Azure environment")
4. Get project source code: `git clone https://github.com/HDFGroup/hsds`
5. Go to admin/config directory: `cd hsds/admin/config`
6. Copy the file "passwd.default" to "passwd.txt".  Add any usernames/passwords you wish.  Modify existing passwords (for admin, test_user1, test_user2) for security.
7. Create environment variables as in "Sample .bashrc" above
8. From hsds directory, build docker image:  `docker build -t hdfgroup/hsds .`
9. Start the service `./runall.sh <n>` where n is the number of containers desired (defaults to 1)
10. Run `docker ps` and verify that the containers are running: hsds_head, hsds_sn_[1-n], hsds_dn_[1-n]
11. Run `curl $HSDS_ENDPOINT/about` where and verify that "cluster_state" is "READY" (might need to give it a minute or two)

Docker Setup
------------

The following are instructions for installing Docker on Linux/Ubuntu.  Details for other Linux distros
may vary.

Run the following commands to install Docker on Linux/Ubuntu:

1. `sudo apt-get update`
2. `sudo apt install docker.io`
3. `sudo systemctl start docker`
4. `sudo systemctl enable docker`
5. `sudo groupadd docker` if group docker doesn't exist already
6. `sudo gpasswd -a $USER docker`
7. Log out and back in again (you may also need to stop/start docker service)
8. `docker ps` to verify that Docker is running.

Install docker-compose.

1. See: <https://docs.docker.com/compose/install/>

Post Install Configuration and Testing
--------------------------------------

The following configuration can be run on your local machine to verify the installation, and configure
user home folders. **Important:** trailing slashes are essential here.  These steps can be run
on the server VM or on your client.

1. Install pip if not installed: `sudo apt install python-pip`
2. Set an environment variable: ADMIN_PASSWORD with the value used in the password.txt file.  E.g.: `export ADMIN_PASSWORD=admin`
3. Set an environment varaible: USER_PASSWORD with the password for test_user1 in the password.txt file.  E.g.: `export USER_PASSWORD=test`
4. Get the hsds project if you haveen't already: `git clone https://github.com/HDFGroup/hsds`
5. In the hsds directory, run the integration test: `python testall.py --skip_unit`. Ignore `WARNING: is test data setup?` messages for now
6. Install h5py: `pip install h5py`
7. Install h5pyd (Python client SDK): `pip install h5pyd`
8. Configure h5pyd: `hsconfigure`
Server endpoint: $HSDS_ENDPOINT enviornment variable
Username: from hsds/admin/config/passwd.txt file above
Password: from hsds/admin/config/passwd.txt file above
9. To setup test data, download the following file: `wget https://s3.amazonaws.com/hdfgroup/data/hdf5test/tall.h5`
10. Create a test folder: `hstouch -u test_user1 -p $USER_PASSWORD /home/test_user1/test/`
11. Import into hsds: `hsload -v -u test_user1 -p $USER_PASSWORD tall.h5 /home/test_user1/test/`
12. Verify upload: `hsls -r -u test_user1 -p $USER_PASWORD /home/test_user1/test/tall.h5`
13. Rerun the integration test: `python testall.py --skip_unit`.  You should not see any WARNING messages now
14. Create home folders for other users if desired: `python hstouch -u admin -p $ADMIN_PASSWORD -o USERNAME /home/USERNAME/`

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
