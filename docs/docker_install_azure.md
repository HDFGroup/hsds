Installation with Docker on Azure
=================================

To begin, export environment variables as shown in "Sample .bashrc" below.

Sample .bashrc
--------------

These environment variables will be used to create Azure resources.

    export RESOURCEGROUP=myresouregroup
    export LOCATION=westus
    export VMNAME=myvmname
    export VM_USER=azureuser
    export ADMIN_USER=admin  # or AD username if AD is used
    export STORAGEACCTNAME=mystorageaccount

    # the following will be the same as the variables exported on the VM below
    export AZURE_CONNECTION_STRING="1234567890"      # use the connection string for your Azure account.                                                     # Note the quotation marks around the string
    export BUCKET_NAME=hsdstest                   # set to the name of the container you will be using

    # the following will be used on the VM if Azure Active Directory authentication is desired
    # See "Azure Active Directory" section below
    export AZURE_APP_ID=12345678-1234-1234-abcd-123456789ab          # if you will be using Azure Active Directory, set this to the application ID
    export AZURE_RESOURCE_ID=00000002-0000-0000-c000-000000000000    # if you will be using Azure Active Directory, set this to the resource ID

Prerequisites
-------------

Setup Pip and Python 3 on your local machine if not already installed (e.g. with Miniconda <https://docs.conda.io/en/latest/miniconda.html>)

Set up your Azure environment
-----------------------------

1. Install azure-cli: `pip install azure-cli`
2. Validate runtime version az-cli is at least 2.0.80: `az version`
3. Login to Azure Subscription using AZ-Cli. `az login`
4. After successful login, the list of available subscriptions will be displayed. If you have access to more than one subscription, set the proper subscription to be used: `az account set --subscription [name]`
5. Run the following commands to create Azure Resource Group `az group create --name $RESOURCEGROUP --location $LOCATION`

Virtual Machine Setup
---------------------

1. Create an Ubuntu Virtual Machine: `az vm create
  --resource-group $RESOURCEGROUP
  --name $VMNAME
  --image UbuntuLTS
  --admin-username $VM_USER
  --public-ip-address-dns-name $VMNAME
  --location $LOCATION
  --generate-ssh-keys`<br/>
The `--generate-ssh-keys` parameter is used to automatically generate an SSH key, and put it in the default key location (~/.ssh). To use a specific set of keys instead, use the `--ssh-key-value` option.<br/>**Note:**: To use $VMNAME as your public DNS name, it will need to be unique across the $LOCATION the VM is located.
2. The above command will output values after the successful creation of the VM.  Keep the publicIpAddress for use below.
3. Open port 80 to web traffic: `az vm open-port --port 80 --resource-group $RESOURCEGROUP --name $VMNAME`
4. Create a storage account if one does not exist: `az storage account create -n $STORAGEACCTNAME -g $RESOURCEGROUP -l $LOCATION --sku Standard_LRS`
5. Create a container for HSDS in the storage account: `az storage container create --name $BUCKET_NAME --connection-string $AZURE_CONNECTION_STRING`

Note: The connection string for the storage account can be found in the portal under Settings > Access keys on the storage account or via this cli command: `az storage account show-connection-string -n $STORAGEACCTNAME -g $RESOURCEGROUP`

Install HSDS on Virtual Machine
-------------------------------

On the VM, export environment variables as shown in "Sample .bashrc" below. **IMPORTANT:** If you are not adding these variables into your .bashrc, they must be exported in step 7 below, after Docker is installed.

These environment variables will be passed to the Docker containers on startup.

    export BUCKET_NAME=hsdstest                   # set to the name of the container you will be using
    export HSDS_ENDPOINT=http://myvmname.westus.cloudapp.azure.com      # Set to the public DNS name of the VM.  Use https protocol if SSL is desired and configured
    export AZURE_CONNECTION_STRING="1234567890"      # use the connection string for your Azure account. Note the quotation marks around the string
    export ADMIN_USER=admin  # The username for the HSDS admin acount.  Set to an AD username if Active Directory is being used
    export AZURE_APP_ID=12345678-1234-1234-abcd-123456789ab          # if you will be using Azure Active Directory, set this to the application ID
    export AZURE_RESOURCE_ID=00000002-0000-0000-c000-000000000000    # if you will be using Azure Active Directory, set this to the resource ID

Follow the following steps to setup HSDS:

1. SSH to the VM created above.  Replace [publicIpAddress] with the public IP displayed in the output of your VM creation command above: `ssh $VM_USER@[publicIpAddress]`
2. Install Docker and docker-compose if necessary (see "Docker Setup" below)
3. Get project source code: `git clone https://github.com/HDFGroup/hsds`
4. If you plan to use HTTP Basic Auth (usernames and passwords managed by the service), go to hsds/admin/config directory: `cd admin/config`, and copy the file "passwd.default" to "passwd.txt".  Add any usernames/passwords you wish.  Modify existing passwords (for admin, test_user1, test_user2, etc.) for security.  If you wish to use Azure Active Directory for authentication, following the instructions in "Azure Active Directory"
5. Create environment variables as in "Sample .bashrc" above.  Or run: `source ~/.bashrc` if you have added them to the bashrc file
6. From the hsds directory (`cd ~/hsds`), start the service `./runall.sh <n>` where n is the number of containers desired (defaults to 1)
7. Run `docker ps` and verify that the containers are running: hsds_head, hsds_sn_1, hsds_dn_[1-n]
8. Run `curl $HSDS_ENDPOINT/about` where and verify that "cluster_state" is "READY" (might need to give it a minute or two)

Post Install Configuration and Testing
--------------------------------------

The following configuration can be run on your local machine to verify the installation, and configure
user home folders. **Important:** trailing slashes are essential here.  These steps can be run
on the server VM or on your client.

1. Install h5py: `pip install h5py`
2. Install h5pyd (Python client SDK): `pip install h5pyd`
3. Configure user credentials for admin user:  
    * If using HTTP Basic Auth, run: `hsconfigure`, and provide the following responses:
        1. For server endpoint: use $HSDS_ENDPOINT value
        2. Username: use $ADMIN_PASSWORD value
        3. Password: use password from passwd.txt file
    * If using Active Directory, create a file ".hscfg" in your home folder with the following lines:
        1. `hs_endpoint = <server_endpoint>`
        2. `hs_ad_app_id = <AD Application (client) ID>`
        3. `hs_ad_tenant_id = <AD tenant_id>`
        4. `hs_ad_resource_id = <AD resource id>`
4. Create home folder on server: `hstouch /home/`.  If using AD, select the admin account when prompted.
5. For each user, create a user home folder: `hstouch -o <username> /home/<username>`
6. Change user credentials to non-admin account
    * If using HTTP Basic Auth, run: `hsconfigure`, and change values for username and password
    * If using Active Directly, login to a non-admin account when prompted (may needd to remove cached credentials: `rm ~/.hsazcfg*`)
7. Set environment variable for test output folder: `export H5PYD_TEST_FOLDER="/home/<username>/h5pyd_test/"`
8. Create folder for test files: `hstouch $H5PYD_TEST_FOLDER`
9. Get h5pyd code: `git clone https://github.com/HDFGroup/h5pyd`
10. Go to the h5pyd directory: `cd h5pyd`
11. Run h5pyd test suite: `python testall.py`

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

Azure Active Directory
----------------------

Rather than user names and passwords being maintained by HSDS, Azure Active Directory can be used for authentication. To enable, in the portal, go to Azure Active Directory, select "App registrations" and
click the the plus sign, "New registration".  In the register page, chose an appropriate name for the application and select the desired "Supported account types".

In "API permissions", add permissions for "Microsoft Graph, openid", and "Microsoft Graph, User Read".

Next, click "Manifest", and copy the "appId" value and use it to set the AZURE_APP_ID environment variable.  Also on this page, copy the "resourceAppId" value, and use it to set the AZURE_RESOURCE_ID environment variable.

When these settings are used with a HSDS docker deployment, clients will be able to authenticate using their Active Directory username and password.

Installing Software Updates
---------------------------

To get the latest codes changes from the HSDS repo do the following:

1. Shutdown the service: `./stopall.sh`
2. Get code changes: `git pull`
3. Rebuild the Docker image: `./build.sh`
4. Start the service: `./runall.sh`
