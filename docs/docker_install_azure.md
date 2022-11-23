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

Follow the following steps to setup HSDS:

1. SSH to the VM created above.  Replace [publicIpAddress] with the public IP displayed in the output of your VM creation command above: `ssh $VM_USER@[publicIpAddress]`
2. Install Docker and docker-compose if necessary.   See [Docker Setup](setup_docker.md)
3. Get project source code: `git clone https://github.com/HDFGroup/hsds`
4. If you plan to use HTTP Basic Auth (usernames and passwords managed by the service), go to hsds/admin/config directory: `cd admin/config`, and copy the file "passwd.default" to "passwd.txt".  Add any usernames/passwords you wish.  Modify existing passwords (for admin, test_user1, test_user2, etc.) for security.  If you wish to use Azure Active Directory for authentication, follow the instructions in [Azure Active Directory Setup](azure_ad_setup.md)
5. If group-level permissions are desired (See [Authorization](authorization.md)), copy the file "groups.default" to "groups.txt".  Modify existing groups as needed
6. Create environment variables as in "Sample .bashrc" above.  Or run: `source ~/.bashrc` if you have added them to the bashrc file
7. From the hsds directory (`cd ~/hsds`), start the service `./runall.sh <n>` where n is the number of containers desired (defaults to 4)
8. Run `docker ps` and verify that the containers are running: hsds_head, hsds_sn_1, hsds_dn_[1-n]
9. Run `curl $HSDS_ENDPOINT/about` where and verify that "cluster_state" is "READY" (might need to give it a minute or two)
10. Perform post install configuration.   See: [Post Install Configuration](post_install.md)


Installing Software Updates
---------------------------

To get the latest codes changes from the HSDS repo do the following:

1. Shutdown the service: `./stopall.sh`
2. Get code changes: `git pull`
3. Rebuild the Docker image: `./build.sh`
4. Start the service: `./runall.sh`
