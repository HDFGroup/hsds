<h1>Installation with Docker on Azure</h1>

To begin, export environment variables as shown in "Sample .bashrc" below.

**Sample .bashrc**

These environment variables will be used to create Azure resources.
<pre><code><small>
export RESOURCEGROUP=myresouregroup
export LOCATION=westus
export VMNAME=myvmname
export ADMINUSER=azureuser
export STORAGEACCTNAME=mystorageaccount

# the following will be the same as the variables exported on the VM below
export AZURE_CONNECTION_STRING="1234567890"      # use the connection string for your Azure account. Note the quotation marks around the string 
export BUCKET_NAME=hsdstest                   # set to the name of the container you will be using
</small></code></pre>

<h2>Prerequisites</h2>

1. Install pip on your local machine if it is not already installed:<br/>`sudo apt-get update && sudo apt-get -y upgrade`<br/>`sudo apt-get install python-pip`

<h2> Set up your Azure environment</h2>

1. Install az-cli <br>`curl -L https://aka.ms/InstallAzureCli | bash`</br>
2. Validate runtime version az-cli is at least 2.0.80: `az version`
3. Login to Azure Subscription using AZ-Cli. `az login`
4. After successful login, the list of avaialble subscriptions will be displayed. If you have access to more than one subscription, set the proper subscription to be used: `az account set --subscription [name]`
5. Run the following commands to create Azure Resource Group:<br>`az group create --name $RESOURCEGROUP --location $LOCATION`</br>
6. Create an Ubuntu Virtual Machine:<br/>`az vm create 
  --resource-group $RESOURCEGROUP 
  --name $VMNAME 
  --image UbuntuLTS 
  --admin-username $ADMINUSER 
  --generate-ssh-keys`

The `--generate-ssh-keys` parameter is used to automatically generate an SSH key, and put it in the default key location (~/.ssh). To use a specific set of keys instead, use the `--ssh-key-value` option.

7. The above command will output values after the successful creation of the VM.  Keep the publicIpAddress for use below.
8. Open port 80 to web traffic:<br/>`az vm open-port --port 80 --resource-group $RESOURCEGROUP --name $VMNAME`
9. Create storage account if one does not exist:<br/> `az storage account create -n $STORAGEACCTNAME -g $RESOURCEGROUP -l $LOCATION --sku Standard_LRS`
10. Create a container for HSDS in the storage account:<br/>`az storage container create --name $BUCKET_NAME --connection-string $AZURE_CONNECTION_STRING`

Note: The connection string for the storage account can be found in the portal under Settings > Access keys on the storage account or via this cli command: `az storage account show-connection-string -n $STORAGEACCTNAME -g $RESOURCEGROUP`

<h2>Set up HSDS on VM</h2>

On the VM, export environment variables as shown in "Sample .bashrc" below. **IMPORTANT:** If you are not adding these variables into your .bashrc, they must be exported in step 11 below, after Miniconda and Docker are installed.

**Sample .bashrc**

These environment variables will be passed to the Docker containers on startup.
<pre><code><small>
export AZURE_CONNECTION_STRING="1234567890"      # use the connection string for your Azure account. Note the quotation marks around the string
export BUCKET_NAME=hsdstest                   # set to the name of the container you will be using
export HSDS_ENDPOINT=http://0.0.0.0      # Set to the public IP of the VM (or DNS name if DNS configured).  Use https protocol if SSL is desired and configured
</small></code></pre>

1. SSH to the VM created above.  Replace [publicIpAddress] with the public IP dispayed in the ouput of your VM creation command above.<br/>`ssh azureuser@[publicIpAddress]`
2. Install Python 3 (e.g. with Miniconda <https://docs.conda.io/en/latest/miniconda.html>)
3. Install pip:<br/>`sudo apt-get update && sudo apt-get -y upgrade`<br/>
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
16. Install pytz module: `pip install pytz`
17. Run the integration test: `python testall.py --skip_unit`<br/>Note: not all integration tests will pass unless the Post Install Configuration steps below are completed.


<h2>Docker Setup</h2>

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

<h2>Post Install Configuration</h2>

The following is some optional configuration steps to create test files and configure
user home folders. **Important:** trailing slashes are essential here.

1. Install h5py: `pip install h5py`
2. Install h5pyd (Python client SDK): `pip install h5pyd`
3. Download the following file: `wget https://s3.amazonaws.com/hdfgroup/data/hdf5test/tall.h5`
4. Configure HSDS: `hsconfigure`<br/>
Server endpoint: $HSDS_ENDPOINT enviornment variable<br/>
Username: from hsds/admin/config/passwd.txt file above<br/>
Password: from hsds/admin/config/passwd.txt file above
5. Set up home directory: `hstouch -o admin /home/`
6. Set up home folders for each username in the passwd file: 
`hstouch -o <username> /home/<username>/`
7. In the following steps use the password that was setup for the test_user1 account in place of \<passwd\>
8. Create a test folder on HSDS: `hstouch -u test_user1 -p <passwd> /home/test_user1/test/` 
9. Import into hsds: `hsload -v -u test_user1 -p <passwd> tall.h5 /home/test_user1/test/`
10. Verify upload: `hsls -r -u test_user1 -p <passwd> /home/test_user1/test/tall.h5`
11. In the hsds directory, run the integration test: `python testall.py --skip_unit`


<h2>Installing Software Updates</h2>

To get the latest codes changes from the HSDS repo do the following:

1. Shutdown the service: `./stopall.sh`
2. Get code changes: `git pull`
3. Rebuild the Docker image: `./build.sh`
4. Start the service: `./runall.sh`

<h2>Updating passwords</h2>

To change passwords or add new user accounts do the following:

1. Shutdown the service: `./stopall.sh`
2. Add new username/passwords to the hsds/admin/config/passwd.txt file
3. Rebuild the Docker image: `./build.sh`
4. Start the service: `./runall.sh`