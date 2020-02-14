<h1>Installation with Azure Kubernetes</h1>

**Note:** These instructions assume you are using a Linux based system. If you are using Windows please see the special notes at the end.

To begin, Export environment variables as shown in "Sample .bashrc" below.

These environment variables will be used to create Azure resources.
<pre><code><small>
export RESOURCEGROUP=myresouregroup
export AKSCLUSTER=myakscluster
export LOCATION=westus
export ACRNAME=myacrname
export STORAGEACCTNAME=mystorageaccount

# the following will be the same as the variables exported on the cluster below
export AZURE_CONNECTION_STRING="1234567890"      # use the connection string for your Azure account. Note the quotation marks around the string
export BUCKET_NAME=hsdstest                   # set to the name of the container you will be using
export HDF5_SAMPLE_BUCKET=""
</small></code></pre>

<h2>Prerequisites</h2>

1. Install pip on your local machine if it is not already installed:<br/>`sudo apt-get update && sudo apt-get -y upgrade`<br/>`sudo apt-get install python-pip`

<h2> Setup your Azure environment </h2>

Here we will deploy an Azure Storage Account, Azure Container Registry (ACR) and Azure Kubernetes Service (AKS).

1. Install az-cli <br>`curl -L https://aka.ms/InstallAzureCli | bash`</br>
2. Validate runtime version az-cli is at least 2.0.80: `az version`
3. Log in to Azure Subscription using AZ-Cli. `$az login`
4. After successful login, the list of avaialble subscriptions will be displayed. If you have access to more than one subscription, set the proper subscription to be used: `az account set --subscription [name]`
5. Run the following commands to create Azure Resource Group:<br>`az group create --name $RESOURCEGROUP --location $LOCATION`</br>
6. Create storage account <br>`az storage account create -n $STORAGEACCTNAME -g $RESOURCEGROUP -l $LOCATION --sku Standard_LRS`</br>
7. Create a blob container in the storage account <br>`az storage container create --name $BUCKET_NAME --connection-string $AZURE_CONNECTION_STRING --fail-on-exist`</br> 

Note: The connection string for the storage account can be found in the portal under Settings > Access keys on the storage account or via this cli command: `az storage account show-connection-string -n $STORAGEACCTNAME -g $RESOURCEGROUP`

8.  The following command will create the new ACR <br>`az acr create --resource-group $RESOURCEGROUP --name $ACRNAME --sku Basic --admin-enabled true`</br>
9. Install AKS cli <br>`az aks install-cli`</br>
10. Create AKS Cluster and attach to ACR <br>`az aks create -n $AKSCLUSTER -g $RESOURCEGROUP --generate-ssh-keys --attach-acr $ACRNAME`</br>
11. Get access to the AKS Cluster <br>`az aks get-credentials -g $RESOURCEGROUP -n $AKSCLUSTER`</br>


<h2>Prepare and deploy your docker image to ACR</h2>

Currently the HSDS server uses simple auth and the login credentials are embedded in the code. The following procedure builds a docker image with your custom set of credentials. If you are just deploying the HSDS server **for testing purposes only**, import the HDF group's docker image as is into ACR as follows and skip the rest of this section. <br>`az acr import -n $ACRNAME --source docker.io/hdfgroup/hsds:latest --image hsds:v1`</br>


1. Clone the hsds repository in a local folder: <br>`git clone https://github.com/HDFGroup/hsds`</br>
2. Go to admin/config directory: `cd hsds/admin/config`
3. Copy the file "passwd.default" to "passwd.txt".  
4. Add/change usernames/passwords that you want to use. **Note**: Do not keep the original example credentials 
5. From hsds directory, build docker image:  `bash build.sh`
6. Tag the docker image using the ACR scheme: <br>`docker tag hdfgroup/hsds $ACRNAME.azurecr.io/hsds:v1`</br>  where $ACRNAME is the ACR being deployed to, and v1 is the version (update this every time you will be deploying a new version of HSDS). 
7.  Login to the Azure container registry (ACR): <br>`az acr login --name $ACRNAME`</br>
8.  You may also need to login into ACR from docker as follows: <br>Get the ACR admin credentials: `az acr credential show -n ACRNAME`</br>docker login with those credentials: `docker login ACRNAME -u xxx -p xxx`
9. Push the image to Azure ACR: `docker push $ACRNAME.azurecr.io/hsds:v1`
<br>**Note:** Use all lowercase ACRNAME in these commands if your actual ACRNAME includes uppercase characters</br>


<h2>Deploy HSDS to AKS</h2>

1. Create RBAC roles <br>`kubectl create -f k8s_rbac.yml`</br>
2. Create HSDS deployment on the AKS cluster: <br>`$ kubectl apply -f k8s_service_lb_azure.yml` </br>
3. This will create an external load balancer with an http endpoint with a public-ip. For additional configuration options to handle SSL related scenerios please see:   <br>*frontdoor_install_azure.md*</br>
Additional reference for Azure Front Door <https://docs.microsoft.com/en-us/azure/frontdoor/>
4. Now we will deploy the HSDS containers. In *k8s_deployment_azure.yml*, customize the values for:
   - env sections:
     - AZURE_CONNECTION_STRING
     - BUCKET_NAME (this is the name of the blob container created earlier)
   - containers sections 
     - image: 'myacrname.azurecr.io/hsds:v1' to reflect the acr repository for deployment.

5. Apply the deployment: `$ kubectl apply -f k8s_deployment_azure.yml`
6.  Verify that the HSDS pod is running: <br>`$ kubectl get pods`  a pod with a name starting with hsds should be displayed with status as "Running".</br>
7.  Addtional verification: <br>Tail the pod logs (`$ kubectl logs -f hsds-1234 sn`) till you see the line: `All nodes healthy, changing cluster state to READY` (requires log level be set to INFO or lower)</br>
8.  To locally test that HSDS functioning 
       - Create a forwarding port to the Kubernetes service `$ sudo kubectl port-forward hsds-1234 8080:5101` (use another port if 8080 is unavailable)
       - From a browser hit: <http://127.0.0.1:8080/about> and verify that "cluster_state" is "READY"
9.  Use kubectl to get the public-ip of the hsds service: <br>`$kubectl get service`</br>Your should see an entry similar to: 
    <br><pre><small>NAME    TYPE           CLUSTER-IP     EXTERNAL-IP      PORT(S)        AGE
    hsds    LoadBalancer   10.0.242.109   20.36.17.252     80:30326/TCP   23h</small></pre></br>
    Note the public-ip (EXTERNAL-IP). This is where you can access the HSDS service externally. It may take some time for the EXTERNAL-IP to show up after the service deployment

<h2>Test the Deployment using Integration Test and Test Data</h2>    

1. Install Anaconda: <https://docs.conda.io/projects/conda/en/latest/user-guide/install/macos.html#install-macos-silent>  <br>Instructions are for macOS but shuold also work for linux. install for python 3.7</br>
2. Install h5pyd: `pip install h5pyd`
3.  Run: `hsconfigure` and set: 
       - hs endpoint: e.g. <http://EXTERNAL-IP>)
       - admin username/password (added to passwd.txt earlier)
       - Ignore API Key
4.  Run: `hsinfo`.  <br>Server state should be "`READY`".  Ignore the "Not Found" error for the admin home folder</br>
5.  Create "/home" folder: <br>`$ hstouch /home/`.  Note: trailing slash is important!</br>
6.  For each username in the passwd file, create a top-level domain: `hstouch -u <username> -p <passwd> /home/<username>/test/`
7.  Run the integration test: `python testall.py --skip_unit`
8.  Download the following file: `wget https://s3.amazonaws.com/hdfgroup/data/hdf5test/tall.h5`
9.  Import into hsds: <br>`hsload -v -u <username> -p <passwd> tall.h5 /home/<username>/test/`</br>
7. Verify upload: <br>`hsls -r -u test_user1 -p <passwd> /home/test_user1/test/tall.h5`</br>


<h2>AKS Cluster Scaling</h2>
    To scale up or down the number of HSDS pods, run: 
    <br><code>$kubectl scale --replicas=n deployment/hsds</code> where n is the number of pods desired.</br>


<h2>Notes for Installation from a Windows Machine</h2>

Follow the instructions above with the following modifications in the respective sections
1. Before you start make sure that you have docker installed on your system by running: `doker --version` otherwise install docker desktop: https://docs.docker.com/docker-for-windows/
2. Sample .bashrc will not work on Windows - instead run the following commands on the console (or include them in a batch file and run the batch file)
<pre><code><small>    SET AZURE_CONNECTION_STRING="1234567890"    
    SET BUCKET_NAME=hsds                        
    SET HDF5_SAMPLE_BUCKET=""
    SET RESOURCEGROUP=myresouregroup
    SET AKSCLUSTER=myakscluster
    SET LOCATION=westus
    SET ACRNAME=myacrname
    SET STORAGEACCTNAME=mystorageaccount
    SET CONTAINERNAME=testcontainer</small></code></pre>
    
      For commands in all sections replace the unix environment variable notation (SVAR) with Windows notation (%VAR%). 
      For example instead of $ACRNAME use %ACRNAME%


3. <font size="4">Setup your Azure environment</font><br>To install Azure cli on Windows, follow instructions here: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli-windows?view=azure-cli-latest<br>
4. <font size="4">Prepare and deploy your docker image to ACR</font><br>
   - On Windows downloaded files have CRLF instead of LF. This will cause the container to fail. To solve this:
     - Download do2unix from: https://sourceforge.net/projects/dos2unix/
     - Apply dos2unix to entrypoint.sh: `dos2unix entrypoint.sh`
   - build.sh will not run on Windows, instead run the docker build directly: `docker build -t ACRNAME.azurecr.io/hsds:v1 .' <br>**Note:** This will not run the pyflakes on the code. Pyflakes is a code checker and not essential to building the container.</br>



