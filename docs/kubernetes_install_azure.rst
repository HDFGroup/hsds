Installation with Azure Kubernetes
==================================

**Note:** These instructions assume you are using a Linux based system.
If you are using Windows please see the special notes at the end.

To begin, export environment variables as shown in “Sample .bashrc”
below.

These environment variables will be used to create Azure resources.

::

   export RESOURCEGROUP=myresouregroup                              # your Azure resource group name
   export AKSCLUSTER=myakscluster                                   # the name of the AKS cluster
   export LOCATION=westus                                           # the Azure region
   export ACRNAME=myacrname                                         # the name of the Azure Container Registry (ACR) you will be using
   export ADMIN_USER=admin                                          # The username for the HSDS admin acount
   export STORAGEACCTNAME=mystorageaccount                          # the storage account name for the Azure Blob Container
   export CONTAINERNAME=testcontainer                               # the name of the Azure Blob Container (default location HSDS will use)
   export AZURE_APP_ID=12345678-1234-1234-abcd-123456789ab          # if you will be using Azure Active Directory, set this to the application ID
   export AZURE_RESOURCE_ID=00000002-0000-0000-c000-000000000000    # if you will be using Azure Active Directory, set this to the resource ID
   # for the following, use the connection string for your Azure account. Note the quotation marks around the string
   export AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=myacct;AccountKey=GZJxxxOPnw==;EndpointSuffix=core.windows.net"

The environment variables AZURE_APP_ID and AZURE_RESOURCE_ID are
required if Azure Active Directory will be used for authentication. See
the “Azure Active Directory” section below for information and setting
up Active Directory for use with HSDS.

Prerequisites
-------------

Setup Pip and Python 3 on your local machine if not already installed
(e.g. with Miniconda https://docs.conda.io/en/latest/miniconda.html).

Clone the hsds repository in a local folder:
``git clone https://github.com/HDFGroup/hsds``.

Setup your Azure environment
----------------------------

Here we will deploy an Azure Storage Account, Azure Container Registry
(ACR) and Azure Kubernetes Service (AKS).

1.  Install az-cli: ``pip install azure-cli``
2.  Validate runtime version az-cli is at least 2.0.80: ``az version``
3.  Log in to Azure Subscription using AZ-Cli. ``az login``
4.  After successful login, the list of available subscriptions will be
    displayed. If you have access to more than one subscription, set the
    proper subscription to be used:
    ``az account set --subscription [name]``
5.  Run the following commands to create Azure Resource Group:
    ``az group create --name $RESOURCEGROUP --location $LOCATION``
6.  Create storage account:
    ``az storage account create -n $STORAGEACCTNAME -g $RESOURCEGROUP -l $LOCATION --sku Standard_LRS``
7.  Create a blob container in the storage account:
    ``az storage container create -n $CONTAINERNAME --account-name $STORAGEACCTNAME --fail-on-exist``
    Note: The connection string for the storage account can be found in
    the portal under Settings > Access keys on the storage account or
    via this cli command:
    ``az storage account show-connection-string -n $STORAGEACCTNAME -g $RESOURCEGROUP``
8.  The following command will create the new ACR:
    ``az acr create --resource-group $RESOURCEGROUP --name $ACRNAME --sku Basic --admin-enabled true``
9.  Install AKS cli: ``az aks install-cli``
10. Create AKS Cluster and attach to ACR:
    ``az aks create -n $AKSCLUSTER -g $RESOURCEGROUP --generate-ssh-keys --attach-acr $ACRNAME``
11. Get access to the AKS Cluster:
    ``az aks get-credentials -g $RESOURCEGROUP -n $AKSCLUSTER``

Create Kubernetes secrets
-------------------------

Kubernetes secrets are used in AKS to make sensitive information
available to the service. HSDS on AKS utilizes the following secrets:

1. user-password: username/password list
2. azure-conn-str: the AZURE_CONNECTION_STRING value
3. azure-ad-ids: AZURE_APP_ID and AZURE_RESOURCE_ID (optional)

HSDS accounts can either be set by creating the user-password secret, or
by using Azure Active Directory (AD). See `Azure Active Directory
Setup <azure_ad_setup.md>`__ for instructions on using AD.

To use user-password secret, first create a text file with the desired
usernames and passwords as follows:

1. Go to admin/config directory: ``cd hsds/admin/config``
2. Copy the file “passwd.default” to “passwd.txt”.
3. Add/change usernames/passwords that you want to use. **Note**: Do not
   keep the original example credentials.
4. Go back to the hsds root directory: ``cd ../..``

Next, verify that you have set the AZURE_CONNECTION_STRING environment
variable, and (if AD support is desired) the AZURE_APP_ID, and
AZURE_RESOURCE_ID.

Run the make_secrets script: ``./make_secrets.sh``

Run: ``kubectl get secrets`` to verify the secrets have been created.

Create RBAC role
----------------

If you anticipate running more than one HSDS pod, you will need to
cluster role bindings to allow pods to call the Kubernetes service to
list running pods. This is to enable HSDS pods to delegate operations to
other HSDS pods running in the same namespace and same app label.

1. Run: ``kubectl create -f k8s_rbac.yml`` Note: if you plan to run HSDS
   in its own Kubernetes namespace, modify the namespace key of
   ClusterRoleBinding in k8s_rbac.yml from “default” to your namespace.

Create Kubernetes service for HSDS
----------------------------------

1. Create HSDS service on the AKS cluster:
   ``$ kubectl apply -f k8s_service_lb_azure.yml``

2. This will create an external load balancer with an http endpoint with
   a public-ip. Use kubectl to get the public-ip of the hsds service:
   ``$ kubectl get service`` You should see an entry similar to:

   ::

      NAME    TYPE           CLUSTER-IP     EXTERNAL-IP      PORT(S)        AGE
      hsds    LoadBalancer   10.0.242.109   20.36.17.252     80:30326/TCP   23

   Note the public-ip (EXTERNAL-IP). This is where you can access the
   HSDS service externally. It may take some time for the EXTERNAL-IP to
   show up after the service deployment. For additional configuration
   options to handle SSL related scenarios please see: `Front Door
   Install <frontdoor_install_azure.md>`__ Additional reference for
   Azure Front Door https://docs.microsoft.com/en-us/azure/frontdoor/

Create Kubernetes ConfigMaps
----------------------------

Kubernetes ConfigMaps are used to store settings that are specific to
your HSDS deployment.

Review the contents of **admin/config/config.yml** and create the file
**admin/config/override.yml** for any keys where you don’t wish to use
the default value. Values that you will most certainly want to override
are:

-  bucket_name # set to the name of the Azure Blob container you will be
   using
-  password_file # if you created the user-password secret, set this to
   the mount path of the secret (“/accounts/passwd.txt” as specified in
   the k8s_deployment yamls)
-  hsds_endpoint # set to “http://” where EXTERNAL_IP is the IP address
   returned by ``$ kubectl get service``. If a DNS name will be mapped
   to this IP, that can be used instead. If HSDS will only be accessed
   within the Kubernetes cluster, you can use:
   http://hsds.default.svc.cluster.local:5101 instead. (use the
   namespace name instead of “default” if HSDS is being deployed to a
   Kubernetes namespace). The hsds_endpoint value is used to return a
   reference back to the service in REST HATEAOS responses.

Run the make_config map script to store the yaml settings as Kubernetes
ConnfigMaps: ``admin/kubernetes/k8s_make_configmap.sh``

Building HSDS image
-------------------

If you need to build and deploy a custom HSDS image (e.g. you have made
changes to the HSDS code), first build and deploy the code to ACR as
described in section “Building a docker image and deploying to ACR”
below. Otherwise, the standard image from docker hub
(https://hub.docker.com/repository/docker/hdfgroup/hsds) will be
deployed.

Deploy HSDS to AKS
==================

Now we are ready to create the HSDS deployment

1. In **k8s_deployment_azure.yml**, make any desired changes:

   -  image: ‘myacrname.azurecr.io/hsds:v1’ to reflect the acr
      repository for deployment (for custom builds only)
   -  resource memory limits: change if defaults are not satisfactory

2. Apply the deployment: ``$ kubectl apply -f k8s_deployment_azure.yml``
3. Verify that the HSDS pod is running: ``$ kubectl get pods`` a pod
   with a name starting with hsds should be displayed with status as
   “Running”.
4. Additional verification: Run (``$ kubectl describe pod hsds-xxxx``)
   and make sure everything looks OK
5. To locally test that HSDS functioning

   -  Create a forwarding port to the Kubernetes service
      ``$ sudo kubectl port-forward hsds-1234 8080:5101`` (use another
      port if 8080 is unavailable)
   -  From a browser hit: http://127.0.0.1:8080/about and verify that
      “cluster_state” is “READY”

6. If an external endpoint has been setup, try accessing HSDS through
   that endpoint

Test the Deployment using Integration Test and Test Data
--------------------------------------------------------

Perform post install configuration. See: `Post Install
Configuration <post_install.md>`__

AKS Cluster Scaling
-------------------

To scale up or down the number of HSDS pods, run:
``$kubectl scale --replicas=n deployment/hsds`` where n is the number of
pods desired.

Building a docker image and deploying to ACR
--------------------------------------------

This step is only needed if a custom image of HSDS needs to be deployed.

1. From hsds directory, build docker image: ``bash build.sh``
2. Tag the docker image using the ACR scheme:
   ``docker tag hdfgroup/hsds $ACRNAME.azurecr.io/hsds:v1`` where
   $ACRNAME is the ACR being deployed to, and v1 is the version (update
   this every time you will be deploying a new version of HSDS).
3. Login to the Azure container registry (ACR):
   ``az acr login --name $ACRNAME``
4. You may also need to login into ACR from docker as follows: Get the
   ACR admin credentials: ``az acr credential show -n $ACRNAME`` then
   docker login with those credentials:
   ``docker login $ACRNAME.azure.io -u xxx -p xxx``
5. Push the image to Azure ACR:
   ``docker push $ACRNAME.azurecr.io/hsds:v1`` **Note:** Use all
   lowercase ACRNAME in these commands if your actual ACRNAME includes
   uppercase characters
6. Update the **k8s_deployment_azure.yml** file to use the ACR image
   path (note there are multiple references to the image)

Notes for Installation from a Windows Machine
---------------------------------------------

Follow the instructions above with the following modifications in the
respective sections

1. Before you start make sure that you have docker installed on your
   system by running: ``docker --version`` otherwise install docker
   desktop: https://docs.docker.com/docker-for-windows/

2. Sample .bashrc will not work on Windows - instead run the following
   commands on the console (or include them in a batch file and run the
   batch file)

   ::

      SET AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=myacct;AccountKey=GZJxxxOPnw==;EndpointSuffix=core.windows.net"
      SET BUCKET_NAME=home
      SET RESOURCEGROUP=myresourcegroup
      SET AKSCLUSTER=myakscluster
      SET LOCATION=westus
      SET ACRNAME=myacrname
      SET STORAGEACCTNAME=mystorageaccount
      SET CONTAINERNAME=testcontainer

   For commands in all sections replace the unix environment variable
   notation (SVAR) with Windows notation (%VAR%). For example instead of
   ``$ACRNAME`` use ``%ACRNAME%``

3. Setup your Azure environment, to install Azure cli on Windows, follow
   instructions here:
   https://docs.microsoft.com/en-us/cli/azure/install-azure-cli-windows?view=azure-cli-latest

4. Prepare and deploy your docker image to ACR To create kubernetes
   secret:

   -  Enter the Azure connection string (just the string, not the set
      command) in a file named **az_conn_str** without double quotes (")
      or the end-of-line.
   -  Run ``kubectl create secret generic azure-conn-str --from-file=``
      **az_conn_str**
   -  Delete **az_conn_str**

   On Windows downloaded files have CRLF instead of LF. This will cause
   the container to fail. To solve this:

   -  Download do2unix from: https://sourceforge.net/projects/dos2unix/
   -  Apply dos2unix to entrypoint.sh: ``dos2unix entrypoint.sh``
   -  build.sh will not run on Windows, instead run the docker build
      directly: ``docker build -t ACRNAME.azurecr.io/hsds:v1 .``

   **Note:** This will not run the pyflakes on the code. Pyflakes is a
   code checker and not essential to building the container.
