<h1>Use Azure Front Door for SSL/https with HSDS on AKS</h1>


With the instructions in *kubernets_install_azure.md*, you can deploy hsds to Azure Kubernetes Service (AKS) and access it over http using the EXTERNAL-IP of the load balancer. We would ideally like to access the service over https especially given that HSDS currently uses simple auth for authentication.

While there are various tools to provide SSL termination, Azure Front Door provides an easy and simple way to achieve this. This document provides the details of adding Azure Front Door (FD) to HSDS created previously. It uses the default domain (azurefd.net) that can be created using FD. To use a custom domain, please refer to the following documentation:
https://docs.microsoft.com/en-us/azure/frontdoor/front-door-custom-domain

There are two methods for deploying Azure Front Door, oulined below.

<h2>Installation with Azure Portal</h2>

1. As was described in the installation document *kubernets_install_azure.md*, use `$kubectl get svc` to get and save the public-ip of the service load balancer - we will need it in a later step.
2. On Azure portal select `+ Create a resource` and type 'Front Door'.
3. Select 'Front Door' and then 'Create'
4. Select the appropriate 'Subscription' and 'Resource group' on the 'Basics' Tab<br> </br>
5. Select the 'Configuration' Tab
   ![alt text](./img/front_door1.jpg "Front Door")
   All the 3 areas: Frontend hosts, Backend pools and Routing rules will be empty<br> </br>
6. Select to add (+) Frontend hosts
    <br>![alt text](./img/front_door2.jpg "Frontend hosts")
    <br>add a valid hostname and select 'Add'<br>Select 'Update' </br><br>
7. Now select add (+) Backend pools
   <br>![alt text](./img/front_door3.jpg "Backend pools")
   <br>Select 'Custom host' for 'Backend host type' and in the 'Backend host name' field enter the public-ip for the load balancer from Step 1 above <br>Select 'Update'<br></br>
8. Now selct add (+) routing rules
   <br>![alt text](./img/front_door4.jpg "Routing rules")
   Select 'Accepted protocol' and 'Forwarding protocol' as shown here.
   <br>For 'Frontend hosts' and 'Backend pool' select the entries created in the previous steps</br>Select 'Update'<br></br>
9. Now select 'Review + Create' and then 'Create'
10. Once the deployment is successful, you can test the HSDS service as:
    <br>http://<frontend_hostname>.azurefd.net/about
    <br>https://<frontend_hostname>.azurefd.net/about
    <br>The endpoints may take a few minutes to become available after the Front Door deployment is complete with the https endpoint taking longer


<h2>Installation with Azure CLI</h2>

These environment variables will be used to configure Front Door.
<pre><code><small>
export RESOURCEGROUP=myresouregroup
export LOCATION=westus
export FRONTDOORNAME=''
export BACKENDADDRESS='' # use $kubectl get svc to get the public-ip of the service load balancer
export FRIENDLYNAME=''
export PROTOCOL='http'
export ACCEPTED_PROTOCOLS='Https'
export FWDING_PROTOCOLS='HttpsOnly'
</small></code></pre>

1.  If not already installed, install AZ-Cli:<br/> `curl -L https://aka.ms/InstallAzureCli | bash`
2.  Install the front door AZ-Cli extension: <br/> `az extension add --name front-door`
3. Login to Azure Subscription using AZ-Cli. `$az login`
4. After successful login, the list of available subscriptions will be displayed. If you have access to more than one subscription, set the proper subscription to be used: `az account set --subscription [name]`
5. The following command will create a new Front Door instance with SSL Offloading:<br/> `az network front-door create --resource-group $RESOURCEGROUP --name $FRONTDOORNAME --backend-address $BACKENDADDRESS --friendly-name $FRIENDLYNAME --protocol $PROTOCOL --accepted-protocols $ACCEPTED_PROTOCOLS --forwarding-protocol $FWDING_PROTOCOLS`
6. Once the deployment is successful, you can test the HSDS service as:
    <br/>http://<$FRIENDLYNAME>.azurefd.net/about
    <br/>https://<$FRIENDLYNAME>.azurefd.net/about
    <br/>The endpoints may take a few minutes to become available after the Front Door deployment is complete with the https endpoint taking longer