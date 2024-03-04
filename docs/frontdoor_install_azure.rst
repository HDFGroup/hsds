.. raw:: html

   <h1>

Use Azure Front Door for SSL/https with HSDS on AKS

.. raw:: html

   </h1>

With the instructions in *kubernetes_install_azure.md*, you can deploy
hsds to Azure Kubernetes Service (AKS) and access it over http using the
EXTERNAL-IP of the load balancer. We would ideally like to access the
service over https especially given that HSDS currently uses simple auth
for authentication.

While there are various tools to provide SSL termination, Azure Front
Door provides an easy and simple way to achieve this. This document
provides the details of adding Azure Front Door (FD) to HSDS created
previously. It uses the default domain (azurefd.net) that can be created
using FD. To use a custom domain, please refer to the following
documentation:
https://docs.microsoft.com/en-us/azure/frontdoor/front-door-custom-domain

There are two methods for deploying Azure Front Door, oulined below.

.. raw:: html

   <h2>

Installation with Azure Portal

.. raw:: html

   </h2>

1.  As was described in the installation document
    *kubernets_install_azure.md*, use ``$kubectl get svc`` to get and
    save the public-ip of the service load balancer - we will need it in
    a later step.
2.  On Azure portal select \`+ Create a resource\` and type ‘Front
    Door’.
3.  Select ‘Front Door’ and then ‘Create’
4.  Select the appropriate ‘Subscription’ and ‘Resource group’ on the
    ‘Basics’ Tab
5.  Select the ‘Configuration’ Tab |alt text1| All the 3 areas: Frontend
    hosts, Backend pools and Routing rules will be empty
6.  Select to add (+) Frontend hosts \ |alt text2| add a valid hostname
    and select ‘Add’Select ‘Update’
7.  Now select add (+) Backend pools \ |alt text3| Select ‘Custom host’
    for ‘Backend host type’ and in the ‘Backend host name’ field enter
    the public-ip for the load balancer from Step 1 above Select
    ‘Update’
8.  Now select add (+) routing rules \ |alt text4| Select ‘Accepted
    protocol’ and ‘Forwarding protocol’ as shown here. For ‘Frontend
    hosts’ and ‘Backend pool’ select the entries created in the previous
    stepsSelect ‘Update’
9.  Now select ‘Review + Create’ and then ‘Create’
10. Once the deployment is successful, you can test the HSDS service as:
    http://.azurefd.net/about https://.azurefd.net/about The endpoints
    may take a few minutes to become available after the Front Door
    deployment is complete with the https endpoint taking longer

.. raw:: html

   <h2>

Installation with Azure CLI

.. raw:: html

   </h2>

These environment variables will be used to configure Front Door.

.. raw:: html

   <pre><code><small>
   export RESOURCEGROUP=myresourcegroup
   export LOCATION=westus
   export FRONTDOORNAME=''
   export BACKENDADDRESS='' # use $kubectl get svc to get the public-ip of the service load balancer
   export FRIENDLYNAME=''
   export PROTOCOL='http'
   export ACCEPTED_PROTOCOLS='Https'
   export FWDING_PROTOCOLS='HttpsOnly'
   </small></code></pre>

1. Install pip

   -  ``$sudo apt-get update && sudo apt-get -y upgrade``
   -  ``$sudo apt-get install python3-pip``

2. If not already installed, install AZ-Cli:
   ``curl -L https://aka.ms/InstallAzureCli | bash``
3. Install the front door AZ-Cli extension:
   ``az extension add --name front-door``
4. Login to Azure Subscription using AZ-Cli. ``$az login``
5. After successful login, the list of available subscriptions will be
   displayed. If you have access to more than one subscription, set the
   proper subscription to be used:
   ``az account set --subscription [name]``
6. The following command will create a new Front Door instance with SSL
   Offloading:
   ``az network front-door create --resource-group $RESOURCEGROUP --name $FRONTDOORNAME --backend-address $BACKENDADDRESS --friendly-name $FRIENDLYNAME --protocol $PROTOCOL --accepted-protocols $ACCEPTED_PROTOCOLS --forwarding-protocol $FWDING_PROTOCOLS``
7. Once the deployment is successful, you can test the HSDS service as:
   http://<:math:`FRIENDLYNAME>.azurefd.net/about  <br/>https://<`\ FRIENDLYNAME>.azurefd.net/about
   The endpoints may take a few minutes to become available after the
   Front Door deployment is complete with the https endpoint taking
   longer

.. |alt text1| image:: ./img/front_door1.jpg
.. |alt text2| image:: ./img/front_door2.jpg
.. |alt text3| image:: ./img/front_door3.jpg
.. |alt text4| image:: ./img/front_door4.jpg
