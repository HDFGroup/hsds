Azure Active Directory
======================

Rather than user names and passwords being maintained by HSDS, Azure Active Directory can be used for authentication. To enable, in the Azure Portal, go to Azure Active Directory, select "App registrations" and
click the the plus sign, "New registration".  In the register page, chose an appropriate name for the application and select the desired "Supported account types".

In "API permissions", add permissions for "Microsoft Graph, openid", and "Microsoft Graph, User Read".

Next, click "Manifest", and copy the "appId" value and use it to set the AZURE_APP_ID environment variable.  Also on this page, copy the "resourceAppId" value, and use it to set the AZURE_RESOURCE_ID environment variable.

When these settings are used with a HSDS Docker or Kubernetes deployment, clients will be able to authenticate using their Active Directory username and password.

On the client machine(s), create a file ".hscfg" in your home folder with the following lines:

1. `hs_endpoint = <server_endpoint>`
2. `hs_ad_app_id = <AD Application (client) ID>`
3. `hs_ad_tenant_id = <AD tenant_id>`
4. `hs_ad_resource_id = <AD resource id>`

Test by running: `$hstouch /home/<username>/foo.h5`.  You will be prompted to enter a code to authenticate via Active Directory.
