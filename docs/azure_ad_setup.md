Azure Active Directory
======================

Rather than user names and passwords being maintained by HSDS, Azure Active Directory can be used for authentication. In this mode, the client will be prompted to go to https://login.microsoftonline.com/ to enter a given pass code, and then sign in to his or her 
AD account.  The client will receive a token that it then passes to the server as an HTTP request with a "Bearer" authorization header.  From this token the server can validate the token and determine the username.  The token is valid for a specific period (typically an hour), so 
the client will not need to go to login.microsoft.com on each request.

Once a client request is authenticated, the requested action still needs to be authorized by the server based on the ACLs for the given folder or domain.  For example, for user *joebob@acme.com* to create a domain /home/joebob/foo.h5, the folder /home/joebob/ will need to provide write permissions for *joebob@acme.com*.  This can be done using the h5pyd hsacl tool.  For example: `$ hsacl /home/joebob/ +crudep joebob@acme.com`.

Active Directory authtication can be used in combination with accounts managed by the server.  If HTTP Basic Auth is used in the client request, the username and password will be validated against the local account.  If HTTP Bearer token is used, the request will be authenticated using Active Directory.

The following sections describe how to setup Active Directory, HSDS, and the client to use AD authentication.

Active Directory Server Configuration
-------------------------------------

In the Azure Portal, go to Azure Active Directory, select "App registrations" and
click the the plus sign: "New registration" that will be used by the HSDS service.  In the register page, chose an appropriate name for the application and select the desired "Supported account types".

In "API permissions", add the following permissions:
 
In "Expose an API", enter an "Application ID URI"  (e.g. "api;://hsds_server")

Also in "Expose an API", add a scope with "Who can consent?" as "Admins only".

In the overview section, note the "Application (client) ID" value, and the "Directory (tenant) ID" value.  You'll need these for HSDS and client configuration steps (see below).

HSDS Configuration
------------------

In the hsds/admin/config directory, create the file "override.yml" if it doesn't already exist.

In the override.yml file, create the following two lines:

    azure_resource_id: 12345678-1234-1234-abcd-123456789ab          # client id value for AD server application

If you would like to use a AD username as the server administrative account instead of "admin", add the following
to override.yml:

    admin_user: <admin_username>   # user who will have admin privileges.

The admin_user override is required if using AD authentication exclusively.

Save the file and then stop and start the server for the configuration changes to take effect.

Active Directory Client Configuration
-------------------------------------

In the Azure Portal, go to Azure Active Directory, select "App registrations" and
click the the plus sign, "New registration" that will be used by the HSDS clients.  In the register page, chose an appropriate name for the application and select the desired "Supported account types".

In "API permissions", add the following permission:

* for "APIs my organization users", select the HSDS server application.  Choose "Delegated permissions" and add permissions for the HSDS scope

Under "Authentication", choose "https://login.microsoftonline.com/common/oauth2/nativeclient" for "Redirect URIs".

Also under "Authenticaton", toggle "Yes" for "Treat application as a public client"

In the overview section, note the "Application (client) ID" value, and the "Directory (tenant) ID" value.  You'll need these for the client configuration steps (see below).

Client Configuration
--------------------

On each client machine(s), create a file ".hscfg" in the user's home folder with the following lines:

1. `hs_endpoint = <server_endpoint>`
2. `hs_ad_app_id = <AD HSDS Client Application (client) ID>`
3. `hs_ad_tenant_id = <AD tenant_id>`
4. `hs_ad_resource_id = <AD HSDS Server Application (client) id>`

Test by running: `$hstouch /home/<username>/foo.h5` where `/home/<username>/` has the approriate ACL as explained in the introduction.
You will be prompted to enter a code to authenticate via Active Directory.

The token information will be saved to a file ".hstokencfg" and the clients can use this data to avoid having to prompt the user to 
signin with each request.

Unattended Authentication
-------------------------

For applications that need to run without human intervention, perform the following steps:

1. In the Azure Portal, go to Azure Active Directory, and select the App Registration that was created in the "Active Directory Client Configuration" section above
2. Under "Certificates and Secrets", create a new client secret.  Copy and save the secret in a secure location as it will only be displayed this one time
3. Add permissions for any HSDS folder or domains the unattended application will need access to using the hsacl tool.  For example, if an application will be creating domains in the folder: "/home/joebob/mynightlyrun/", run: `hsacl /home/joebob/mynightlyrun/ +crue <client_id>`, where client_id is the hs_ad_app_id from the .hscfg file
4. In your ".hscfg" file, add the following line: `hs_ad_client_secret = <the secret>`
5. Clients will now be able to authenticate with server without any prompt using the client id as the username

Note: Rather than modifying the .hscfg file, you can use environment variables instead.  For example, for the client secret, use the following command: `export HS_AD_CLIENT_SECRET=my_secret`.
