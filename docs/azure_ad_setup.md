Azure Active Directory
======================

Rather than user names and passwords being maintained by HSDS, Azure Active Directory can be used for authentication. In this mode, the client will be prompted to go to https://login.microsoftonline.com/ to enter a given pass code, and then sign in to his or her 
AD account.  The client will receive a token that it then passes to the server as an HTTP request with a "Bearer" authorization header.  From this token the server can validate the token and determine the username.  The token is valid for a specific period (typically an hour), so 
the client will not need to go to login.microsoft.com on each request.

Once a client request is authenticated, the requested action still needs to be authorized by the server based on the ACLs for the given folder or domain.  For example, for user *joebob@acme.com* to create a domain /home/joebob/foo.h5, the folder /home/joebob/ will need to provide write permissions for *joebob@acme.com*.  This can be done using the h5pyd hsacl tool.  For example: `$ hsacl /home/joebob/ +crudep joebob@acme.com`.

Active Directory authtication can be used in combination with accounts managed by the server.  If HTTP Basic Auth is used in the client request, the username and password will be validated against the local account.  If HTTP Bearer token is used, the request will be authenticated using Active Directory.

The following sections describe how to setup Active Directory, HSDS, and the client to use AD authentication.

Active Directory Configuration
------------------------------

In the Azure Portal, go to Azure Active Directory, select "App registrations" and
click the the plus sign, "New registration".  In the register page, chose an appropriate name for the application and select the desired "Supported account types".

In "API permissions", add permissions for "Microsoft Graph, openid", and "Microsoft Graph, User Read".

Under "Authentication", choose "https://login.microsoftonline.com/common/oauth2/nativeclient" for "Redirect URIs".

In the overview section, note the "Application (client) ID" value, the "Directory (tenant) ID" value, and udner "Manifest" the "resourceAppId" value.  You'll need these for HSDS and client configuration steps (see below).

HSDS Configuration
------------------

In the hsds/admin/config directory, create the file "override.yml" if it doesn't already exist.

In the override.yml file, create the following two lines:

    azure_app_id: 12345678-1234-1234-abcd-123456789ab          # App ID value for your AD application
    azure_resource_id: 00000002-0000-0000-c000-000000000000    # Resource ID for your AD application

If you would like to use a AD username as the server administrative account instead of "admin", add the following
to override.yml:

    admin_user: <admin_username>   # user who will have admin privileges.

The admin_user override is required if using AD authentication exclusively.

Save the file and then stop and start the server for the configuration changes to take effect.

Client Configuration
--------------------

On each client machine(s), create a file ".hscfg" in the user's home folder with the following lines:

1. `hs_endpoint = <server_endpoint>`
2. `hs_ad_app_id = <AD Application (client) ID>`
3. `hs_ad_tenant_id = <AD tenant_id>`
4. `hs_ad_resource_id = <AD resource id>`

Test by running: `$hstouch /home/<username>/foo.h5` where `/home/<username>/` has the approriate ACL as explained in the introduction.
You will be prompted to enter a code to authenticate via Active Directory.

The token information will be saved to a file ".hstokencfg" and the clients can use this data to avoid having to prompt the user to 
signin with each request.
