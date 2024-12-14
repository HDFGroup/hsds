Keycloak Authentication
=======================

Rather than user names and passwords being maintained by HSDS, Keycloak ("https://www.keycloak.org/") can be used for authentication. In this mode, the client will fetch atoken from the Keycloak service that it then passes to the server as an HTTP request with a "Bearer" authorization header.  From this token the server can validate the token and determine the username.  The token is valid for a specific period (typically an hour), so 
the client will not need to fetch a new token on each request.

Once a client request is authenticated, the requested action still needs to be authorized by the server based on the ACLs for the given folder or domain.  For example, for user *joebob@acme.com* to create a domain /home/joebob/foo.h5, the folder /home/joebob/ will need to provide write permissions for *joebob@acme.com*.  This can be done using the h5pyd hsacl tool.  For example: `$ hsacl /home/joebob/ +crudep joebob@acme.com`.

Keycloak authentication can be used in combination with accounts managed by the server.  If HTTP Basic Auth is used in the client request, the username and password will be validated against the local account (if any have been setup).  If HTTP Bearer token is used, the request will be authenticated be decoding the token.
 
HSDS Configuration
------------------

In the hsds/admin/config directory, create the file "override.yml" if it doesn't already exist.

In the override.yml file, create the following lines.  Replace "server_dns" and "server_port" with 
the DNS name and port for the Keycloak server.  Replace "keycloak_realm" with the Realm being used 
in Keycloak.

    openid_provider: keycloak  # Use "keycloak" as the authentication provider
    openid_url: http://<server_dns>:<server_port>/realms/<keycloak_realm>/.well-known/openid-configuration   # update to use your Keycloak location and realm
    openid_audience: account # OpenID audience.  Keycloak client id.
    openid_claims: preferred_username,appid   # Comma seperated list of claims to resolve to usernames.

If you would like to use a Keycloak username as the server administrative account instead of "admin", add the following
to override.yml:

    admin_user: <admin_username>   # user who will have admin privileges.

The admin_user override is required if using Keycloak authentication exclusively.

Save the file and then stop and start the server for the configuration changes to take effect.

If you are deploying on Kubernetes or OpenShift you'll need to delete and recreate the configmap objects (see the HSDS kubernetes install guide for your platform).

Keycloak Client Configuration
------------------------------

In the Keycloak Admin console, choose the realm being used with HSDS, the select the "Clients" tab.
Click the the "create" button and enter a name for the client to be used with HSDS.  

On each client machine(s), create a file ".hscfg" in the user's home folder with the following lines (updated for your particular configuration):

    hs_endpoint = http://hsds.hdf.test                 # endpoint for HSDS
    hs_username = my_username                          # username 
    hs_password = my_password                          # password
    hs_keycloak_uri = http://keycloak.acme.org:8080    # endpoint for Keycloak
    hs_keycloak_client_id = myclient                   # client id
    hs_keycloak_realm = myrealm                        # realm on Keycloak
    hs_api_key =                                       # leave this empty
 
Test by running: `$hstouch /home/<username>/foo.h5` where `/home/<username>/` has the approriate ACL as explained in the introduction.
You will be prompted to enter a code to authenticate via Active Directory.

Note: Rather than modifying the .hscfg file, you can use environment variables instead.  For example, for the client id, use the following command: `export HS_KEYCLOAK_CLIENT_ID=my_client`.
