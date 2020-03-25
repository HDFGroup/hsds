# HSDS Authentication

## Authentication Methods

### Password file authentication
By default, HSDS will load local usernames and passwords from a text file located at `admin/config/passwd.txt` when the container is built. The example password file contains three users:

```bash
# HSDS password file template
#
#
# This file contians a list of usernames/passwords that will be used to authenticate
# requests to HSDS.
# Copy file to "passwd.txt" in the same directory before building and deploying HSDS.
# For production use, replace the "test" password below with secret passwords and add
# and any new accounts desired.
# Text file get's copied to image at build time, so any changes require a new build/deployment.
admin:admin
test_user1:test
test_user2:test
```

Password file authentication can be combined with any of the following authentication methods (except for no authentication) by explicitly specifying the `PASSWORD_FILE` environment variable. To set the `PASSWORD_FILE` variable, the appropriate `docker-compose.*.yml` must be edited. Other possible authentication methods are given below in order of priority.

### Password-less authentication

Password-less authentication is enabled when the `PASSWORD_FILE` environment variable is explicitly set empty, `PASSWORD_FILE=""`. In this mode users are still required to provide a password in the HTTP basic authentication header, but it is not checked and permissions are granted based solely on the username provided.

### AWS DynamoDB authentication

Users are authenticated against an AWS DynamoDB table when the environment variables `AWS_DYNAMODB_GATEWAY` and `AWS_DYNAMODB_USERS_TABLE` are set appropriately.

### Salted password authentication

In salted password authentication, a master password is provided at runtime via the `PASSWORD_SALT` environment variable. If given, passwords are calculated for each user based on the combination of the username with the salt. The password for each username is computed by first concatenating the username with `PASSWORD_SALT`, then taking the first 32 characters of the SHA512 hash. For example, if `PASSWORD_SALT=salt`, then the password for `admin` is `3c4a79782143337be4492be072abcfe9`.

### Kerberos authentication

In Kerberos authentication, usernames and passwords are authenticated against a remote Kerberos domain controller. Currently, single-sign on is not supported and passwords must be explicitly sent with each request. To use Kerberos authentication, a valid `krb5.conf` file must be present at `admin/config/krb5.conf` when the container is built. Additionally, the environment variable `KRB5_REALM` must be set at runtime to the Kerberos realm against which to authenticate, for example `KRB5_REALM=HDFGROUP.ORG`.

Note, the current implementation does not validate the authenticity of the Kerberos domain controller itself, and should only be used on trusted networks.

## Credential caching

By default, credentials are cached indefinitely to improve performance. However some authentication methods such as DynamoDB and Kerberos support dynamic passwords that can be changed outside of HSDS. In this case, the cache time can be changed by setting the `AUTH_EXPIRATION` environment variable to the number of seconds to store credentials for. For example, `AUTH_EXPIRATION=60` to cache credentials for one minute.
