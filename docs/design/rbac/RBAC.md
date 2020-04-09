# Role Based Access Control (RBAC) Design



# Introduction:

The purpose of this feature is to enable permissions to perform operations on HSDS domains via a set of *roles*.  Roles are groups of users that can be used in setting permissions.  Currently authorization in HSDS is based on *ACLs*.  An ACL defines a username and a set of permissions (read, write, update, delete, etc).  Each domain in HSDS has one or more ACLs.  When a request is received by the service, the authenticated username associatted with the request is used to find the ACL with that username, and authority to perform the operations is determined by the set of permissions in the ACL.

This works well if the number of users is limited, but becomes unwieldy if large number of users need to be managed.  By using roles, ACLs can be defined as role + permissions, providing a better means of access management.



## Requirements

- Allow roles to defined either by a *roles.txt* file (similar to *passwd.txt*) or using Active Directory groups"
- Update schema to persist ACLs with group identifiers
- Support REST /acl operations with groups
- Update authorization logic
- Update h5acl tool for managing domain ACLs with groups



## Role Definitions

Roles will be set either by a groups.txt file that is loaded at server startup or (if Active Directory is configured) using Active Directory  groups.

For roles.txt, the format will consist of one line per role as follows:

```
role_name: username_1, username_2, ...
```

On startup the groups.txt file will be loaded by each SN container and changes to the file after that will not take effect until the service is restarted.

Alternatively if AD is used, role definations will be queried from AD groups as needed.  An expiration will be used (similar to how user tokens are managed now) that will refresh the group definitions periodically.

## S3 Schema Changes

Currently in the schema, ACLs are stored as part of the domain JSON as a dictionary of username to permissions.  E.g.:

```
"acls": {
   "test_user1": {"create": true, "read": true, "update": true, "delete": true, "readACL": true, "updateACL": true},
   "test_user2": {"create": false, "read": true, "update": false, "delete": false, "readACL": true, "updateACL": false}
   }
```

In this example, test_user1 has full control (can perform any action) of the domain, while test_user2 is only authorized to perform read and read ACL operations.

To support RBAC, this schema will be extended so that groups or usernames can be defined.  To distinguish groups from users a "u:" or "r:" prefix will be appended.

In the following example, the group "blue_org" has permissions to read or update the domain:

```
"acls": {
   "u:test_user1": {"create": true, "read": true, "update": true, "delete": true, "readACL": true, "updateACL": true},
   "u:test_user2": {"create": false, "read": true, "update": false, "delete": false, "readACL": true, "updateACL": false},
   "r:blue_org": {"create": false, "read": true, "update": true, "delete": false, "readACL": false, "updateACL": false}   
   }
```



##  Update REST /acl operation

The PUT /acl operation will allow role names to be used in addition to usernames.

The GET /acl operation will return user or role ACLs.  A parameter will be added that can optionally be used to restrict to eitehr user or role operations.

The GET /acls operation will return all ACLs.  A parameter will be added that can optionally be used to restrict to eitehr user or role operations.

## Update authorization logic

TBD



Update hsacl tool

TBD