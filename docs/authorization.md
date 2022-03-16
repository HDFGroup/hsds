Authorization and Authentication
================================

Request Authentication
-----------------------
The HSDS REST supports HTTP Basic authentication to authenticate users by comparing an encrypted 
username and password against a value stored within a password file.  Alternatively, Bearer authentication
can be used where a security token is issued by a provider such as Active Directory.  In either case, once the 
request is authenticated a requestor username will be determine (unless anonymous request are supported, in which
case the authorization will follow as below for the "Default" case).  After authentication, the request will
then be *Authorized* based on the request action (creation, update, delete, etc.) and the Access Control Lists (ACLs)
of the domain the request is acting on.

Access Control Lists
--------------------

Once a request is authenticated (i.e. the requesting user is verified) it is then authorized (i.e. determine if the user is allowed to perform a specific action, say deleting a domain).  To do this Access Control Lists (ACLs) are associated with each domain or folder and they determine which actions are permitted for each user (or user group). 

Each ACL consists of 1 or more items in the form:

(username, read, create, update, delete, readACL, updateACL)

where username is a string, and read, create, update, delete, readACL, updateACL are booleans.
There flags have the following semantics when the given username is provided in the http
Authorization header:

* read: The given user is authorized for read access to the resource (generally all GET requests)
* create: The given user is authorized to create new resources (generally POST or PUT requests)
* update: The given user is authorized to modify a resource (e.g. PUT /datasets/&lt;id&gt;/value)
* delete: The given user is authorized to delete a resource (e.g. DELETE /groups/&lt;id&gt;)
* readACL: The given user is authorized to read the ACLs of a resource (excepting that users are always allowed to read their own ACL, if defined)
* updateACL: The given user is authorized to modify the ACLs of a resource

A special username 'default' is used to denote the access permission for all other users who
are not otherwise listed in the ACL.  Anonymous requests (requests that don't have an authorization header) will also use the default ACL (the "allow_noauth" config can be set to "False" to disable anonymous requests).

In addition requests with the username "admin" (or whatever the "admin_user" config is set to), can 
perform any action regardless of the ACLs used in the domain or folder.

Example
-------

Suppose a given domain has the following ACLs:

    ========   ====  ======   ======  ======  =======  ========
    username   read  create   update  delete  readACL  writeACL
    ========   ====  ======   ======  ======  =======  ========
    default    true  false    false   false   false    false
    joe        true  false    true    false   false    false
    ann        true  true     true    true    true     true
    ========   ====  ======   ======  ======  =======  ========

The ACL with username "default" would enable anyone to read (perform GET requests) from the domain. 
The ACL with username "joe" would enable requests with username "joe" 
to read and update the domain (e.g. modify values in a dataset).  While user 'ann' would have full 
control to do any operation on the domain (including modifying permissions for herself or
other users) based on the ACL with username "ann".

If anonymous requests are supported ("allow_noauth" config value is True), the permissions would be
the same as for authenticated users who are not "joe" or "ann".  

For example, the following unauthenticated (no HTTP Authorization header) 
requests on the domain would be granted or denied as follows

* GET /datasets/&lt;id&gt; - granted (returns HTTP Status 200 - OK)
* POST /datasets/&lt;id&gt;/value - granted (returns HTTP Status 200 - OK)
* PUT /datasets/&lt;id&gt;/shape) - denied (returns HTTP Status 401 - Unauthorized)
* PUT /datasets/&lt;id&gt;/attributes/&lt;name&gt; - denied (returns HTTP Status 401 - Unauthorized)
* DELETE /datasets/&lt;id&gt;  - denied (returns HTTP Status 401 - Unauthorized)

The same response would be returned for an authenticated request where the user is neither 'joe' or 'ann'.

Next the same set of requests are sent with 'joe' as the user in the HTTP Authorization header:

* GET /datasets/&lt;id&gt; - granted (returns HTTP Status 200 - OK)
* POST /datasets/&lt;id&gt;/value - granted (returns HTTP Status 200 - OK)
* PUT /datasets/&lt;id&gt;/shape) - grant (returns HTTP Status 200 - OK)
* PUT /datasets/&lt;id&gt;/attributes/&lt;name&gt; - denied (returns HTTP Status 403 - Forbidden)
* DELETE /datasets/&lt;id&gt; - denied (returns HTTP Status 403 - Forbidden)

Finally the same set of requests are sent with 'ann' as the user:

* GET /datasets/&lt;id&gt; - granted (returns HTTP Status 200 - OK)
* POST /datasets/&lt;id&gt;/value - granted (returns HTTP Status 200 - OK)
* PUT /datasets/&lt;id&gt;/shape) - grant (returns HTTP Status 200 - OK)
* PUT /datasets/&lt;id&gt;/attributes/&lt;name&gt; - denied (returns HTTP Status 201 - Created)
* DELETE /datasets/&lt;id&gt;  - denied (returns HTTP Status 200 - OK)
 
Note: HTTP Status 401 basically says: "you can't have access until you tell me who your are", 
while HTTP Status 403 says: "I know who you are, but you don't have permissions to access this
resource."

Creating and Managing ACLs
--------------------------

New ACLs can be created (or existing ACLs modified) by using the PUT /acls/&lt;username&gt; request.

Modifications to ACLs require that the requester have the writeACL permission
(or the request come from an admin user).

To retrieve the ACL for a given user, use the GET /acls/&lt;username&gt; request (requires the readACL permission),
or to retrieve all the ACLs for a given domain or folder, use GET /acls.

To modify ACLs from the command line, the h5pyd package includes a utility "hsacl" for reading and creating ACLs.  Run
"hsacl --help" for usage information.

Group ACLs and RBAC
-------------------

Rather than maintain a large number of ACLs for a set of related users (say, "developers"), you create ACLs that apply to
groups of users; this is known as "Role Based Access Control" or RBAC.

To specify an RBAC that applies to a group of users, use the string: "g:&lt;groupname&gt;" rather than "&lt;username&gt;" in the PUT /acls request.  The &lt;groupname&gt;
refers to a valid group name defined in hsds/admin/config/groups.txt.  Refer to to the install instructions for your platform for information on creating the user groups.

By using group ACLs, you can control access to a given domain or folder based on "roles" rather than individually managing user ACLs.  As member of a group change, the user's ability to perform a given action will change based on his or her membership in the group without requiring any change in the ACLs.

If a group ACL is defined for a given domain or folder, and a requesting user is a member of the group (and no user ACL for that user exists), then the group ACL's permissions will be used to determine if the given action is authorized.  If multiple group ACLs are defined, then the action is permitted if any of the ACLs for which the user is member authorize the action.

The overall flow for validation is as follows:

1. If the request is from an admin user, the request is authorized
2. If an ACL for a given user exists, then the action is authorized or denied (403 error) based on the permissions of that ACL
3. Otherwise, if there is a group ACL for which the user is a member that authorizes an action, the action is authorized
4. If the action is not otherwise authorized, and there is a "default" ACL that authorizes the action, the action is authorized
5. If none of the above hold, the action is denied

Group ACL Example
-----------------

Suppose a given domain has the following ACLs:

    ========   ====  ======   ======  ======  =======  ========
    username   read  create   update  delete  readACL  writeACL
    ========   ====  ======   ======  ======  =======  ========
    default    true  false    false   false   false    false
    g:devs     true  false    true    false   false    false
    ann        true  true     true    true    true     true
    ========   ====  ======   ======  ======  =======  ========

and that 'ann' and 'joe' are both members of the 'devs' group.

The following requests on the domain sent with 'joe' as the user would be authorized or denied as follows:

* GET /datasets/&lt;id&gt; - granted (returns HTTP Status 200 - OK)
* POST /datasets/&lt;id&gt;/value - granted (returns HTTP Status 200 - OK)
* PUT /datasets/&lt;id&gt;/shape) - grant (returns HTTP Status 200 - OK)
* PUT /datasets/&lt;id&gt;/attributes/&lt;name&gt; - denied (returns HTTP Status 403 - Forbidden)
* DELETE /datasets/&lt;id&gt;  - denied (returns HTTP Status 403 - Forbidden)

The same requests with 'ann' as the requestor would return HTTP Status 200 - OK, since the 'ann' ACL overrides any 
settings in the 'default' or 'g:devs' ACL.  

Finally, any requests that come from a user other than 'ann' or 'bob' would be authorized or denied based on 
the values of the 'default' ACL (in this case, only read requests or authorized).

ACL Inheritance
---------------

When a new domain or folder is created, a user ACL will be automatically created that gives the requesting user full control
over that resource.  Additionally, any other ACLs (user, group, or default) defined in the parent folder will be copied to the ACLs of the new resource (unless that ACL does not authorize any action).
 
 
