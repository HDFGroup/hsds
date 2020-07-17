##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of HSDS (HDF5 Scalable Data Service), Libraries and      #
# Utilities.  The full HSDS copyright notice, including                      #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################
import time
import base64
import hashlib
import json
import binascii
import subprocess
import os.path as pp
import datetime
from botocore.exceptions import ClientError
from aiobotocore import get_session
from aiohttp.web_exceptions import HTTPBadRequest, HTTPUnauthorized, HTTPNotFound, HTTPForbidden, HTTPServiceUnavailable, HTTPInternalServerError
import jwt
from jwt.exceptions import InvalidAudienceError, InvalidSignatureError
import requests
from cryptography.x509 import load_pem_x509_certificate
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers 
from .. import hsds_logger as log
from .. import config

MSONLINE_OPENID_URL = "https://login.microsoftonline.com/common/.well-known/openid-configuration"
GOOGLE_OPENID_URL = "https://accounts.google.com/.well-known/openid-configuration"

GROUP_PREFIX = "g:"  # pre-appended to acl names to distinguish group acls from user acls

def getDynamoDBClient(app):
    """ Return dynamodb handle
    """
    if "session" not in app:
        session = get_session()
        app["session"] = session
    else:
        session = app["session"]

    if "dynamodb" in app:
        if "token_expiration" in app:
            # check that our token is not about to expire
            expiration = app["token_expiration"]
            now = datetime.datetime.now()
            delta = expiration - now
            if delta.total_seconds() > 10:
                return app["dynamodb"]
            # otherwise, fall through and get a new token
            log.info("DynamoDB access token has expired - renewing")
        else:
            return app["dynamodb"]

    # first time setup of s3 client or limited time token has expired
    aws_region = config.get("aws_region")
    aws_secret_access_key = None
    aws_access_key_id = None
    aws_session_token = None
    aws_iam_role = config.get("aws_iam_role")
    log.info(f"using iam role: {aws_iam_role}")
    aws_secret_access_key = config.get("aws_secret_access_key")
    aws_access_key_id = config.get("aws_access_key_id")
    if not aws_secret_access_key or aws_secret_access_key == 'xxx':
        log.info("aws secret access key not set")
        aws_secret_access_key = None
    if not aws_access_key_id or aws_access_key_id == 'xxx':
        log.info("aws access key id not set")
        aws_access_key_id = None

    if aws_iam_role and not aws_secret_access_key:
        # TODO - refactor with similar code in s3Util
        log.info("getted EC2 IAM role credentials")
        # Use EC2 IAM role to get credentials
        # See: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html?icmpid=docs_ec2_console
        curl_cmd = ["curl", f"http://169.254.169.254/latest/meta-data/iam/security-credentials/{aws_iam_role}"]
        p = subprocess.run(curl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if p.returncode != 0:
            msg = f"Error getting IAM role credentials: {p.stderr}"
            log.error(msg)
        else:
            stdout = p.stdout.decode("utf-8")
            try:
                cred = json.loads(stdout)
                aws_secret_access_key = cred["SecretAccessKey"]
                aws_access_key_id = cred["AccessKeyId"]
                log.info(f"Got ACCESS_KEY_ID: {aws_access_key_id} from EC2 metadata")
                aws_session_token = cred["Token"]
                log.info("Got Expiration of: {}".format(cred["Expiration"]))
                expiration_str = cred["Expiration"][:-1] + "UTC" # trim off 'Z' and add 'UTC'
                # save the expiration
                app["token_expiration"] = datetime.datetime.strptime(expiration_str, "%Y-%m-%dT%H:%M:%S%Z")
            except json.JSONDecodeError:
                msg = "Unexpected error decoding EC2 meta-data response"
                log.error(msg)
            except KeyError:
                msg = "Missing expected key from EC2 meta-data response"
                log.error(msg)

    dynamodb_gateway = config.get('aws_dynamodb_gateway')
    if not dynamodb_gateway:
        msg="Invalid aws dynamodb gateway"
        log.error(msg)
        raise ValueError(msg)
    use_ssl = False
    if dynamodb_gateway.startswith("https"):
        use_ssl = True
    dynamodb = session.create_client('dynamodb', region_name=aws_region,
                                   aws_secret_access_key=aws_secret_access_key,
                                   aws_access_key_id=aws_access_key_id,
                                   aws_session_token=aws_session_token,
                                   endpoint_url=dynamodb_gateway,
                                   use_ssl=use_ssl)

    app['dynamodb'] = dynamodb  # save so same client can be returned in subsiquent calls

    return dynamodb

def releaseDynamoDBClient(app):
    """ release the client collection to dynamoDB
     (Used for cleanup on application exit)
    """
    if 'dynamodb' in app:
        client = app['dynamodb']
        client.close()
        del app['dynamodb']

def loadPasswordFile(password_file):
    log.info(f"using password file: {password_file}")
    line_number = 0
    user_db = {}
    try:
        with open(password_file) as f:
            for line in f:
                line_number += 1
                s = line.strip()
                if not s:
                    continue
                if s[0] == '#':
                    # comment line
                    continue
                fields = s.split(':')
                if len(fields) < 2:
                    msg = f"line: {line_number} of {password_file} is not valid"
                    log.warn(msg)
                    continue
                username = fields[0]
                passwd = fields[1]
                if len(username) < 3 or len(passwd) < 3:
                    msg = f"line: {line_number} of {password_file} is not valid, username and password must be 3 characters are longer"
                    log.warn(msg)
                    continue
                if username in user_db:
                    msg = f"line: {line_number} of {password_file}, username is repated"
                    log.warn(msg)
                    continue
                user_db[username] = {"pwd": passwd}
                log.info(f"added user: {username}")
    except FileNotFoundError:
        log.error(f"unable to open password file: {password_file}")
    return user_db


def initUserDB(app):
    """
    Called at startup to initialize user/passwd dictionary from a password text file
    """
    log.info("initUserDB")
    if "user_db" in app:
        msg = "user_db already initilized"
        log.warn(msg)
        return

    if config.get("aws_dynamodb_gateway") and config.get("aws_dynamodb_users_table"):
        # user entries will be obtained dynamicaly
        log.info("Getting DynamoDB client")
        getDynamoDBClient(app)  # get client here so any errors will be seen right away
        user_db = {}
    elif config.get("password_salt"):
        # use salt key to verify passwords
        log.info("using PASSWORD_SALT")
        user_db = {}
    else:
        password_file = config.get("password_file")
        if not password_file or not pp.isfile(password_file) :
            log.info("No password file")
            user_db = {}
        else:
            log.info(f"Loading password file: {password_file}")
            user_db = loadPasswordFile(password_file)

    app["user_db"] = user_db

    log.info(f"user_db initialized: {len(user_db)} users")

def setPassword(app, username, password, **kwargs):
    """
    setPassword: sets a password and metadata.
    """
    log.info(f"Saving user/password to user_db for: {username}")
    if "user_db" not in app:
        log.info("initializing user_db")
        app["user_db"] = {}
    user_db = app["user_db"]
    user_data = dict(pwd=password, **kwargs)
    expiration = float(config.get("auth_expiration"))
    if "exp" not in user_data and expiration > 0:
        log.debug(f"setting expiration on credentials for user: {username}")
        user_data["exp"] = time.time() + expiration
    user_db[username] = user_data

def getPassword(app, username):
    """
    getPassword: gets a password and metadata if valid.
    """
    user_db = app["user_db"]
    if username in user_db:
        user_data = user_db[username]
        if "exp" in user_data and time.time() >= user_data["exp"]:
            log.debug(f"removing expired credentials for user: {username}")
            del user_db[username]
            return None
        return user_data
    return None

async def validateUserPasswordDynamoDB(app, username, password):
    """
    validateUserPassword: verify user and password.
        throws exception if not valid
    Note: make this async since we'll eventually need some sort of http request to validate user/passwords
    """
    if getPassword(app, username) is None:
        # look up name in dynamodb table
        dynamodb = getDynamoDBClient(app)
        table_name = config.get("aws_dynamodb_users_table")
        log.info(f"looking for user: {username} in DynamoDB table: {table_name}")
        try:
            response = await dynamodb.get_item(
                TableName=table_name,
                Key={'username': {'S': username}}
            )
        except ClientError as e:
            log.error("Unable to read dyanamodb table: {}".format(e.response['Error']['Message']))
            raise HTTPInternalServerError()  # 500
        if "Item" not in response:
            log.info(f"user: {username} not found")
            raise HTTPUnauthorized()  # 401
        item = response['Item']
        if "password" not in item:
            log.error("Expected to find password key in DynamoDB table")
            raise HTTPInternalServerError()  # 500
        password_item = item["password"]
        if 'S' not in password_item:
            log.error("Expected to find 'S' key for password item")
            raise HTTPInternalServerError()  # 500
        log.debug(f"password: {password_item}")
        if password_item['S'] != password:
            log.warn(f"user password is not valid for user: {username}")
            raise HTTPUnauthorized()  # 401
        # add user/password to user_db map
        setPassword(app, username, password)

def validatePasswordSHA512(app, username, password):
    if getPassword(app, username) is None:
        log.info(f"SHA512 check for username: {username}")
        salt = config.get("password_salt")
        hex_hash = hashlib.sha512(username.encode('utf-8') + salt.encode('utf-8')).hexdigest()
        if hex_hash[:32] != password:
            log.warn(f"user password is not valid (didn't equal sha512 hash) for user: {username}")
            raise HTTPUnauthorized()  # 401
        setPassword(app, username, password)


async def validateUserPassword(app, username, password):
    """
    validateUserPassword: verify user and password.
        throws exception if not valid
    Note: make this async since we'll eventually need some sort of http request to validate user/passwords
    """
    log.debug(f"validateUserPassword username: {username}")

    if not username:
        log.info('validateUserPassword - null user')
        raise HTTPUnauthorized()
    if not password:
        log.info('isPasswordValid - null password')
        raise HTTPUnauthorized()

    log.debug(f"looking up username: {username}")
    if "user_db" not in app:
        msg = "user_db not intialized"
        log.error(msg)
        raise HTTPInternalServerError()  # 500

    user_data = getPassword(app, username)

    if user_data is None:
        if "no_auth" in app and app["no_auth"]:
            log.info(f"no-auth access for user: {username}")
            setPassword(app, username, "")
        elif config.get("aws_dynamodb_users_table"):
            # look up in Dyanmo db - will throw exception if user not found
            await validateUserPasswordDynamoDB(app, username, password)
        elif config.get("password_salt"):
            validatePasswordSHA512(app, username, password)
        else:
            log.info("user not found")
            raise HTTPUnauthorized() # 401
        user_data = getPassword(app, username)

    if user_data['pwd'] == password:
        log.debug("user password validated")
    else:
        log.info(f"user password is not valid for user: {username}")
        raise HTTPUnauthorized() # 401

def _checkTokenCache(app, token):
    # iterate through use_db and return username if this token if found
    # (and it is still valid)
    # TBD: create reverse lookup table for efficiency
    if "user_db" not in app:
        return None
    user_db = app["user_db"]
    expired_ids = set() # might as well clean up any expired tokens we find
    user_id = None
    for username in user_db:
        user = user_db[username]
        if "scheme" not in user or user["scheme"] != "bearer":
            continue
        if "exp" not in user:
            log.warn(f"expected to find key: 'exp' in user data for user: {username}")
            continue
        exp = user["exp"]
        if "pwd" not in user:
            log.warn(f"expected to find key: 'pwd' in user data for user: {username}")
            continue
        if "pwd" not in user or user["pwd"] != token:
            continue
        pwd = user['pwd']

        if time.time() < exp:
            # still valid!
            if pwd == token:
                log.info(f"returning user: {username} from bearer cache")
                user_id = username
        else:
            # add to the expired set
            expired_ids.add(username)
    for username in expired_ids:
        log.debug(f"removing expired token userdb for user: {username}")
        del user_db[username]

    return user_id

def loadGroupsFile(group_file):
    """ 
    Parse the given file and return a dict where each key is a group name and the key value is a set of usernames.
    Expectedd format of the file is multiple lines with each line being: <group_name>: user1 user2 user3.
    Hashmakr '#' in the first column indicates comment and the line is ignored.
    """
    log.info(f"using group file: {group_file}")
    line_number = 0
    group_db = {}
    try:
        with open(group_file) as f:
            for line in f:
                line_number += 1
                s = line.strip()
                if not s:
                    continue
                log.debug(f"group[{line_number}]: {s}")
                if s[0] == '#':
                    # comment line
                    continue
                fields = s.split(':')
                if len(fields) < 2:
                    msg = f"line: {line_number} is not valid"
                    log.warn(msg)
                    continue
                group_name = fields[0]
                users = fields[1]
                user_names = users.split(' ')
                log.debug(f"group: {group_name} users: {user_names}")

                if group_name in group_db:
                    msg = f"line: {line_number} of {group_file} group name: {group_name} is repeated"
                    log.warn(msg)
                else:
                    group_db[group_name] = set()
                group_set = group_db[group_name]

                for user_name in user_names:
                    if not user_name:
                        continue  # skip null naame
                    if len(user_name) < 3:
                        msg = f"line: {line_number} of {group_file} is not valid, username must be 3 characters are longer"
                        log.warn(msg)
                        continue
                    group_set.add(user_name)
                    log.info(f"added {user_name} to group: {group_name}")
    except FileNotFoundError:
        log.error(f"unable to open group file: {group_file}")
    return group_db

def initGroupDB(app):
    """
    Called at startup to initialize group dictionary from a groups text file
    """
    log.info("initgroupDB")
    if "group_db" in app:
        msg = "group_db already initilized"
        log.warn(msg)
        return

    groups_file = config.get("groups_file")
    if not groups_file or not pp.isfile(groups_file) :
        log.info("No groups file")
        group_user_db = {}
    else:
        log.info(f"Loading groups file: {groups_file}")
        group_user_db = loadGroupsFile(groups_file)

    app["group_user_db"] = group_user_db

    # create a reverse (user -> set of groups) lookup map
    user_group_db = {}
    for group_name in group_user_db:
        user_names = group_user_db[group_name]
        for user_name in user_names:
            if user_name not in user_group_db:
                user_group_db[user_name] = set()
            users = user_group_db[user_name]
            users.add(group_name)
    app["user_group_db"] = user_group_db

    log.info(f"group_db initialized: {len(group_user_db)} groups")

def _verifyBearerToken(app, token):
    # Contact OpenID provider to validate bearer token.
    # if valid, update user db and return username
    username = None
    provider = config.get('openid_provider')
    audience = config.get('openid_audience')
    claims = config.get('openid_claims').split(',')

    # Maintain Azure defualts for compatibility.
    if not audience:
        audience = config.get('azure_resource_id')

    # If we still dont have a provider and audience, abort.
    if not provider or not audience or not claims:
        log.warn('Bearer authorization, but no OpenID configuration.')
        raise HTTPUnauthorized()

    log.debug(f"Bearer authorization, using provider: {provider}")
    log.debug(f"Bearer authorization, using audience: {audience}")
    log.debug(f"Bearer authorization, using claims: {claims}")

    # Resolve provider into an OpenID configuration.
    if provider.lower() == 'azure':
        openid_url = MSONLINE_OPENID_URL
    elif provider.lower() == 'google':
        openid_url = GOOGLE_OPENID_URL
    else:
        log.warn(f"Unknown OpenID provider: {provider}")
        raise HTTPInternalServerError()

    token_header = jwt.get_unverified_header(token)
    res = requests.get(openid_url)
    if res.status_code != 200:
        log.warn("Bad response from {openid_url}: {res.status_code}")
        if res.status_code == 404:
            raise HTTPNotFound()
        elif res.status_code == 401:
            raise HTTPUnauthorized()
        elif res.status_code == 403:
            raise HTTPForbidden()
        elif res.status_code == 503:
            raise HTTPServiceUnavailable()
        else:
            raise HTTPInternalServerError()

    jwk_uri = res.json()['jwks_uri']
    res = requests.get(jwk_uri)
    jwk_keys = res.json()
    x5c = None
    rsa = {}
    log.info("_verifyBearerToken")

    # Iterate JWK keys and extract matching x5c chain
    for key in jwk_keys['keys']:
        if key['kid'] == token_header['kid']:
            if 'x5c' in key:
                x5c = key['x5c']
            elif 'e' in key and 'n' in key:
                for field in ['e', 'n']:
                    val = key[field]
                    val = val + '='*((4 - len(val)%4)%4)
                    val = base64.urlsafe_b64decode(val.encode('utf-8'))
                    rsa[field] = int.from_bytes(val, 'big')

    # Use the X5C chain to load a public key.
    if x5c:
        log.debug("using x5c public key")
        cert = ''.join([
            '-----BEGIN CERTIFICATE-----\n',
            x5c[0],
            '\n-----END CERTIFICATE-----\n',
            ])
        public_key = load_pem_x509_certificate(cert.encode(), default_backend()).public_key()
        """
        public_key_bytes = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo)
        log.debug(f"got public key: {public_key_bytes.decode('utf-8')}")
        """


    # Use RSA numbers to load a public key.
    elif rsa:
        log.debug("using rsa public key")
        public_key = RSAPublicNumbers(**rsa).public_key(default_backend())

    # We cannot load a public key.
    else:
        log.error("Unable to extract x5c chain or RSA key from JWK keys")
        raise HTTPInternalServerError()

    #log.debug(f"bearer token - public_key: {public_key}")

    
    try:
        jwt_decode = jwt.decode(
            token,
            public_key,
            algorithms='RS256',
            audience=audience,
        )
    except InvalidAudienceError:
        log.warn(f"OpenID InvalidAudienceError with {audience}")
        raise HTTPUnauthorized()
    except InvalidSignatureError:
        log.warn("OpenID InvalidSignatureError")
        raise HTTPUnauthorized()

    for name in claims:
        if name in jwt_decode:
            username = jwt_decode[name]
            break
    else:
        log.warn("unable to retreive username from bearer token")
        return None

    exp = None
    if "exp" in jwt_decode:
        exp = jwt_decode["exp"]
        if exp < 0:
            log.warn("invalid expire time")
            raise HTTPUnauthorized()

    if exp:
        log.info(f"decoded bearer token for user: {username}, expired: {exp}")
        setPassword(app, username, token, scheme="bearer", exp=exp)
    else:
        log.info(f"decoded bearer token for user: {username}, no expiration")
        setPassword(app, username, token, scheme="bearer")

    return username


def getUserPasswordFromRequest(request):
    """ Return user defined in Auth header (if any)
    """
    user = None
    pswd = None
    app = request.app
    if 'Authorization' not in request.headers:
        log.debug("no Authorization in header")
        return None, None
    scheme, _, token =  request.headers.get('Authorization', '').partition(' ')
    if not scheme or not token:
        log.info("Invalid Authorization header")
        raise HTTPBadRequest(reason="Invalid Authorization header")

    if scheme.lower() == 'basic':
        # HTTP Basic Auth
        try:
            token = token.encode('utf-8')  # convert to bytes
            token_decoded = base64.decodebytes(token)
        except binascii.Error:
            msg = "Malformed authorization header"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if token_decoded.index(b':') < 0:
            msg = "Malformed authorization header (No ':' character)"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        user, _, pswd = token_decoded.partition(b':')
        if not user or not pswd:
            msg = "Malformed authorization header, user/password not found"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        user = user.decode('utf-8')   # convert bytes to string
        pswd = pswd.decode('utf-8')   # convert bytes to string

    elif scheme.lower() == 'bearer':
        # OpenID Auth.
        log.debug("Got OpenID bearer token.")

        # see if we've already validated this token
        user = _checkTokenCache(app, token)
        if not user:
            user = _verifyBearerToken(app, token)
        if user:
            pswd = token

    else:
        msg = f"Unsupported Authorization header scheme: {scheme}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    return user, pswd

def isAdminUser(app, username):
    """ Return true if user is an admin """
    if not username:
        return False
    if username == config.get("admin_user"):
        return True
    admin_group = config.get("admin_group")
    if not admin_group:
        return False
    group_user_db = app["group_user_db"]
    if admin_group not in group_user_db:
        return False
    admin_users = group_user_db[admin_group]
    if username in admin_users:
        return True
    return False

def aclCheck(app, obj_json, req_action, req_user):
    log.info(f"aclCheck: {req_action} for user: {req_user}")
    if isAdminUser(app, req_user):
        return  # allow admin user to do anything
    if obj_json is None:
        log.error("aclCheck: no obj json")
        raise HTTPInternalServerError() # 500
    if "acls" not in obj_json:
        log.error("no acl key")
        raise HTTPInternalServerError() # 500
    acls = obj_json["acls"]
    log.debug(f"acls: {acls}")
    if req_action not in ("create", "read", "update", "delete", "readACL", "updateACL"):
        log.error(f"unexpected req_action: {req_action}")
    
    if req_user in acls:
        acl = acls[req_user]
        log.debug(f"got acl: {acl} for user: {req_user}")
        if req_action in acl and acl[req_action]:
            log.debug("action permitted by user acl")
            return
        else:
            # treat deny for username as authorative deny
            log.warn(f"Action: {req_action} not permitted for user: {req_user}")
            raise HTTPForbidden()  # 403

    if "default" in acls:
        acl = acls["default"]
        log.debug(f"got default acl: {acl}")
        if req_action in acl and acl[req_action]:
            log.debug("action permitted by default acl")
            return

    user_group_db = app["user_group_db"]
    if req_user in user_group_db:
        user_groups = user_group_db[req_user]
        for user_group in user_groups:
            acl_name = GROUP_PREFIX + user_group
            log.debug(f"checking group acl: {acl_name}")
            if acl_name in acls:
                acl = acls[acl_name]
                if req_action in acl and acl[req_action]:
                    log.debug(f"action permitted by group acl: {acl_name}")
                    return
    
    log.warn(f"Action: {req_action} not permitted for user: {req_user}")
    raise HTTPForbidden()  # 403

def validateAclJson(acl_json):
    acl_keys = getAclKeys()
    for username in acl_json.keys():
        acl = acl_json[username]
        for acl_key in acl.keys():
            if acl_key not in acl_keys:
                msg = f"Invalid ACL key: {acl_key}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            acl_value = acl[acl_key]
            if acl_value not in (True, False):
                msg = f"Invalid ACL value: {acl_value}"

def aclOpForRequest(request):
    """ return default ACL action for request method
    """
    req_action = None
    if request.method == "GET":
        req_action = "read"
    elif request.method == "PUT":
        req_action = "create"
    elif request.method == "POST":
        req_action = "create"
    elif request.method == "DELETE":
        req_action = "delete"
    else:
        # Treat other operations (e.g. HEAD) as read
        req_action = "read"
    return req_action

def getAclKeys():
    """ Return the set of ACL keys """
    return ('create', 'read', 'update', 'delete', 'readACL', 'updateACL')


    
    
