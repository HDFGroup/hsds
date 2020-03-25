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
import os
import time
import base64
import hashlib
import json
import binascii
import subprocess
import datetime
from botocore.exceptions import ClientError
from aiobotocore import get_session
from aiohttp.web_exceptions import HTTPBadRequest, HTTPUnauthorized, HTTPNotFound, HTTPForbidden, HTTPServiceUnavailable, HTTPInternalServerError
import jwt
from jwt.exceptions import InvalidAudienceError, InvalidSignatureError
import requests
import gssapi
from cryptography.x509 import load_pem_x509_certificate
from cryptography.hazmat.backends import default_backend
import hsds_logger as log
import config

MSONLINE_OPENID_URL = "https://login.microsoftonline.com/common/.well-known/openid-configuration"

def getDynamoDBClient(app):
    """ Return dynamodb handle
    """
    if "session" not in app:
        loop = app["loop"]
        session = get_session(loop=loop)
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
    log.info("using iam role: {}".format(aws_iam_role))
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
        curl_cmd = ["curl", "http://169.254.169.254/latest/meta-data/iam/security-credentials/{}".format(aws_iam_role)]
        p = subprocess.run(curl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if p.returncode != 0:
            msg = "Error getting IAM role credentials: {}".format(p.stderr)
            log.error(msg)
        else:
            stdout = p.stdout.decode("utf-8")
            try:
                cred = json.loads(stdout)
                aws_secret_access_key = cred["SecretAccessKey"]
                aws_access_key_id = cred["AccessKeyId"]
                log.info("Got ACCESS_KEY_ID: {} from EC2 metadata".format(aws_access_key_id))
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
    log.info("using password file: {}".format(password_file))
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
                    msg = "line: {} is not valid".format(line_number)
                    log.warn(msg)
                    continue
                username = fields[0]
                passwd = fields[1]
                if len(username) < 3 or len(passwd) < 3:
                    msg = "line: {} is not valid, username and password must be 3 characters are longer".format(line_number)
                    log.warn(msg)
                    continue
                if username in user_db:
                    msg = "line: {}, username is repated".format(line_number)
                    log.warn(msg)
                    continue
                user_db[username] = {"pwd": passwd}
                log.info("added user: {}".format(username))
    except FileNotFoundError:
        log.error("unable to open password file")
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

    if config.get("AWS_DYNAMODB_GATEWAY") and config.get("AWS_DYNAMODB_USERS_TABLE"):
        # user entries will be obtained dynamicaly
        log.info("Getting DynamoDB client")
        getDynamoDBClient(app)  # get client here so any errors will be seen right away
        user_db = {}
    elif config.get("PASSWORD_SALT"):
        # use salt key to verify passwords
        log.info("using PASSWORD_SALT")
        user_db = {}
    else:
        password_file = None
        if "PASSWORD_FILE" in os.environ:
            # need to fetch this directly from os.environ to
            # have null override existing config value
            password_file = os.environ["PASSWORD_FILE"]
        else:
            password_file = config.get("password_file")
        if not password_file:
            log.info("No password file, allowing no-auth access")
            app["no_auth"] = True  # flag so we know we are in no auth mode
            user_db = {}
        else:
            log.info("Loading password file: {}".format(password_file))
            user_db = loadPasswordFile(password_file)

    app["user_db"] = user_db

    log.info("user_db initialized: {} users".format(len(user_db)))

def setPassword(app, username, password, **kwargs):
    """
    setPassword: sets a password and metadata.
    """
    log.info("Saving user/password to user_db for: {}".format(username))
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
        table_name = config.get("AWS_DYNAMODB_USERS_TABLE")
        log.info("looking for user: {} in DynamoDB table: {}".format(username, table_name))
        try:
            response = await dynamodb.get_item(
                TableName=table_name,
                Key={'username': {'S': username}}
            )
        except ClientError as e:
            log.error("Unable to read dyanamodb table: {}".format(e.response['Error']['Message']))
            raise HTTPInternalServerError()  # 500
        if "Item" not in response:
            log.info("user: {} not found".format(username))
            raise HTTPUnauthorized()  # 401
        item = response['Item']
        if "password" not in item:
            log.error("Expected to find password key in DynamoDB table")
            raise HTTPInternalServerError()  # 500
        password_item = item["password"]
        if 'S' not in password_item:
            log.error("Expected to find 'S' key for password item")
            raise HTTPInternalServerError()  # 500
        log.debug("password: {}".format(password_item))
        if password_item['S'] != password:
            log.warn("user password is not valid for user: {}".format(username))
            raise HTTPUnauthorized()  # 401
        # add user/password to user_db map
        setPassword(app, username, password)

def validatePasswordSHA512(app, username, password):
    if getPassword(app, username) is None:
        log.info("SHA512 check for username: {}".format(username))
        salt = config.get("PASSWORD_SALT")
        hex_hash = hashlib.sha512(username.encode('utf-8') + salt.encode('utf-8')).hexdigest()
        if hex_hash[:32] != password:
            log.warn("user password is not valid (didn't equal sha512 hash) for user: {}".format(username))
            raise HTTPUnauthorized()  # 401
        setPassword(app, username, password)

async def validatePasswordGSSAPI(app, username, password):
    """
    Validates a username/password against a kerberos domain controller using
    GSSAPI. This method does NOT validate the integrity of the KDC itself, and
    is open to KDC spoofing attacks if the network is compromised.
    """

    if getPassword(app, username) is None:
        log.info("GSSAPI password check for username: {}".format(username))
        realm = config.get('krb5_realm').upper()
        principle = gssapi.Name('{}@{}'.format(username, realm),
                                name_type=gssapi.NameType.user)

        # Authentication errors generate exceptions.
        try:
            loop = app['loop']
            credentials = await loop.run_in_executor(None,
                                                     gssapi.raw.acquire_cred_with_password,
                                                     principle,
                                                     password.encode('utf-8'))
        except gssapi.exceptions.GSSError as exc:
            log.warn('GSSAPI authentication failure for user: {}'.format(username))
            log.debug('GSSAPI error: {}'.format(exc))
            raise HTTPUnauthorized()

        # Set the password and expire it if a lifetime is set.
        now = time.time()
        lifetime = credentials.lifetime
        if lifetime is not None:
            expiration = min(lifetime, float(config.get("auth_expiration")))
            setPassword(app, username, password, exp=expiration + now)

        else:
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
        raise HTTPBadRequest("provide user name and password")
    if not password:
        log.info('isPasswordValid - null password')
        raise HTTPBadRequest("provide  password")

    log.debug("looking up username: {}".format(username))
    if "user_db" not in app:
        msg = "user_db not intialized"
        log.error(msg)
        raise HTTPInternalServerError()  # 500

    user_data = getPassword(app, username)

    if user_data is None:
        if "no_auth" in app and app["no_auth"]:
            log.info(f"no-auth access for user: {username}")
            setPassword(app, username, "")
        elif config.get("AWS_DYNAMODB_USERS_TABLE"):
            # look up in Dyanmo db - will throw exception if user not found
            await validateUserPasswordDynamoDB(app, username, password)
        elif config.get("PASSWORD_SALT"):
            validatePasswordSHA512(app, username, password)
        elif config.get('KRB5_REALM'):
            await validatePasswordGSSAPI(app, username, password)
        else:
            log.info("user not found")
            raise HTTPUnauthorized() # 401
        user_data = getPassword(app, username)

    if user_data['pwd'] == password:
        log.debug("user password validated")
    else:
        log.info("user password is not valid for user: {}".format(username))
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

def _verifyBearerToken(app, token):
    # Contact AD to validate bearer token.
    # if valid, update user db and return username
    username = None
    token_header = jwt.get_unverified_header(token)
    res = requests.get(MSONLINE_OPENID_URL)
    if res.status_code != 200:
        log.warn("Bad response from {MSONLINE_OPENID_URL}: {res.status_code}")
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
    log.info("_verifyBearerToken")
    resource_id = config.get('azure_resource_id')
    # Iterate JWK keys and extract matching x5c chain
    for key in jwk_keys['keys']:
        if key['kid'] == token_header['kid']:
            x5c = key['x5c']

    if not x5c:
        log.error("Unable to extract x5c chain from JWK keys")
        raise HTTPInternalServerError()

    log.debug(f"bearer token - x5c: {x5c}")

    cert = ''.join([
        '-----BEGIN CERTIFICATE-----\n',
        x5c[0],
        '\n-----END CERTIFICATE-----\n',
        ])
    public_key =  load_pem_x509_certificate(cert.encode(), default_backend()).public_key()

    log.debug(f"bearer token - public_key: {public_key}")

    try:
        jwt_decode = jwt.decode(
            token,
            public_key,
            algorithms='RS256',
            audience=resource_id,
        )
    except InvalidAudienceError:
        log.warn(f"AAD InvalidAudienceError with {resource_id}")
        raise HTTPUnauthorized()
    except InvalidSignatureError:
        log.warn("AAD InvalidSignatureError")
        raise HTTPUnauthorized()
    if "unique_name" in jwt_decode:
        # TBD: is the proper key to use?
        username = jwt_decode["unique_name"]
        exp = jwt_decode["exp"]
        log.info(f"decoded bearer token for user: {username}, expired: {exp}")
        if "user_db" not in app:
            log.info("initializing user_db")
            app["user_db"] = {}
        setPassword(app, username, token, scheme="bearer", exp=exp)
    else:
        log.warn("unable to retreive username from bearer token")

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
        raise HTTPBadRequest("Invalid Authorization header")

    if scheme.lower() == 'basic':
        # HTTP Basic Auth
        try:
            token = token.encode('utf-8')  # convert to bytes
            token_decoded = base64.decodebytes(token)
        except binascii.Error:
            msg = "Malformed authorization header"
            log.warn(msg)
            raise HTTPBadRequest(msg)
        if token_decoded.index(b':') < 0:
            msg = "Malformed authorization header (No ':' character)"
            log.warn(msg)
            raise HTTPBadRequest(msg)
        user, _, pswd = token_decoded.partition(b':')
        if not user or not pswd:
            msg = "Malformed authorization header, user/password not found"
            log.warn(msg)
            raise HTTPBadRequest(msg)
        user = user.decode('utf-8')   # convert bytes to string
        pswd = pswd.decode('utf-8')   # convert bytes to string
    elif scheme.lower() == 'bearer' and config.get('azure_app_id') and config.get('azure_resource_id'):
        # Azure AD Oauth
        app_id = config.get('azure_app_id')
        resource_id = config.get('azure_resource_id')
        log.debug(f"Got bearer token  app_id: {app_id} resource_id: {resource_id}")
        #log.debug(f"bearer token: {token}")
        # see if we've already validated this token
        user = _checkTokenCache(app, token)
        if not user:
            user = _verifyBearerToken(app, token)
        if user:
            pswd = token

    else:
        msg = "Unsupported Authorization header scheme: {}".format(scheme)
        log.warn(msg)
        raise HTTPBadRequest(msg)

    return user, pswd

def aclCheck(obj_json, req_action, req_user):
    log.info("aclCheck: {} for user: {}".format(req_action, req_user))
    if req_user == "admin":
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
        log.error("unexpected req_action: {}".format(req_action))
    acl = None
    if req_user in acls:
        acl = acls[req_user]
        log.debug(f"got acl: {acl} for user: {req_user}")
    elif "default" in acls:
        acl = acls["default"]
        log.debug(f"got default acl: {acl}")
    else:
        acl = { }
        log.debug(f"no acl found")
    if req_action not in acl or not acl[req_action]:
        log.warn("Action: {} not permitted for user: {}".format(req_action, req_user))
        raise HTTPForbidden()  # 403
    log.debug("action permitted")

def validateAclJson(acl_json):
    acl_keys = getAclKeys()
    for username in acl_json.keys():
        acl = acl_json[username]
        for acl_key in acl.keys():
            if acl_key not in acl_keys:
                msg = "Invalid ACL key: {}".format(acl_key)
                log.warn(msg)
                raise HTTPBadRequest(msg)
            acl_value = acl[acl_key]
            if acl_value not in (True, False):
                msg = "Invalid ACL value: {}".format(acl_value)

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
