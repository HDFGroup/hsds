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
import base64
import hashlib
import json
import binascii
import subprocess
import datetime
from botocore.exceptions import ClientError
from aiohttp.web_exceptions import HTTPBadRequest, HTTPUnauthorized, HTTPForbidden, HTTPInternalServerError
import hsds_logger as log
import config


def getDynamoDBClient(app):
    """ Return dynamodb handle
    """
    if "session" not in app:
        # app startup should have set this
        raise KeyError("Session not initialized")
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
        user_db = {}
    else:
        password_file = config.get("password_file")
        if not password_file:
            log.info("No password file, allowing no-auth access")
            user_db = {}
        else:
            log.info("Loading password file: {}".format(password_file))
            user_db = loadPasswordFile(password_file)

    app["user_db"] = user_db
    
    log.info("user_db initialized: {} users".format(len(user_db)))
 

async def validateUserPasswordDynamoDB(app, username, password):
    """
    validateUserPassword: verify user and password.
        throws exception if not valid
    Note: make this async since we'll eventually need some sort of http request to validate user/passwords
    """
    user_db = app["user_db"]    
    if username not in user_db:
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
        # TODO - have the entry expire after x minutes
        log.info("Saving user/password to user_db for: {}".format(username))
        user_data = {"pwd": password}
        user_db[username] = user_data

def validatePasswordSHA512(app, username, password):
    user_db = app["user_db"] 
    if username not in user_db:
        log.info("SHA512 check for username: {}".format(username))
        salt = config.get("PASSWORD_SALT")
        hex_hash = hashlib.sha512(username.encode('utf-8') + salt.encode('utf-8')).hexdigest()
        if hex_hash[:32] != password:
            log.warn("user password is not valid (didn't equal sha512 hash) for user: {}".format(username))
            raise HTTPUnauthorized()  # 401
        log.info("Saving user/password to user_db for: {}".format(username))
        user_data = {"pwd": password}
        user_db[username] = user_data


async def validateUserPassword(app, username, password):
    """
    validateUserPassword: verify user and password.
        throws exception if not valid
    Note: make this async since we'll eventually need some sort of http request to validate user/passwords
    """
    
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
    user_db = app["user_db"]    
    if username not in user_db:
        if config.get("AWS_DYNAMODB_USERS_TABLE"):
            # look up in Dyanmo db - will throw exception if user not found
            await validateUserPasswordDynamoDB(app, username, password)
        elif config.get("PASSWORD_SALT"):
            validatePasswordSHA512(app, username, password)
        elif not config.get("password_file"):
            log.info(f"no-auth access for user: {username}")
            user_db[username] = {"pwd": password}
        else:
            log.info("user not found")
            raise HTTPUnauthorized() # 401   

    user_data = user_db[username] 
    
    if user_data['pwd'] == password:
        log.debug("user password validated")
    else:
        log.info("user password is not valid for user: {}".format(username))
        raise HTTPUnauthorized() # 401  


def getUserPasswordFromRequest(request):
    """ Return user defined in Auth header (if any)
    """
    user = None
    pswd = None
    if 'Authorization' not in request.headers:
        log.debug("no Authorization in header")
        return None, None
    scheme, _, token =  request.headers.get('Authorization', '').partition(' ')
    if not scheme or not token:
        log.info("Invalid Authorization header")
        raise HTTPBadRequest("Invalid Authorization header")
    if scheme.lower() != 'basic':
        msg = "Unsupported Authorization header scheme: {}".format(scheme)
        log.warn(msg)
        raise HTTPBadRequest(msg)
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
