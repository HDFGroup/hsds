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
import binascii
from aiohttp.errors import HttpBadRequest, HttpProcessingError
import hsds_logger as log

def initUserDB(app, password_file):
    """
    Called at startup to initialize user/passwd dictionary from a password text file
    """
    log.info("initUserDB")
    if "user_db" in app:
        msg = "user_db already initilized"
        log.warn(msg)
        return
    log.info("using password file: {}".format(password_file))
    line_number = 0
    user_count = 0
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
                user_count += 1
    except FileNotFoundError:
        log.error("unable to open password file")
    app["user_db"] = user_db
    log.info("added: {} users".format(user_count))
    


def validateUserPassword(app, user_name, password):
    """
    validateUserPassword: verify user and password.
        throws exception if not valid
    Note: make this async since we'll eventually need some sort of http request to validate user/passwords
    """
    
    if not user_name:
        log.info('validateUserPassword - null user')
        raise HttpBadRequest("provide user name and password")
    if not password:
        log.info('isPasswordValid - null password')
        raise HttpBadRequest("provide  password")

    log.info("looking up username: {}".format(user_name))
    if "user_db" not in app:
        msg = "user_db not intialized"
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg)
    user_db = app["user_db"]    
    if user_name not in user_db:
        log.info("user not found")
        raise HttpProcessingError(code=401, message="provide user and password")

    user_data = user_db[user_name] 
    
    if user_data['pwd'] == password:
        log.info("user  password validated")
    else:
        log.info("user password is not valid")
        raise HttpProcessingError(code=401, message="provide user and password")


def getUserPasswordFromRequest(request):
    """ Return user defined in Auth header (if any)
    """
    user = None
    pswd = None
    if 'Authorization' not in request.headers:
        log.info("no Authorization in header")
        return None, None
    scheme, _, token =  request.headers.get('Authorization', '').partition(' ')
    if not scheme or not token:
        log.info("Invalid Authorization header")
        raise HttpBadRequest("Invalid Authorization header")
    if scheme.lower() != 'basic':
        msg = "Unsupported Authorization header scheme: {}".format(scheme)
        log.warn(msg)
        raise HttpBadRequest(msg)
    try:
        token = token.encode('utf-8')  # convert to bytes
        token_decoded = base64.decodebytes(token)
    except binascii.Error:
        msg = "Malformed authorization header"
        log.warn(msg)
        raise HttpBadRequest(msg)
    if token_decoded.index(b':') < 0:
        msg = "Malformed authorization header (No ':' character)"
        log.warn(msg)
        raise HttpBadRequest(msg)
    user, _, pswd = token_decoded.partition(b':')
    if not user or not pswd:
        msg = "Malformed authorization header, user/password not found"
        log.warn(msg)
        raise HttpBadRequest(msg)
   
    user = user.decode('utf-8')   # convert bytes to string
    pswd = pswd.decode('utf-8')   # convert bytes to string
    
    return user, pswd

def aclCheck(obj_json, req_action, req_user):
    log.info("aclCheck: {} for user: {}".format(req_action, req_user))
    if obj_json is None:
        log.error("no acls found")
        raise HttpProcessingError(code=500, message="Unexpected error")
    if "acls" not in obj_json:
        log.error("no acls found")
        raise HttpProcessingError(code=500, message="Unexpected error")
    acls = obj_json["acls"]
    if req_action not in ("create", "read", "update", "delete", "readACL", "updateACL"):
        log.error("unexpected req_action: {}".format(req_action))
    acl = None
    if req_user in acls:
        acl = acls[req_user]
    elif "default" in acls:
        acl = acls["default"]
    else:
        acl = { }
    if req_action not in acl or not acl[req_action]:
        log.warn("Action: {} not permitted for user: {}".format(req_action, req_user))
        raise HttpProcessingError(code=403, message="Forbidden")
    log.info("action permitted")

def validateAclJson(acl_json):
    acl_keys = getAclKeys()
    for username in acl_json.keys():
        acl = acl_json[username]
        for acl_key in acl.keys():
            if acl_key not in acl_keys:
                msg = "Invalid ACL key: {}".format(acl_key)
                log.warn(msg)
                raise HttpBadRequest(msg)
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
 



         