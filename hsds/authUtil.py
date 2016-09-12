import base64
import binascii
from aiohttp.errors import HttpBadRequest, HttpProcessingError
import hsds_logger as log
from domainUtil import getDomainJson, getDomainFromRequest


def validateUserPassword(user_name, password):
    """
    validateUserPassword: verify user and password.
        throws exception if not valid
    """

    # just hard-code a couple of users for now
    user_db = { "test_user1": {"pwd": "test" },
                "test_user2": {"pwd": "test" } }
    
    if not user_name:
        log.info('validateUserPassword - null user')
        raise HttpBadRequest("provide user name and password")
    if not password:
        log.info('isPasswordValid - null password')
        raise HttpBadRequest("provide  password")

    log.info("looking up username: {}".format(user_name))
    if user_name not in user_db:
        log.info("user not found")
        raise HttpProcessingError(code=401, message="provide user and password")

    user_data = user_db[user_name] 
    
    if user_data['pwd'] == password:
        log.info("user  password validated")
    else:
        log.info("user password is not valid")
        raise HttpProcessingError(code=401, message="provide user and password")


def getUserFromRequest(request):
    """ Return user defined in Auth header (if any)
    """
    user = None
    pswd = None
    if 'Authorization' not in request.headers:
        return None
    scheme, _, token = auth_header = request.headers.get(
        'Authorization', '').partition(' ')
    if not scheme or not token:
        raise HttpBadRequest("Invalid Authorization header")
    if scheme.lower() != 'basic':
        raise HttpBadRequest("Unsupported Authorization header scheme: {}".format(scheme))
    try:
        token = token.encode('utf-8')  # convert to bytes
        token_decoded = base64.decodebytes(token)
    except binascii.Error:
        raise HTTPBadRequest("Malformed authorization header")
    if token_decoded.index(b':') < 0:
        raise HTTPBadRequest("Malformed authorization header")
    user, _, pswd = token_decoded.partition(b':')
    if not user or not pswd:
        raise HTTPBadRequest("Malformed authorization header")
   
    user = user.decode('utf-8')   # convert bytes to string
    pswd = pswd.decode('utf-8')   # convert bytes to string
    validateUserPassword(user, pswd)  
    return user

def aclCheck(acls, req_action, req_user):
    log.info("aclCheck: {} for user: {}".format(req_action, req_user))
    if acls is None:
        log.error("no acls found")
        raise HttpProcessingError(code=500, message="Unexpected error")
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
        domain = getDomainFromRequest(request)
        log.warn("Action: {} not permitted for user: {} in domain: {}".format(req_action, req_user, domain))
        raise HttpProcessingError(code=403, message="Forbidden")
    log.info("action permitted")
    

async def authValidate(request, req_action=None):
    """ check user credentials and that user has permissions for the given domain
    """
    app = request.app
    if req_action is None:
        if request.method == "GET":
            req_action = "read"
        elif request.method == "PUT":
            req_action = "create"
        elif request.method == "POST":
            req_action = "create"
        elif request.method == "DELETE":
            req_action = "delete"
        else:
            req_action = "read"

    req_user = getUserFromRequest(request)  # throws exception if user/password is invalid
    domain = getDomainFromRequest(request)
    domain_json =  await getDomainJson(app, domain)
    acls = domain_json["acls"]
    aclCheck(acls, req_action, req_user)    # throws exception if action not permitted
    






         