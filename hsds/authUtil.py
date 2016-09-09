import base64
import binascii
from aiohttp.errors import HttpBadRequest, HttpProcessingError
import hsds_logger as log


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
    print("user_data[pwd]", user_data['pwd'])
    print("password:", password)
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
         