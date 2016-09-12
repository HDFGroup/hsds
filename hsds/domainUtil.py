from aiohttp.errors import HttpBadRequest, ClientError
from hsdsUtil import getDataNodeUrl, http_get_json, isValidUuid
                                                    
#
# Domain utilities
# 
def getParentDomain(domain):
    indx = domain.find('.')
    if indx < 0:
        return None  # already at top-level domain
    if indx == len(domain) - 1:
        raise ValueError("Invalid domain") # can't end with dot
    indx += 1
    parent = domain[indx:]
    return parent

def validateDomain(id):
    if not isinstance(id, str):
        raise ValueError("Expected string type")
    if isValidUuid(id):
        raise ValueError("Domain looks like a uuid")
    if id.startswith('.'):
        raise ValueError("Domain cannot start with dot")
    if id.endswith('.'):
        raise ValueError("Domain cannot end with dot")
    if id.find('..') > 0:
        raise ValueError("Domain cannot contain consecutive dots")

def isValidDomain(id):
    try:
        validateDomain(id)
        return True
    except ValueError:
        return False


def getS3KeyForDomain(host_value):
    # Convert domain paths to S3 keys
    npos = host_value.rfind(':')
    if npos > 0:
        host = host_value[:npos]
    else:
        host = host_value

    if len(host) < 3:
        # by equivalence to internet top-level domains, .org, .com, etc
        raise ValueError('domain name is not valid')

    if host[0] == '.' or host[-1] == '.':
        # can't have a first or last dot'
        raise ValueError('domain name is not valid')

    dns_path = host.split('.')
    dns_path.reverse()  # flip to filesystem ordering
    s3Path = ''
    for field in dns_path:      
        if len(field) == 0:   
            # consecutive dots are not allowed
            raise ValueError('domain name is not valid')
        s3Path += field
        s3Path += '/'

    s3Path = s3Path[:-1]  # remove trailing slash
     
    return s3Path


def getDomainFromRequest(request):
    host = None
    if 'host' in request.GET:
        host = request.GET['host']
    else:
        host = request.host
        if "X-Forwarded-Host" in request.headers:
            host = request.headers["X-Forwarded-Host"]
    return host

async def getDomainJson(app, domain):
    """ Return domain JSON from cache or fetch from DN if not found
    """
    domain_cache = app["domain_cache"]
    #domain = getDomainFromRequest(request)

    if domain in domain_cache:
        return domain_cache[domain]

    domain_json = { }
    req = getDataNodeUrl(app, domain)
    req += "/domains/" + domain 

    try:
        domain_json = await http_get_json(app, req)
    except ClientError as ce:
        msg="Error getting domain state -- " + str(ce)
        log.warn(msg)
        raise HttpProcessingError(message=msg, code=503)
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HttpProcessingError("Unexpected error", code=500)

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HttpProcessingError("Unexpected error", code=500)

    domain_cache[domain] = domain_json  # add to cache
    return domain_json


 
