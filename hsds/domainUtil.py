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


 
