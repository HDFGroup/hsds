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


#
# Domain utilities
# 
def isIPAddress(s):
    """Return True if the string looks like an IP address:
        n.n.n.n where n is between 0 and 255 """
    
    if s == 'localhost':
        return True # special case for loopback dns_path

    parts = s.split('.')
    
    if len(parts) != 4:
        return False
    for part in parts:
        try:
            n = int(part)
            if n < 0 or n > 255:
                return False
        except ValueError:
            return False
    return True

def getParentDomain(domain):
    """Get parent domain of given domain.
    E.g. getParentDomain("www.hdfgroup.org") returns "hdfgroup.org"
    Return None if the given domain is already a top-level domain.
    """
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
    if len(id) < 3:
        raise ValueError("Domain name is too short")
    if len(id) == 38 and id[5] == '-' and id[7] == '-' and id[16] == '-' and id[21] == '-' and id[26] == '-':  
        raise ValueError("Domain name not allowed")
    if len(id) == 14 and id.endswith("-headnode"):
        raise ValueError("Domain name not allowed")
    if id.startswith('.'):
        raise ValueError("Domain cannot start with dot")
    if id.endswith('.'):
        raise ValueError("Domain cannot end with dot")
    if id.startswith('-'):
        raise ValueError("Domain cannot start with hyphen")
    if id.endswith('-'):
        raise ValueError("Domain cannot end with hyphen")
    if id.find('..') > 0:
        raise ValueError("Domain cannot contain consecutive dots")
    if isIPAddress(id):
        raise ValueError("Domain looks like IP address")
    if id.find('/') >= 0:
        raise ValueError("Domain cannot contain slash")

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




 
