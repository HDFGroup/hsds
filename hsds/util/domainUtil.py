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

import os.path as op
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
    if domain.endswith("/domain.json"):
        n = len("/domain.json")
        domain = domain[:-n]
    parent = op.dirname(domain)
    
    if not parent:
        parent = None
    return parent

def validateHostDomain(id):
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

def isValidHostDomain(id):
    try:
        validateHostDomain(id)
        return True
    except ValueError:
        return False

def validateDomain(id):
    if not isinstance(id, str):
        raise ValueError("Expected string type")
    if id[-1] ==  '/':
        raise ValueError("Slash at end not allowed")
    

def isValidDomain(id):
    try:
        validateDomain(id)
        return True
    except ValueError:
        return False

def validateDomainKey(domain_key):
    if not domain_key.endswith("/domain.json"):
        raise ValueError("Invalid domain key")


def getDomainForHost(host_value):
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
    domain = ''
    for field in dns_path:      
        if len(field) == 0:   
            # consecutive dots are not allowed
            raise ValueError('domain name is not valid')
        domain += field
        domain += '/'

    domain = domain[:-1]  # remove trailing slash
     
    return domain

def getDomainFromRequest(request):
    #domain = request.match_info.get()
    domain = None
    if "domain" in request.GET:
        domain = request.GET["domain"]
        validateDomain(domain)
    else:
        host = None
        if 'host' in request.GET:
            host = request.GET['host']
        else:
            host = request.host
            if "X-Forwarded-Host" in request.headers:
                host = request.headers["X-Forwarded-Host"]
        print("getDomainFromRequest, host: {}".format(host))
        if host[0] == '/':  # path style host
            domain = host
            validateDomain(domain)
        else:  # DNS style host
            validateHostDomain(host) # throw ValueError if invalid
            domain = getDomainForHost(host)  # convert to s3 path
    return domain

def getS3KeyForDomain(domain):
    s3_key = op.join(domain, "domain.json")
    return s3_key




 
