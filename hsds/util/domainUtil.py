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

DOMAIN_SUFFIX = "/.domain.json"  # key suffix used to hold domain info

def isIPAddress(s):
    """Return True if the string looks like an IP address:
        n.n.n.n where n is between 0 and 255 """
    if not s:
        return False
    # see if there is a port specifier
    if s.find(':') > 0:
        return True
     
    if s == 'localhost':
        return True # special case for loopback dns_path

    parts = s.split('.')
    
    if len(parts) != 4:
        return False
    for part in parts:
        if part == ':':
            # skip past a possible port specifier
            break
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
    if domain.endswith(DOMAIN_SUFFIX):
        n = len(DOMAIN_SUFFIX)
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
    if len(id) < 3:
        raise ValueError("Domain name too short")
    if id[0] != '/':
        raise ValueError("Domain names should start with '/'")
    if id[-1] ==  '/':
        raise ValueError("Slash at end not allowed")    

def isValidDomain(id):
    try:
        validateDomain(id)
        return True
    except ValueError:
        return False


def validateDomainPath(path):
    if not isinstance(path, str):
        raise ValueError("Expected string type")
    if len(path) < 1:
        raise ValueError("Domain path too short")
    if path.find('/') == -1:
        raise ValueError("Domain path should have at least one '/'")
    if path[-1] !=  '/':
        raise ValueError("Domain path must end with '/'")

def isValidDomainPath(path):
    try:
        validateDomainPath(path)
        return True
    except ValueError:
        return False

def validateDomainKey(domain_key):
    if not domain_key.endswith(DOMAIN_SUFFIX):
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
    domain = '/'
    for field in dns_path:      
        if len(field) == 0:   
            # consecutive dots are not allowed
            raise ValueError('domain name is not valid')
        domain += field
        domain += '/'

    domain = domain[:-1]  # remove trailing slash
     
    return domain

def getDomainFromRequest(request, domain_path=False, validate=True):
    app = request.app
    domain = None
    params = request.rel_url.query
    if "domain" in params:
        domain = params["domain"]
    else:
        if 'host' in params:
            domain = params['host']
        elif "X-Hdf-domain" in request.headers:
            domain = request.headers['X-Hdf-domain']
        elif "X-Forwarded-Host" in request.headers:
            domain = request.headers["X-Forwarded-Host"]
        else:
            domain = request.host
            
    if domain and not domain.find('/') > -1:  #DNS style host
        if domain_path and validate:
            raise ValueError("Domain paths can not be DNS-style")
        if validate:
            validateHostDomain(domain) # throw ValueError if invalid
            domain = getDomainForHost(domain)  # convert to s3 path
        else:
            try:
                validateHostDomain(domain)
                domain = getDomainForHost(domain)
            except ValueError:
                pass # ignore
    # now validate that its a properly formed domain
    if validate:
        if domain_path:
            validateDomainPath(domain)
        else:
            validateDomain(domain)
    if domain[0] == '/':
        bucket = None
        if "bucket_name" in request.app and request.app["bucket_name"]:
            # prefix the domain with the bucket name
            domain = request.app["bucket_name"] + domain
        else:
            # if no default bucket is set, domain paths must include bucket name
            raise ValueError("bucket not specified")
    
    return domain


def getS3PrefixForDomain(domain):
    if domain[0] == '/':
        domain_key = domain[1:]  # strip off leading slash
    else:
        # get path after bucket specifiers
        index = domain.find['/']
        domain_key = domain[(index+1):]
    if domain_key.endswith(DOMAIN_SUFFIX):
        path_len = len(domain_key) - len(DOMAIN_SUFFIX)
        domain_key = domain_key[:path_len]
    if not domain_key[-1] == '/':
        #domain_key += '/'
        domain_key = domain_key[:-1]
    return domain_key

def getBucketForDomain(domain):
    if domain[0] == '/':
        # no bucket specified
        return None
    index = domain.find['/']
    return domain[:index]
