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
import re
from aiohttp.web_exceptions import HTTPBadRequest

from .. import config
#
# Domain utilities
#

DOMAIN_SUFFIX = "/.domain.json"  # key suffix used to hold domain info


def isIPAddress(s):
    """Return True if the string looks like an IP address:
    n.n.n.n where n is between 0 and 255"""
    if not s:
        return False
    # see if there is a port specifier
    if s.find(":") > 0:
        return True

    if s == "localhost":
        return True  # special case for loopback dns_path

    parts = s.split(".")

    if len(parts) != 4:
        return False
    for part in parts:
        if part == ":":
            # skip past a possible port specifier
            break
        try:
            n = int(part)
            if n < 0 or n > 255:
                return False
        except ValueError:
            return False
    return True


def _stripProtocol(uri):
    """ returns part of the uri or bucket name after any protocol specification:
      'xyz://' or 'https://myaccount.blob.core.windows.net/'
    """

    if not uri or uri.startswith("/"):
        return uri

    n = uri.find("://")
    if n < 0:
        return uri

    uri = uri[(n + 3):]
    parts = uri.split("/")
    if len(parts) == 1:
        return uri
    if parts[0].endswith(".blob.core.windows.net"):
        # part of the URI to indicate azure blob storage, skip it
        parts = parts[1:]
    return "/".join(parts)


def isValidBucketName(bucket):
    """
    Check whether the given bucket name is valid
    """
    is_valid = True

    if bucket is None:
        return True

    bucket = _stripProtocol(bucket)

    # Bucket names must contain at least 1 character
    if len(bucket) < 1:
        is_valid = False

    # Bucket names can consist only of alphanumeric characters, underscores, dots, and hyphens
    # other than
    if not re.fullmatch("[a-zA-Z0-9_\\.\\-]+", bucket):
        is_valid = False

    return is_valid


def getBucketForDomain(domain):
    """get the bucket for the domain or None
    if no bucket is given
    """
    if not domain:
        return None

    # strip s3://, file://, etc
    domain_path = _stripProtocol(domain)
    if domain_path.startswith("/"):
        # no bucket specified
        return None

    nchars = len(domain) - len(domain_path)
    protocol = domain[:nchars]  # save this so we can re-attach to the bucket name

    parts = domain_path.split("/")
    if len(parts) < 2:
        # invalid domain?
        msg = f"invalid domain: {domain}"
        raise HTTPBadRequest(reason=msg)
    bucket_name = parts[0]
    if not isValidBucketName(bucket_name):
        return None

    # fit back the protocol prefix if set
    if protocol:
        bucket = protocol
    else:
        bucket = ""
    bucket += bucket_name
    return bucket


def getPathForDomain(domain):
    """
    Return the non-bucket part of the domain
    """
    if not domain:
        return None

    domain_path = _stripProtocol(domain)
    if domain_path.startswith("/"):
        # no bucket
        return domain_path

    nindex = domain_path.find("/")
    if nindex > 0:
        # don't include the bucket
        domain_path = domain_path[nindex:]

    return domain_path


def getParentDomain(domain):
    """Get parent domain of given domain.
    E.g. getParentDomain("www.hdfgroup.org") returns "hdfgroup.org"
    Return None if the given domain is already a top-level domain.
    """
    if domain.endswith(DOMAIN_SUFFIX):
        n = len(DOMAIN_SUFFIX) - 1
        domain = domain[:-n]

    bucket = getBucketForDomain(domain)
    domain_path = getPathForDomain(domain)
    if len(domain_path) > 1 and domain_path[-1] == "/":
        domain_path = domain_path[:-1]
    dirname = op.dirname(domain_path)
    if bucket:
        parent = bucket + dirname
    else:
        parent = dirname

    if not parent:
        parent = None
    return parent


def validateHostDomain(id):
    if not isinstance(id, str):
        raise ValueError("Expected string type")
    if len(id) < 3:
        raise ValueError("Host Domain name is too short")
    if len(id) == 38 and all(
        (
            id[5] == "-",
            id[7] == "-",
            id[16] == "-",
            id[21] == "-",
            id[26] == "-",
        )
    ):
        raise ValueError("Host Domain name not allowed")
    if len(id) == 14 and id.endswith("-headnode"):
        raise ValueError("Host Domain name not allowed")
    if id.startswith("."):
        raise ValueError("Host Domain cannot start with dot")
    if id.endswith("."):
        raise ValueError("Host Domain cannot end with dot")
    if id.startswith("-"):
        raise ValueError("Host Domain cannot start with hyphen")
    if id.endswith("-"):
        raise ValueError("Host Domain cannot end with hyphen")
    if id.find("..") > 0:
        raise ValueError("Host Domain cannot contain consecutive dots")
    if isIPAddress(id):
        raise ValueError("Host Domain looks like IP address")
    if id.find("/") >= 0:
        raise ValueError("Host Domain cannot contain slash")
    if id.find(".") == -1:
        raise ValueError("Host domain must have a dot")


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
    if id.find("/") == -1:
        raise ValueError("Domain names should include a '/'")
    if id[-1] == "/":
        raise ValueError("Slash at end not allowed")
    # the non-bucket part of the domain should start with '/'
    domain_path = getPathForDomain(id)
    if not domain_path or domain_path[0] != "/":
        raise ValueError("Domain path should start with '/'")


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
    if path == "/":
        return  # default bucket, root folder
    if path[:-1].find("/") == -1:
        msg = "Domain path should have at least one '/' before trailing slash"
        raise ValueError(msg)
    if path[-1] != "/":
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


def getDomainFromRequest(request, validate=True):
    # print("gotDomainFromRequest:", request, "validate=", validate)
    app = request.app
    domain = None
    bucket = None
    params = request.rel_url.query
    if "domain" in params:
        domain = params["domain"]
    elif "X-Hdf-domain" in request.headers:
        domain = request.headers["X-Hdf-domain"]
    else:
        return None

    if domain.startswith("hdf5:/"):
        # strip off the prefix to make following logic easier
        domain = domain[6:]

    # validate that its a properly formed domain
    if validate:
        validateDomain(domain)
    if "bucket" in params and params["bucket"]:
        bucket = params["bucket"]
    elif "X-Hdf-bucket" in request.headers:
        bucket = request.headers["X-Hdf-bucket"]
    elif "bucket_name" in request.app and request.app["bucket_name"]:
        # prefix the domain with the bucket name
        bucket = app["bucket_name"]
    else:
        pass  # no bucket specified

    if bucket and validate:
        if not isValidBucketName(bucket):
            raise ValueError(f"bucket name: {bucket} is not valid")
        if domain[0] == "/":
            domain = bucket + domain
    return domain


def verifyRoot(domain_json):
    """Throw bad request if we are expecting a domain,
    but got a folder instead
    """
    if "root" not in domain_json:
        msg = "Expected root key for domain"
        # can't use hsds logger, since it would create a circular dependency
        print("WARN> " + msg)
        raise HTTPBadRequest(reason=msg)


def getLimits():
    """return limits the client may need"""
    limits = {}
    limits["min_chunk_size"] = int(config.get("min_chunk_size"))
    limits["max_chunk_size"] = int(config.get("max_chunk_size"))
    limits["max_request_size"] = int(config.get("max_request_size"))

    return limits
