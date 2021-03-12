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
import requests
import json
import os.path as op
from datetime import datetime
import time
import base64
try:
    import pytz
    USE_UTC=True
except ModuleNotFoundError:
    USE_UTC=False

import config
"""
    Helper function - get endpoint we'll send http requests to
"""
def getEndpoint():

    endpoint = config.get("hsds_endpoint")
    return endpoint

"""
    Helper function - get endpoint we'll send http requests to
"""
def getRangeGetEndpoint():

    endpoint = config.get("rangeget_endpoint")
    return endpoint


"""
Helper function - return true if the parameter looks like a UUID
"""
def validateId(id):
    if type(id) != str:
        # should be a string
        return False
    if len(id) != 38:
        # id's returned by uuid.uuid1() are always 38 chars long
        return False
    return True

"""
Helper - return number of active sn/dn nodes
"""
def getActiveNodeCount():
    req = getEndpoint("head") + "/info"
    rsp = requests.get(req)
    rsp_json = json.loads(rsp.text)
    sn_count = rsp_json["active_sn_count"]
    dn_count = rsp_json["active_dn_count"]
    return sn_count, dn_count

"""
Helper - get base domain to use for test_cases
"""
def getTestDomainName(name):
    now = time.time()
    if USE_UTC:
        dt = datetime.fromtimestamp(now, pytz.utc)
    else:
        dt = datetime.fromtimestamp(now)
    domain = "/home/"
    domain += config.get('user_name')
    domain += '/'
    domain += 'hsds_test'
    domain += '/'
    domain += name.lower()
    domain += '/'
    domain += "{:04d}{:02d}{:02d}T{:02d}{:02d}{:02d}_{:06d}Z".format(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond)
    return domain

"""
Helper - get default request headers for domain
"""
def getRequestHeaders(domain=None, username=None, bucket=None, password=None, **kwargs):
    if username is None:
        username = config.get("user_name")
        if password is None:
            password = config.get("user_password")
    elif username == config.get("user2_name"):
        if password is None:
            password = config.get("user2_password")
    headers = { }
    if domain is not None:
        #if config.get("bucket_name"):
        #    domain = config.get("bucket_name") + domain
        headers['X-Hdf-domain'] = domain.encode('utf-8')
    if username and password:
        auth_string = username + ':' + password
        auth_string = auth_string.encode('utf-8')
        auth_string = base64.b64encode(auth_string)
        auth_string = b"Basic " + auth_string
        headers['Authorization'] = auth_string

    if config.get("bucket_name"):
        bucket_name = config.get("bucket_name")
    else:
        bucket_name = bucket
    if bucket_name:
        headers['X-Hdf-bucket'] = bucket_name.encode('utf-8')

    for k in kwargs.keys():
        headers[k] = kwargs[k]
    return headers

"""
Helper - Get parent domain of given domain.
"""
def getParentDomain(domain):
    parent = op.dirname(domain)
    if not parent:
        raise ValueError("Invalid domain") # can't end with dot
    return parent

"""
Helper - Get DNS-style domain name given a filepath domain
"""
def getDNSDomain(domain):
    names = domain.split('/')
    names.reverse()
    dns_domain = ''
    for name in names:
        if name:
            dns_domain += name
            dns_domain += '.'
    dns_domain = dns_domain[:-1]  # str trailing dot
    return dns_domain

"""
Helper - Create domain (and parent domin if needed)
"""
def setupDomain(domain, folder=False):
    endpoint = config.get("hsds_endpoint")
    headers = getRequestHeaders(domain=domain)
    req = endpoint + "/"
    rsp = requests.get(req, headers=headers)
    if rsp.status_code == 200:
        return  # already have domain
    if rsp.status_code != 404:
        # something other than "not found"
        raise ValueError(f"Unexpected get domain error: {rsp.status_code}")
    parent_domain = getParentDomain(domain)
    if parent_domain is None:
        raise ValueError(f"Invalid parent domain: {domain}")
    # create parent domain if needed
    setupDomain(parent_domain, folder=True)

    headers = getRequestHeaders(domain=domain)
    body=None
    if folder:
        body = {"folder": True}
        rsp = requests.put(req, data=json.dumps(body), headers=headers)
    else:
        rsp = requests.put(req, headers=headers)
    if rsp.status_code != 201:
        raise ValueError(f"Unexpected put domain error: {rsp.status_code}")

"""
Helper function - get root uuid for domain
"""
def getRootUUID(domain, username=None, password=None):
    req = getEndpoint() + "/"
    headers = getRequestHeaders(domain=domain, username=username, password=password)

    rsp = requests.get(req, headers=headers)
    root_uuid= None
    if rsp.status_code == 200:
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
    return root_uuid


"""
Helper function - get a domain for one of the test files
"""
def getTestDomain(name):
    username = config.get("user_name")
    path = f'/home/{username}/test/{name}'
    return path

"""
Helper function - get uuid for a given path
"""
def getUUIDByPath(domain, path, username=None, password=None):
    if path[0] != '/':
        raise KeyError("only abs paths") # only abs paths

    parent_uuid = getRootUUID(domain, username=username, password=password)

    if path == '/':
        return parent_uuid

    headers = getRequestHeaders(domain=domain)

    # make a fake tgt_json to represent 'link' to root group
    tgt_json = {'collection': "groups", 'class': "H5L_TYPE_HARD", 'id': parent_uuid }
    tgt_uuid = None

    names = path.split('/')

    for name in names:
        if not name:
            continue
        if parent_uuid is None:
            raise KeyError("not found")

        req = getEndpoint() + "/groups/" + parent_uuid + "/links/" + name
        rsp = requests.get(req, headers=headers)
        if rsp.status_code != 200:
            raise KeyError("not found")
        rsp_json = json.loads(rsp.text)
        tgt_json = rsp_json['link']

        if tgt_json['class'] == 'H5L_TYPE_HARD':
            if tgt_json['collection'] == 'groups':
                parent_uuid = tgt_json['id']
            else:
                parent_uuid = None
            tgt_uuid = tgt_json['id']
        else:
            raise KeyError("non-hard link")
    return tgt_uuid

"""
Helper function = get HDF5 JSON dump for chunbk locations
"""
def getHDF5JSON(filename):
    if not op.isfile(filename):
        return None
    hdf5_json = None
    with open(filename) as f:
        hdf5_json = json.load(f)
    return hdf5_json
