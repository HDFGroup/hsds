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
import pytz
import base64

import config
"""
    Helper function - get endpoint we'll send http requests to 
""" 
def getEndpoint():
    
    endpoint = config.get("hsds_endpoint")
    return endpoint

"""
Helper function - return true if the parameter looks like a UUID
"""
def validateId(id):
    try:
        return type(id) == str and len(id) == 38
    except Exception:
        pass
    return False

"""
Helper - return number of active sn/dn nodes
"""
def getActiveNodeCount():
    rsp_json = requests.get(getEndpoint("head") + "/info").json()
    return rsp_json["active_sn_count"], rsp_json["active_dn_count"]

"""
Helper - get base domain to use for test_cases
"""
def getTestDomainName(name):
    now = time.time()
    dt = datetime.fromtimestamp(now, pytz.utc)
    return '/'.join([
        "", # for leading (root) slash
        "home",
        config.get("user_name"),
        "hsds_test",
        name.lower(),
        "{:04d}{:02d}{:02d}T{:02d}{:02d}{:02d}_{:06d}Z".format(
                dt.year,
                dt.month,
                dt.day,
                dt.hour,
                dt.minute,
                dt.second,
                dt.microsecond)
    ])

"""
Helper - get default request headers for domain
"""
def getRequestHeaders(domain=None, username=None, password=None, **kwargs):
    headers = {}
    username = username or config.get("user_name")
    password = password or config.get("user_password")

    if domain is not None:
        headers['host'] = domain
    if username and password:
        auth_string = username + ':' + password
        auth_string = auth_string.encode('utf-8')
        auth_string = base64.b64encode(auth_string)
        auth_string = b"Basic " + auth_string
        headers['Authorization'] = auth_string
    for k,v in kwargs.items():
        headers[k] = v 
    return headers

"""
Helper - Get parent domain of given domain.
"""
def getParentDomain(domain):
    parent = op.dirname(domain)
    if not parent:
        raise ValueError("Invalid domain")
    return parent

"""
Helper - Get DNS-style domain name given a filepath domain
"""
def getDNSDomain(domain):
    # slice at end to cut off tailing dot from leading (root) slash in domain
    return '.'.join(reversed(domain.split('/')))[:-1]

"""
Helper - Create domain, creating parent folder(s) for complete heirarchy
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
    if requests.get(
            req,
            headers=getRequestHeaders(domain=parent_domain)
    ).status_code != 200:
        setupDomain(parent_domain, folder=True)

    headers = getRequestHeaders(domain=domain)
    if folder:
        body = {"folder": True}
        rsp = requests.put(req, data=json.dumps(body), headers=headers)
    else:
        rsp = requests.put(req, headers=headers)
    if rsp.status_code != 201:
        which = "folder" if folder else "domain"
        raise ValueError(
                f"Unable to put {which}: {domain}\nError {rsp.status_code}")

"""
Helper function - get root uuid for domain (raises Exceptions if problem)
""" 
def getRootUUID(domain, username=None, password=None):
    req = getEndpoint() + "/"
    headers = getRequestHeaders(
            domain=domain, username=username, password=password)
    return requests.get(req, headers=headers).json()["root"]

"""
Helper function - get a domain for one of the test files
"""
def getTestDomain(name):
    folder = '/home/test_user1/test/' #TODO: un-hardcode "test_user1"?
    return folder + name

"""
Helper function - get uuid for a given path (must be reached via hard link)
"""
def getUUIDByPath(domain, path, username=None, password=None):
    if not path.startswith("/"):
        raise KeyError("only abs paths")

    parent_uuid = getRootUUID(domain, username=username, password=password)  

    if path == '/':
        return parent_uuid

    headers = getRequestHeaders(domain=domain)
    tgt_uuid = None
    endpoint = getEndpoint()
    path = path[1:] # strip leading slash (root)

    for name in path.split('/'):
        if parent_uuid is None:
            # found non-group object that is not last name in path
            raise KeyError("not found")

        response = requests.get(
                f"{endpoint}/groups/{parent_uuid}/links/{name}",
                headers=headers)
        if response.status_code != 200:
            raise KeyError("not found")
        link = response.json()["link"]

        if link['class'] != 'H5L_TYPE_HARD':
            raise KeyError("non-hard link")

        tgt_uuid = link['id']
        if link['collection'] == 'groups':
            parent_uuid = tgt_uuid
        else:
            parent_uuid = None # flags non-group object

    return tgt_uuid


