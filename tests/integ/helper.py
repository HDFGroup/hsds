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
from copy import copy
import requests
import json
import os.path as op
from datetime import datetime
import time
import pytz
import base64
import unittest

import config
"""
    Helper function - get endpoint we'll send http requests to 
""" 
def getEndpoint():
    return config.get("hsds_endpoint")

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
    headers = getRequestHeaders(
            domain=domain, username=username, password=password)
    response = requests.get(getEndpoint() + "/", headers=headers)
    try:
        return response.json()["root"]
    except json.decoder.JSONDecodeError:
        code = response.status_code
        raise ValueError(
                f"Unable to get root group uuid for `{domain}`.\n" + 
                f"HTTP code {code}")

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

"""
Helper - post group and return its UUID. ValueError raised if problem.
Optionally links on absolute path is path is valid.
"""
def postGroup(domain, path=None, response=False):
    return _post(
            "groups",
            domain,
            {},
            path=path,
            response=response)

"""
Helper - post dataset and return its UUID. ValueError raised if problem.
Optionally links on absolute path if path is valid.
If keyword argument `response` is True, will return `requests` response;
else returns UUID of dataset.
"""
def postDataset(domain, data, linkpath=None, response=False) :
    return _post(
            "datasets",
            domain,
            copy(data),
            path=linkpath,
            response=response)

"""
Helper - Go-to util function to create objects
"""
def _post(collection, domain, data, path=None, response=False):
    endpoint = getEndpoint()
    parent_uuid = None
    headers = getRequestHeaders(domain=domain)
    if path is not None:
        linkname = path.split('/')[-1]
        path = op.dirname(path)
        parent_uuid = getUUIDByPath(domain, path)
        data["link"] = {"id": parent_uuid, "name": linkname}
    post_rsp = requests.post(
            f"{endpoint}/{collection}",
            headers=headers,
            data=json.dumps(data))
    if response:
        return post_rsp
    code = post_rsp.status_code
    if code != 201:
        raise ValueError(f"Unable to post to {collection}: {code}")
    return post_rsp.json()["id"]

"""
Helper - Update a dataset with given dimensions. Raises Exceptions.
If response is true, returns the `requests` response; else attempts to verify
that the operation was successful.
"""
def resizeDataset(domain, dset_uuid, dims, response=False):
    endpoint = getEndpoint()
    headers = getRequestHeaders(domain=domain)
    res = requests.put(
            f"{endpoint}/datasets/{dset_uuid}/shape",
            headers=headers,
            data=json.dumps({"shape": dims}))
    if response == True:
        return res
    code = res.status_code
    if code != 201:
        raise ValueError(f"Unable to update dataset shape: {code}")

# ----------------------------------------------------------------------

def verifyUUID(testcase, s):
    testcase.assertTrue(validateId(s), "probably not UUID: " + s)

def verifyListMembership(testcase, actual, expected):
    are_same_set = (sorted(actual) == sorted(expected))
    if are_same_set:
        return
    which = "extra"
    diff = [x for x in actual if x not in expected]
    if diff == [] :
        which = "missing"
        diff = [m for m in expected if m not in actual]
    assert len(diff) != 0, "sanity check"
    testcase.assertTrue(are_same_set, f"{which}: {diff}")

def verifyDictionaryKeys(testcase, d, keys):
    verifyListMembership(testcase, list(d.keys()), keys)

def verifyRelsInJSONHrefs(testcase, _json, _rels):
    href_rels = [item["rel"] for item in _json["hrefs"]]
    verifyListMembership(testcase, href_rels, _rels)

"""
Helper - unittest TestCase wrapper with default self-setup
"""
class TestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestCase, self).__init__(*args, **kwargs)
        self.domain = getTestDomainName(self.__class__.__name__)
        setupDomain(self.domain)
        self.endpoint = getEndpoint()
        self.headers = getRequestHeaders(domain=self.domain)
        self.root_uuid = getRootUUID(self.domain)

    assertDictHasOnlyKeys = verifyDictionaryKeys
    assertHrefsHasOnlyRels = verifyRelsInJSONHrefs
    assertJSONHasOnlyKeys = verifyDictionaryKeys
    assertListMembershipEqual = verifyListMembership
    assertLooksLikeUUID = verifyUUID

    def assertGroupsListLenIs(self, num):
        rsp = requests.get(
                f"{self.endpoint}/groups",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "could not get groups")
        self.assertEqual(len(rsp.json()["groups"]), num)

    def assertGroupHasNLinks(self, group_uuid, num):
        rsp = requests.get(
                f"{self.endpoint}/groups/{group_uuid}",
                headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "could not get group")
        self.assertEqual(rsp.json()["linkCount"], num)


