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
    if type(id) != str and type(id) != unicode: 
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
    print("rsp_json", rsp_json)
    sn_count = rsp_json["active_sn_count"]
    dn_count = rsp_json["active_dn_count"]
    return sn_count, dn_count

"""
Helper - get base domain to use for test_cases
"""
def getTestDomainName(name):
    now = int(time.time())
    dt = datetime.fromtimestamp(now, pytz.utc)
    domain = "{:04d}{:02d}{:02d}T{:02d}{:02d}{:02d}Z".format(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)  
    domain += '.' 
    domain += name.lower()
    domain += '.'
    domain += config.get('user_name')
    domain += '.home'
    return domain



"""
Helper - get default request headers for domain
"""
def getRequestHeaders(domain=None, username=None, password=None):
    if username is None:
        username = config.get("user_name")
    if password is None:
        password = config.get("user_password")
    headers = { }
    if domain is not None:
        headers['host'] = domain
    if username and password:
        auth_string = username + ':' + password
        auth_string = auth_string.encode('utf-8')
        auth_string = base64.b64encode(auth_string)
        auth_string = b"Basic " + auth_string
        headers['Authorization'] = auth_string
    return headers

"""
Helper - Get parent domain of given domain.
"""
def getParentDomain(domain):
    indx = domain.find('.')
    if indx < 0:
        return None  # already at top-level domain
    if indx == len(domain) - 1:
        raise ValueError("Invalid domain") # can't end with dot
    indx += 1
    parent = domain[indx:]
    return parent

"""
Helper - Create domain (and parent domin if needed)
"""
def setupDomain(domain):
    endpoint = config.get("hsds_endpoint")
    headers = getRequestHeaders(domain=domain)
    req = endpoint + "/"
    rsp = requests.get(req, headers=headers)
    if rsp.status_code == 200:
        return  # already have domain
    if rsp.status_code != 404:
        # something other than "not found"
        raise ValueError("Unexpected get domain error: {}".format(rsp.status_code))

    parent_domain = getParentDomain(domain)
    if parent_domain is None:
        raise ValueError("Invalid domain")
    # create parent domain if needed
    setupDomain(parent_domain)  
     
    headers = getRequestHeaders(domain=domain)
    rsp = requests.put(req, headers=headers)
    if rsp.status_code != 201:
        raise ValueError("Unexpected put domain error: {}".format(rsp.status_code))

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
       

     

    



        
