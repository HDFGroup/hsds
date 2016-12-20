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
import base64
import config
import time
from datetime import datetime
import pytz
import requests
 
"""
    Helper function - get endpoint we'll send http requests to 
""" 
def getEndpoint():
    endpoint = config.get("hsds_endpoint")
    return endpoint 

"""
Helper - get base domain to use for test_cases
"""
def getTestDomainName(name):
    now = time.time()
    dt = datetime.fromtimestamp(now, pytz.utc)
    domain = "{:04d}{:02d}{:02d}T{:02d}{:02d}{:02d}_{:06d}Z".format(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond)  
    domain += '.' 
    domain += name.lower()
    domain += '.'
    domain += config.get('user_name')
    domain += '.home'
    return domain
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
    print("setupdomain: ", domain)
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
        raise ValueError("Invalid parent domain: {}".format(domain))
    # create parent domain if needed
    setupDomain(parent_domain)  
     
    headers = getRequestHeaders(domain=domain)
    rsp = requests.put(req, headers=headers)
    if rsp.status_code != 201:
        raise ValueError("Unexpected put domain error: {}".format(rsp.status_code))

"""
get default request headers for domain
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
        auth_string = auth_string.decode('utf-8')
        auth_string = "Basic " + auth_string
        headers['Authorization'] = auth_string
    return headers


 


        
