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
 
"""
    Helper function - get endpoint we'll send http requests to 
""" 
def getEndpoint():
    endpoint = config.get("hsds_endpoint")
    return endpoint 

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


 


        
