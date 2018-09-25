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
# data node of hsds cluster
# 
import time
from aiohttp.web_exceptions import HTTPConflict, HTTPInternalServerError
from aiohttp.web import json_response

from util.authUtil import  getAclKeys
from util.domainUtil import isValidDomain
from util.idUtil import validateInPartition
from datanode_lib import get_metadata_obj, save_metadata_obj, delete_metadata_obj, check_metadata_obj
import hsds_logger as log

def get_domain(request, body=None):
    """ Extract domain and validate """
    app = request.app
    params = request.rel_url.query

    domain = None
    log.debug(f"request.has_body: {request.has_body}")
    if "domain" in params:
        domain = params["domain"]
        log.debug("got domain param: {}".format(domain))
    elif body and "domain" in body:
        domain = body["domain"]
             
    if not domain: 
        msg = "No domain provided"  
        log.error(msg)
        raise HTTPInternalServerError() 

    if not isValidDomain(domain):
        msg = "Expected valid domain for [{}]".format(domain)
        log.error(msg)
        raise HTTPInternalServerError() 
    try:
        validateInPartition(app, domain)
    except KeyError as ke:
        msg = "Domain not in partition"
        log.error(msg)
        raise HTTPInternalServerError()
    return domain

async def GET_Domain(request):
    """HTTP GET method to return JSON for /domains/
    """
    log.request(request)
    app = request.app

    domain = get_domain(request)
    log.debug("get domain: {}".format(domain))
    domain_json = await get_metadata_obj(app, domain)

    resp = json_response(domain_json)
    log.response(request, resp=resp)
    return resp

async def PUT_Domain(request):
    """HTTP PUT method to create a domain
    """
    log.request(request)
    app = request.app

    if not request.has_body:
        msg = "Expected body in put domain"
        log.error(msg)
        raise HTTPInternalServerError() 
    body = await request.json() 
    log.debug(f"got body: {body}")
    log.debug(f"request_has_body: {request.has_body}")

    domain = get_domain(request, body=body)
 
    log.debug("PUT domain: {}".format(domain))
 
    body_json = await request.json()
    if "owner" not in body_json:
        msg = "Expected Owner Key in Body"
        log.warn(msg)
        raise HTTPInternalServerError() 
    if "acls" not in body_json:
        msg = "Expected Owner Key in Body"
        log.warn(msg)
        raise HTTPInternalServerError() 

    # try getting the domain, should raise 404
    domain_exists = await check_metadata_obj(app, domain)
      
    if domain_exists:
        # domain already exists
        msg = "Conflict: resource exists: " + domain
        log.info(msg)
        raise HTTPConflict()

          
    domain_json = { }
    if "root" in body_json:
        domain_json["root"] = body_json["root"]
    else:
        log.info("no root id, creating folder")
    domain_json["owner"] = body_json["owner"]
    domain_json["acls"] = body_json["acls"]
    now = time.time()
    domain_json["created"] = now
    domain_json["lastModified"] = now

    await save_metadata_obj(app, domain, domain_json, notify=True)
 
    resp = json_response(domain_json, status=201)
    log.response(request, resp=resp)
    return resp

async def DELETE_Domain(request):
    """HTTP DELETE method to delete a domain
    """
    log.request(request)
    app = request.app
    if not request.has_body:
        msg = "Expected body in delete domain"
        log.error(msg)
        raise HTTPInternalServerError() 
    body = await request.json() 
    domain = get_domain(request, body=body)

    log.info("delete domain: {}".format(domain))

    # raises exception if domain not found
    domain_json = await get_metadata_obj(app, domain)
    if domain_json:
        log.debug("got domain json")
    # delete domain
    await delete_metadata_obj(app, domain, notify=True)


    json_rsp = { "domain": domain }

    resp = json_response(json_rsp)
    log.response(request, resp=resp)
    return resp

async def PUT_ACL(request):
    """ Handler creating/update an ACL"""
    log.request(request)
    app = request.app
    acl_username = request.match_info.get('username')

    if not request.has_body:
        msg = "Expected body in delete domain"
        log.error(msg)
        raise HTTPInternalServerError() 
    body_json = await request.json() 

    domain = get_domain(request, body=body_json)

    log.info("put_acl - domain: {}, username:".format(domain, acl_username))

    # raises exception if domain not found
    domain_json = await get_metadata_obj(app, domain)

    if "acls" not in domain_json:
        log.error( "unexpected domain data for domain: {}".format(domain))
        raise HTTPInternalServerError() # 500 

    acl_keys = getAclKeys()
    acls = domain_json["acls"]
    acl = {}
    if acl_username in acls:
        acl = acls[acl_username]
    else:
        # initialize acl with no perms
        for k in acl_keys:
            acl[k] = False

    # replace any permissions given in the body
    for k in body_json.keys():
        acl[k] = body_json[k]

    # replace/insert the updated/new acl
    acls[acl_username] = acl
    
    # update the timestamp
    now = time.time()
    domain_json["lastModified"] = now
     
    # write back to S3
    await save_metadata_obj(app, domain, domain_json)
    
    resp_json = { } 
     
    resp = json_response(resp_json, status=201)
    log.response(request, resp=resp)
    return resp


   