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
from aiohttp.errors import  HttpProcessingError
from util.authUtil import  getAclKeys
from util.httpUtil import  jsonResponse
from util.domainUtil import isValidDomain
from util.idUtil import validateInPartition
from datanode_lib import get_metadata_obj, save_metadata_obj, delete_metadata_obj
import hsds_logger as log

def get_domain(request, body=None):
    """ Extract domain and validate """
    app = request.app

    domain = None
    if "domain" in request.GET:
        domain = request.GET["domain"]
        log.info("got domain param: {}".format(domain))
    elif request.has_body:
        if "domain" in body:
            domain = body["domain"]
             
    if not domain: 
        msg = "No domain provided"  
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg) 

    if not isValidDomain(domain):
        msg = "Expected valid domain"
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg) 
    try:
        validateInPartition(app, domain)
    except KeyError as ke:
        msg = "Domain not in partition"
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg)
    return domain

async def GET_Domain(request):
    """HTTP GET method to return JSON for /domains/
    """
    log.request(request)
    app = request.app

    domain = get_domain(request)
    log.info("get domain: {}".format(domain))
    domain_json = await get_metadata_obj(app, domain)

    resp = await jsonResponse(request, domain_json)
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
        raise HttpProcessingError(code=500, message=msg) 
    body = await request.json() 

    domain = get_domain(request, body=body)
 
    log.info("PUT domain: {}".format(domain))
 
    body_json = await request.json()
    if "owner" not in body_json:
        msg = "Expected Owner Key in Body"
        log.warn(msg)
        raise HttpProcessingError(code=500, message=msg) 
    if "acls" not in body_json:
        msg = "Expected Owner Key in Body"
        log.warn(msg)
        raise HttpProcessingError(code=500, message=msg) 

    # try getting the domain, should raise 404
    domain_json = None
    try:
        domain_json = await get_metadata_obj(app, domain)
    except HttpProcessingError as hpe:
        if hpe.code in (404, 410):
            pass # Expected
        else:
            msg = "Unexpected error"
            log.error(msg)
            raise HttpProcessingError(code=500, message=msg)

    if domain_json != None:
        # domain already exists
        msg = "Conflict: resource exists: " + domain
        log.info(msg)
        raise HttpProcessingError(code=409, message=msg)

          
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

    save_metadata_obj(app, domain, domain_json)
 
    resp = await jsonResponse(request, domain_json, status=201)
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
        raise HttpProcessingError(code=500, message=msg) 
    body = await request.json() 
    domain = get_domain(request, body=body)

    log.info("delete domain: {}".format(domain))

    # raises exception if domain not found
    domain_json = await get_metadata_obj(app, domain)
    if domain_json:
        log.info("got domain json")
    # delete domain
    notify=True
    if "Notify" in request.GET and not request.GET["Notify"]:
        notify=False
    await delete_metadata_obj(app, domain, notify=notify)

 
    json_response = { "domain": domain }

    resp = await jsonResponse(request, json_response, status=200)
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
        raise HttpProcessingError(code=500, message=msg) 
    body = await request.json() 

    domain = get_domain(request, body=body)

    if not request.has_body:
        msg = "Expected Body to be in request"
        log.warn(msg)
        raise HttpProcessingError(code=500, message=msg) 

    body_json = await request.json()

    log.info("put_acl - domain: {}, username:".format(domain, acl_username))

    # raises exception if domain not found
    domain_json = await get_metadata_obj(app, domain)

    if "acls" not in domain_json:
        log.error( "unexpected domain data for domain: {}".format(domain))
        raise HttpProcessingError(code=500, message="Unexpected Error")

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
    save_metadata_obj(app, domain, domain_json)
    
    resp_json = { } 
     
    resp = await jsonResponse(request, resp_json, status=201)
    log.response(request, resp=resp)
    return resp


   