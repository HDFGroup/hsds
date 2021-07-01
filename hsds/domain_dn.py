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

from .util.authUtil import getAclKeys
from .util.domainUtil import isValidDomain, getBucketForDomain
from .util.idUtil import validateInPartition
from .datanode_lib import get_metadata_obj, save_metadata_obj
from .datanode_lib import delete_metadata_obj, check_metadata_obj
from . import hsds_logger as log


def get_domain(request, body=None):
    """ Extract domain and validate """
    app = request.app
    params = request.rel_url.query

    domain = None
    if "domain" in params:
        domain = params["domain"]
        log.debug(f"got domain param: {domain}")
    elif body and "domain" in body:
        domain = body["domain"]

    if not domain:
        msg = "No domain provided"
        log.error(msg)
        raise HTTPInternalServerError()

    if not isValidDomain(domain):
        msg = f"Expected valid domain for [{domain}]"
        log.error(msg)
        raise HTTPInternalServerError()
    try:
        validateInPartition(app, domain)
    except KeyError:
        log.error(f"Domain {domain} not in partition")
        raise HTTPInternalServerError()
    return domain


async def GET_Domain(request):
    """HTTP GET method to return JSON for /domains/
    """
    log.request(request)
    app = request.app

    domain = get_domain(request)
    log.debug(f"get domain: {domain}")
    bucket = getBucketForDomain(domain)
    if not bucket:
        log.error(f"expected bucket to be used in domain: {domain}")
        raise HTTPInternalServerError()
    log.debug(f"using bucket: {bucket}")
    domain_json = await get_metadata_obj(app, domain)
    log.debug(f"returning domain_json: {domain_json}")

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
    log.debug("PUT_Domain, get request.json")
    if "Content-Type" not in request.headers:
        log.error("expected Content-Type in request headers")
        raise HTTPInternalServerError()
    content_type = request.headers["Content-Type"]
    if content_type != "application/json":
        msg = "PUT_Domain, expected json content-type but got: "
        msg += f"{content_type}"
        log.error(msg)
        raise HTTPInternalServerError()

    body = await request.json()
    log.debug(f"got body: {body}")

    domain = get_domain(request, body=body)

    log.debug(f"PUT domain: {domain}")
    bucket = getBucketForDomain(domain)
    if not bucket:
        log.error(f"expected bucket to be used in domain: {domain}")
        raise HTTPInternalServerError()

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

    domain_json = {}
    if "root" in body_json:
        domain_json["root"] = body_json["root"]
    else:
        log.info("no root id, creating folder")
    domain_json["owner"] = body_json["owner"]
    domain_json["acls"] = body_json["acls"]
    now = time.time()
    domain_json["created"] = now
    domain_json["lastModified"] = now

    # write the domain json to S3 immediately so it will show up in a get
    # domains S3 scan
    await save_metadata_obj(app, domain, domain_json, notify=True, flush=True)

    resp = json_response(domain_json, status=201)
    log.response(request, resp=resp)
    return resp


async def DELETE_Domain(request):
    """HTTP DELETE method to delete a domain
    """
    log.request(request)
    app = request.app
    domain = get_domain(request)
    bucket = getBucketForDomain(domain)

    log.info(f"delete domain: {domain}")

    # raises exception if domain not found
    if not bucket:
        log.error(f"expected bucket to be used in domain: {domain}")
        raise HTTPInternalServerError()
    log.debug(f"using bucket: {bucket}")

    domain_json = await get_metadata_obj(app, domain)
    if domain_json:
        log.debug("got domain json")
    # delete domain
    await delete_metadata_obj(app, domain, notify=True)

    json_rsp = {"domain": domain}

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

    log.info(f"put_acl - domain: {domain}, username: {acl_username}")

    # raises exception if domain not found
    domain_json = await get_metadata_obj(app, domain)

    if "acls" not in domain_json:
        log.error(f"unexpected domain data for domain: {domain}")
        raise HTTPInternalServerError()  # 500

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
    await save_metadata_obj(app, domain, domain_json, flush=True)

    resp_json = {}

    resp = json_response(resp_json, status=201)
    log.response(request, resp=resp)
    return resp
