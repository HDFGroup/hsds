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
from copy import copy
from bisect import bisect_left

from aiohttp.web_exceptions import HTTPBadRequest, HTTPNotFound, HTTPConflict, HTTPInternalServerError
from aiohttp.web import json_response

 
from util.idUtil import  isValidUuid
from util.linkUtil import validateLinkName
from datanode_lib import get_obj_id, get_metadata_obj, save_metadata_obj
import hsds_logger as log

def index(a, x):
    """ Locate the leftmost value exactly equal to x
    """
    i = bisect_left(a, x)
    if i != len(a) and a[i] == x:
        return i
    return -1

async def GET_Links(request):
    """HTTP GET method to return JSON for a link collection
    """
    log.request(request)
    app = request.app
    params = request.rel_url.query
    group_id = get_obj_id(request)  
    log.info(f"GET links: {group_id}")
    if not isValidUuid(group_id, obj_class="group"):
        log.error(f"Unexpected group_id: {group_id}")
        raise HTTPInternalServerError()
 
    limit = None
    if "Limit" in params:
        try:
            limit = int(params["Limit"])
            log.info(f"GET_Links - using Limit: {limit}")
        except ValueError:
            msg = "Bad Request: Expected int type for limit"
            log.error(msg)  # should be validated by SN
            raise HTTPBadRequest(reason=msg)
    marker = None
    if "Marker" in params:
        marker = params["Marker"]
        log.info(f"GET_Links - using Marker: {marker}")

    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = None
     
    group_json = await get_metadata_obj(app, group_id, bucket=bucket)
    
    log.info(f"for id: {group_id} got group json: {group_json}")
    if "links" not in group_json:
        msg.error(f"unexpected group data for id: {group_id}")
        raise HTTPInternalServerError()

    # return a list of links based on sorted dictionary keys
    link_dict = group_json["links"]
    titles = list(link_dict.keys())
    titles.sort()  # sort by key 
    # TBD: provide an option to sort by create date

    start_index = 0
    if marker is not None:
        start_index = index(titles, marker) + 1
        if start_index == 0:
            # marker not found, return 404
            msg = f"Link marker: {marker}, not found"
            log.warn(msg)
            raise HTTPNotFound()

    end_index = len(titles) 
    if limit is not None and (end_index - start_index) > limit:
        end_index = start_index + limit
    
    link_list = []
    for i in range(start_index, end_index):
        title = titles[i]
        link = copy(link_dict[title])
        link["title"] = title
        link_list.append(link)

    resp_json = {"links": link_list} 
    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp    

async def GET_Link(request):
    """HTTP GET method to return JSON for a link
    """
    log.request(request)
    app = request.app
    params = request.rel_url.query
    group_id = get_obj_id(request)
    log.info(f"GET link: {group_id}")

    if not isValidUuid(group_id, obj_class="group"):
        log.error(f"Unexpected group_id: {group_id}")
        raise HTTPInternalServerError()

    link_title = request.match_info.get('title')

    validateLinkName(link_title)

    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = None

    group_json = await get_metadata_obj(app, group_id, bucket=bucket)
    log.info(f"for id: {group_id} got group json: {group_json}")
    if "links" not in group_json:
        log.error(f"unexpected group data for id: {group_id}")
        raise HTTPInternalServerError()

    links = group_json["links"]
    if link_title not in links:
        log.warn(f"Link name {link_title} not found in group: {group_id}")
        raise HTTPNotFound()

    link_json = links[link_title]
     
    resp = json_response(link_json)
    log.response(request, resp=resp)
    return resp

async def PUT_Link(request):
    """ Handler creating a new link"""
    log.request(request)
    app = request.app
    params = request.rel_url.query
    group_id = get_obj_id(request)
    log.info(f"PUT link: {group_id}")
    if not isValidUuid(group_id, obj_class="group"):
        log.error(f"Unexpected group_id: {group_id}")
        raise HTTPInternalServerError()

    link_title = request.match_info.get('title')
    validateLinkName(link_title)

    log.info(f"link_title: {link_title}")

    if not request.has_body:
        msg = "PUT Link with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    body = await request.json()   
    
    if "class" not in body: 
        msg = "PUT Link with no class key body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    link_class = body["class"]
     
    link_json = {}
    link_json["class"] = link_class

    if "id" in body:
        link_json["id"] = body["id"]    
    if "h5path" in body:    
        link_json["h5path"] = body["h5path"]
    if "h5domain" in body:
        link_json["h5domain"] = body["h5domain"]

    if "bucket" in params:
        bucket = params["bucket"]
    elif "bucket" in body:
        bucket = params["bucket"]
    else:
        bucket = None

    group_json = await get_metadata_obj(app, group_id, bucket=bucket)
    if "links" not in group_json:
        log.error(f"unexpected group data for id: {group_id}")
        raise HTTPInternalServerError()

    links = group_json["links"]
    if link_title in links:
        msg = f"Link name {link_title} already found in group: {group_id}"
        log.warn(msg)
        raise HTTPConflict()
    
    now = time.time()
    link_json["created"] = now

    # add the link
    links[link_title] = link_json

    # update the group lastModified
    group_json["lastModified"] = now

    # write back to S3, save to metadata cache
    await save_metadata_obj(app, group_id, group_json, bucket=bucket)
    
    resp_json = { } 
     
    resp = json_response(resp_json, status=201)
    log.response(request, resp=resp)
    return resp


async def DELETE_Link(request):
    """HTTP DELETE method for group links
    """
    log.request(request)
    app = request.app
    params = request.rel_url.query
    group_id = get_obj_id(request)
    log.info(f"DELETE link: {group_id}")

    if not isValidUuid(group_id, obj_class="group"):
        msg = f"Unexpected group_id: {group_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
 
    link_title = request.match_info.get('title')
    validateLinkName(link_title)

    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = None

    group_json = await get_metadata_obj(app, group_id, bucket=bucket)
    # TBD: Possible race condition
    if "links" not in group_json:
        log.error(f"unexpected group data for id: {group_id}")
        raise HTTPInternalServerError()

    links = group_json["links"]
    if link_title not in links:
        msg = f"Link name {link_title} not found in group: {group_id}"
        log.warn(msg)
        raise HTTPNotFound()

    del links[link_title]  # remove the link from dictionary

    # update the group lastModified
    now = time.time()
    group_json["lastModified"] = now

    # write back to S3
    await save_metadata_obj(app, group_id, group_json, bucket=bucket)

    hrefs = []  # TBD
    resp_json = {"href":  hrefs} 
     
    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp
