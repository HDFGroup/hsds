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

from aiohttp.errors import HttpBadRequest, HttpProcessingError 
 
from util.idUtil import  isValidUuid
from util.httpUtil import jsonResponse
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
    group_id = get_obj_id(request)  
    log.info("GET links: {}".format(group_id))
    if not isValidUuid(group_id, obj_class="group"):
        log.error( "Unexpected group_id: {}".format(group_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")
 
    limit = None
    if "Limit" in request.GET:
        try:
            limit = int(request.GET["Limit"])
            log.info("GET_Links - using Limit: {}".format(limit))
        except ValueError:
            msg = "Bad Request: Expected int type for limit"
            log.error(msg)  # should be validated by SN
            raise HttpBadRequest(message=msg)
    marker = None
    if "Marker" in request.GET:
        marker = request.GET["Marker"]
        log.info("GET_Links - using Marker: {}".format(marker))
     
    group_json = await get_metadata_obj(app, group_id)
    
    log.info("for id: {} got group json: {}".format(group_id, str(group_json)))
    if "links" not in group_json:
        msg.error("unexpected group data for id: {}".format(group_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

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
            msg = "Link marker: {}, not found".format(marker)
            log.warn(msg)
            raise HttpProcessingError(code=404, message=msg)

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
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp    

async def GET_Link(request):
    """HTTP GET method to return JSON for a link
    """
    log.request(request)
    app = request.app
    group_id = get_obj_id(request)
    log.info("GET link: {}".format(group_id))

    if not isValidUuid(group_id, obj_class="group"):
        log.error( "Unexpected group_id: {}".format(group_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    link_title = request.match_info.get('title')

    validateLinkName(link_title)

    group_json = await get_metadata_obj(app, group_id)
    log.info("for id: {} got group json: {}".format(group_id, str(group_json)))
    if "links" not in group_json:
        log.error("unexpected group data for id: {}".format(group_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    links = group_json["links"]
    if link_title not in links:
        log.warn("Link name {} not found in group: {}".format(link_title, group_id))
        raise HttpProcessingError(code=404, message="Unexpected Error")

    link_json = links[link_title]
     
    resp = await jsonResponse(request, link_json)
    log.response(request, resp=resp)
    return resp

async def PUT_Link(request):
    """ Handler creating a new link"""
    log.request(request)
    app = request.app
    group_id = get_obj_id(request)
    log.info("PUT link: {}".format(group_id))
    if not isValidUuid(group_id, obj_class="group"):
        log.error( "Unexpected group_id: {}".format(group_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    link_title = request.match_info.get('title')
    validateLinkName(link_title)

    log.info("link_title: {}".format(link_title))

    if not request.has_body:
        msg = "PUT Link with no body"
        log.error(msg)
        raise HttpBadRequest(message=msg)

    body = await request.json()   
    
    if "class" not in body: 
        log.error("Expected class in PUT Link")
        raise HttpProcessingError(code=500, message="Unexpected Error")
    link_class = body["class"]
     
    link_json = {}
    link_json["class"] = link_class

    if "id" in body:
        link_json["id"] = body["id"]    
    if "h5path" in body:    
        link_json["h5path"] = body["h5path"]
    if "h5domain" in body:
        link_json["h5domain"] = body["h5domain"]

    group_json = await get_metadata_obj(app, group_id)
    if "links" not in group_json:
        log.error( "unexpected group data for id: {}".format(group_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    links = group_json["links"]
    if link_title in links:
        msg = "Link name {} already found in group: {}".format(link_title, group_id)
        log.warn(msg)
        raise HttpProcessingError(code=409, message=msg)
    
    now = time.time()
    link_json["created"] = now

    # add the link
    links[link_title] = link_json

    # update the group lastModified
    group_json["lastModified"] = now

    # write back to S3, save to metadata cache
    save_metadata_obj(app, group_id, group_json)
    
    resp_json = { } 
     
    resp = await jsonResponse(request, resp_json, status=201)
    log.response(request, resp=resp)
    return resp


async def DELETE_Link(request):
    """HTTP DELETE method for group links
    """
    log.request(request)
    app = request.app
    group_id = get_obj_id(request)
    log.info("DELETE link: {}".format(group_id))

    if not isValidUuid(group_id, obj_class="group"):
        log.error( "Unexpected group_id: {}".format(group_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")
 
    link_title = request.match_info.get('title')
    validateLinkName(link_title)

    group_json = await get_metadata_obj(app, group_id)
    # TBD: Possible race condition
    if "links" not in group_json:
        log.error("unexpected group data for id: {}".format(group_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    links = group_json["links"]
    if link_title not in links:
        msg = "Link name {} not found in group: {}".format(link_title, group_id)
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)

    del links[link_title]  # remove the link from dictionary

    # update the group lastModified
    now = time.time()
    group_json["lastModified"] = now

    # write back to S3
    save_metadata_obj(app, group_id, group_json)

    hrefs = []  # TBD
    resp_json = {"href":  hrefs} 
     
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp
   
