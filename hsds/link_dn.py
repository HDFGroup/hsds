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

from aiohttp import HttpProcessingError 
from aiohttp.errors import HttpBadRequest 
 
from util.idUtil import  validateUuid
from util.httpUtil import jsonResponse
from util.linkUtil import validateLinkName
from datanode_lib import get_metadata_obj, save_metadata_obj
import hsds_logger as log
    

async def GET_Link(request):
    """HTTP GET method to return JSON for a link
    """
    log.request(request)
    app = request.app
    group_id = request.match_info.get('id')
    validateUuid(group_id, "group")
    link_title = request.match_info.get('title')
    validateLinkName(link_title)

    group_json = await get_metadata_obj(app, group_id)
    if "links" not in group_json:
        msg = "unexpected group data for id: {}".format(group_id)
        msg.error(msg)
        raise HttpProcessingError(code=500, message=msg)

    links = group_json["links"]
    if link_title not in links:
        msg = "Link name {} not found in group: {}".format(link_title, group_id)
        msg.error(msg)
        raise HttpProcessingError(code=404, message=msg)

    link_json = links[link_title]
     
    resp = await jsonResponse(request, link_json)
    log.response(request, resp=resp)
    return resp

async def PUT_Link(request):
    """ Handler creating a new link"""
    log.request(request)
    app = request.app
    group_id = request.match_info.get('id')
    validateUuid(group_id, "group")

    if not request.has_body:
        msg = "PUT Link with no body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    body = await request.json()   
    
    if "class" not in body: 
        log.error("Expected class in PUT Link")
        raise HttpProcessingError(code=500, message="Unexpected Error")
    link_class = body["class"]
    if "title" not in body:
        log.error("Expected title in PUT Link")
        raise HttpProcessingError(code=500, message="Unexpected Error")
    link_title = body["title"]

    now = int(time.time())
    link_json = {}
    link_json["class"] = link_class
    link_json["title"] = link_title
    link_json["created"] = now
    link_json["lastModified"] = now


    if "id" in body:
        link_json["id"] = body["id"]    
    if "h5path" in body:    
        link_json["h5path"] = body["h5path"]
    if "h5domain" in body:
        link_json = body["h5domain"]

    group_json = await get_metadata_obj(app, group_id)
    if "links" not in group_json:
        msg = "unexpected group data for id: {}".format(group_id)
        msg.error(msg)
        raise HttpProcessingError(code=500, message=msg)

    links = group_json["links"]
    if link_title in links:
        msg = "Link name {} already found in group: {}".format(link_title, group_id)
        msg.error(msg)
        raise HttpProcessingError(code=404, message=msg)
    
    # add the link
    links[link_title] = body

    # write back to S3
    save_metadata_obj(app, group_json)
    
    hrefs = []  # TBD
    resp_json = {"href":  hrefs} 
     
    resp = await jsonResponse(request, resp_json, status=201)
    log.response(request, resp=resp)
    return resp


async def DELETE_Link(request):
    """HTTP DELETE method for group links
    """
    log.request(request)
    app = request.app
    group_id = request.match_info.get('id')
    validateUuid(group_id, "group")
    link_title = request.match_info.get('title')
    validateLinkName(link_title)

    group_json = await get_metadata_obj(app, group_id)
    if "links" not in group_json:
        msg = "unexpected group data for id: {}".format(group_id)
        msg.error(msg)
        raise HttpProcessingError(code=500, message=msg)

    links = group_json["links"]
    if link_title not in links:
        msg = "Link name {} not found in group: {}".format(link_title, group_id)
        msg.error(msg)
        raise HttpProcessingError(code=404, message=msg)

    del links[link_title]  # remove the link from dictionary

    # write back to S3
    save_metadata_obj(app, group_json)

    hrefs = []  # TBD
    resp_json = {"href":  hrefs} 
     
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp
   