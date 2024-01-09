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

from aiohttp.web_exceptions import HTTPBadRequest, HTTPNotFound, HTTPConflict
from aiohttp.web_exceptions import HTTPInternalServerError
from aiohttp.web import json_response

from .util.idUtil import isValidUuid
from .util.linkUtil import validateLinkName
from .datanode_lib import get_obj_id, get_metadata_obj, save_metadata_obj
from . import hsds_logger as log


def _index(items, marker, create_order=False):
    """Locate the leftmost value exactly equal to x"""
    if create_order:
        # list is not ordered, juse search linearly
        for i in range(len(items)):
            if items[i] == marker:
                return i
    else:
        i = bisect_left(items, marker)
        if i != len(items) and items[i] == marker:
            return i
    # not found
    return -1


async def GET_Links(request):
    """HTTP GET method to return JSON for a link collection"""
    log.request(request)
    app = request.app
    params = request.rel_url.query
    group_id = get_obj_id(request)
    log.info(f"GET links: {group_id}")
    if not isValidUuid(group_id, obj_class="group"):
        log.error(f"Unexpected group_id: {group_id}")
        raise HTTPInternalServerError()

    create_order = False
    if "CreateOrder" in params and params["CreateOrder"]:
        create_order = True

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

    bucket = None
    if "bucket" in params:
        bucket = params["bucket"]

    if not bucket:
        msg = "GET_Links - no bucket param"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    group_json = await get_metadata_obj(app, group_id, bucket=bucket)

    log.debug(f"for id: {group_id} got group json: {group_json}")
    if "links" not in group_json:
        msg.error(f"unexpected group data for id: {group_id}")
        raise HTTPInternalServerError()

    # return a list of links based on sorted dictionary keys
    link_dict = group_json["links"]

    titles = []
    if create_order:
        order_dict = {}
        for title in link_dict:
            item = link_dict[title]
            if "created" not in item:
                log.warning(f"expected to find 'created' key in link item {title}")
                continue
            order_dict[title] = item["created"]
        log.debug(f"order_dict: {order_dict}")
        # now sort by created
        for k in sorted(order_dict.items(), key=lambda item: item[1]):
            titles.append(k[0])
        log.debug(f"links by create order: {titles}")
    else:
        titles = list(link_dict.keys())
        titles.sort()  # sort by key
        log.debug(f"links by lexographic order: {titles}")

    start_index = 0
    if marker is not None:
        start_index = _index(titles, marker, create_order=create_order) + 1
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
        log.debug(f"link list[{i}: {link}")
        link["title"] = title
        link_list.append(link)

    resp_json = {"links": link_list}
    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp


async def POST_Links(request):
    """HTTP POST method to return JSON for a link or a given set of links """
    log.request(request)
    app = request.app
    params = request.rel_url.query
    group_id = get_obj_id(request)
    log.info(f"POST_Links: {group_id}")

    if not isValidUuid(group_id, obj_class="group"):
        log.error(f"Unexpected group_id: {group_id}")
        raise HTTPInternalServerError()

    body = await request.json()
    if "titles" not in body:
        msg = f"POST_Links expected titles in body but got: {body.keys()}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    titles = body["titles"]  # list of link names to fetch

    for title in titles:
        validateLinkName(title)

    bucket = None
    if "bucket" in params:
        bucket = params["bucket"]

    if not bucket:
        msg = "POST_Links - no bucket param"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    group_json = await get_metadata_obj(app, group_id, bucket=bucket)
    log.info(f"for id: {group_id} got group json: {group_json}")

    if "links" not in group_json:
        log.error(f"unexpected group data for id: {group_id}")
        raise HTTPInternalServerError()

    links = group_json["links"]

    link_list = []  # links to be returned

    for title in titles:
        if title not in links:
            log.info(f"Link name {title} not found in group: {group_id}")
            raise HTTPNotFound()
        link_json = links[title]
        item = {}
        if "class" not in link_json:
            log.warn(f"expected to find class key for link: {title}")
            continue
        link_class = link_json["class"]
        item["class"] = link_class
        if "created" not in link_json:
            log.warn(f"expected to find created time for link: {title}")
            link_created = 0
        else:
            link_created = link_json["created"]
        item["created"] = link_created
        if link_class == "H5L_TYPE_HARD":
            if "id" not in link_json:
                log.warn(f"expected to id for hard linK: {title}")
                continue
            item["id"] = link_json["id"]
        elif link_class == "H5L_TYPE_SOFT":
            if "h5path" not in link_json:
                log.warn(f"expected to find h5path for soft link: {title}")
                continue
            item["h5path"] = link_json["h5path"]
        elif link_class == "H5L_TYPE_EXTERNAL":
            if "h5path" not in link_json:
                log.warn(f"expected to find h5path for external link: {title}")
                continue
            item["h5path"] = link_json["h5path"]
            if "h5domain" not in link_json:
                log.warn(f"expted to find h5domain for external link: {title}")
                continue
            item["h5domain"] = link_json["h5domain"]
        else:
            log.warn(f"unexpected to link class {link_class} for link: {title}")
            continue

        item["title"] = title

        link_list.append(item)

    if not link_list:
        msg = f"POST_links - requested {len(titles)} but none were found"
        log.warn(msg)
        raise HTTPNotFound()

    if len(link_list) != len(titles):
        msg = f"POST_links - requested {len(titles)} links but only "
        msg += f"{len(link_list)} were found"
        log.warn(msg)
        raise HTTPNotFound()

    rspJson = {"links": link_list}
    resp = json_response(rspJson)
    log.response(request, resp=resp)
    return resp


async def PUT_Links(request):
    """Handler creating new links """
    log.request(request)
    app = request.app
    params = request.rel_url.query
    group_id = get_obj_id(request)
    log.info(f"PUT links: {group_id}")

    if not isValidUuid(group_id, obj_class="group"):
        log.error(f"Unexpected group_id: {group_id}")
        raise HTTPInternalServerError()

    if not request.has_body:
        msg = "PUT_Links with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    body = await request.json()

    if "links" not in body:
        msg = "PUT_Links with no links key in body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    items = body["links"]

    # validate input
    for title in items:
        validateLinkName(title)
        item = items[title]

        if "id" in item:
            if not isValidUuid(item["id"]):
                msg = f"invalid uuid for {title}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

    if "bucket" in params:
        bucket = params["bucket"]
    elif "bucket" in body:
        bucket = params["bucket"]
    else:
        bucket = None

    if not bucket:
        msg = "PUT_Links - no bucket provided"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    group_json = await get_metadata_obj(app, group_id, bucket=bucket)
    if "links" not in group_json:
        log.error(f"unexpected group data for id: {group_id}")
        raise HTTPInternalServerError()

    links = group_json["links"]
    dup_titles = []
    for title in items:
        if title in links:
            link_json = items[title]
            existing_link = links[title]
            for prop in ("class", "id", "h5path", "h5domain"):
                if prop in link_json:
                    if prop not in existing_link:
                        msg = f"PUT Link - prop {prop} not found in existing "
                        msg += "link, returning 409"
                        log.warn(msg)
                        raise HTTPConflict()

                    if link_json[prop] != existing_link[prop]:
                        msg = f"PUT Links - prop {prop} value is different, old: "
                        msg += f"{existing_link[prop]}, new: {link_json[prop]}, "
                        msg += "returning 409"
                        log.warn(msg)
                        raise HTTPConflict()
            msg = f"Link name {title} already found in group: {group_id}"
            log.warn(msg)
            dup_titles.append(title)

    for title in dup_titles:
        del items[title]

    if items:

        now = time.time()

        # add the links
        for title in items:
            item = items[title]
            item["created"] = now
            links[title] = item

        # update the group lastModified
        group_json["lastModified"] = now

        # write back to S3, save to metadata cache
        await save_metadata_obj(app, group_id, group_json, bucket=bucket)

        status = 201
    else:
        # nothing to update
        status = 200

    # put the status in the JSON response since the http_put function
    # used the the SN won't return it
    resp_json = {"status": status}

    resp = json_response(resp_json, status=status)
    log.response(request, resp=resp)
    return resp


async def DELETE_Links(request):
    """HTTP DELETE method for group links"""
    log.request(request)
    app = request.app
    params = request.rel_url.query
    group_id = get_obj_id(request)
    log.info(f"DELETE links: {group_id}")

    if not isValidUuid(group_id, obj_class="group"):
        msg = f"Unexpected group_id: {group_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if "separator" in params:
        separator = params["separator"]
    else:
        separator = "/"

    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = None

    if not bucket:
        msg = "DELETE_Links - no bucket param"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if "titles" not in params:
        msg = "expected titles for DELETE links"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    titles_param = params["titles"]

    titles = titles_param.split(separator)

    log.info(f"DELETE links {titles} in {group_id} bucket: {bucket}")

    group_json = await get_metadata_obj(app, group_id, bucket=bucket)

    if "links" not in group_json:
        log.error(f"unexpected group data for id: {group_id}")
        raise HTTPInternalServerError()

    links = group_json["links"]

    for title in titles:
        if title not in links:
            msg = f"Link name {title} not found in group: {group_id}"
            log.warn(msg)
            raise HTTPNotFound()

    # now delete the links
    for title in titles:
        del links[title]  # remove the link from dictionary

    # update the group lastModified
    now = time.time()
    group_json["lastModified"] = now

    # write back to S3
    await save_metadata_obj(app, group_id, group_json, bucket=bucket)

    resp_json = {}

    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp
