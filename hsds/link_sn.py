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
# service node of hsds cluster
#

from aiohttp.web_exceptions import HTTPBadRequest
from json import JSONDecodeError

from .util.httpUtil import http_get, getHref
from .util.httpUtil import jsonResponse
from .util.idUtil import isValidUuid, getDataNodeUrl, getCollectionForId
from .util.authUtil import getUserPasswordFromRequest, validateUserPassword
from .util.domainUtil import getDomainFromRequest, isValidDomain, verifyRoot
from .util.domainUtil import getBucketForDomain
from .util.linkUtil import validateLinkName
from .servicenode_lib import getDomainJson, validateAction
from .servicenode_lib import getLink, putLink, getLinks, deleteLinks
from .domain_crawl import DomainCrawler
from . import hsds_logger as log


async def GET_Links(request):
    """HTTP method to return JSON for link collection"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

    group_id = request.match_info.get("id")

    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(group_id, obj_class="Group"):
        msg = f"Invalid group id: {group_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    limit = None
    create_order = False
    if "CreateOrder" in params and params["CreateOrder"]:
        create_order = True

    if "Limit" in params:
        try:
            limit = int(params["Limit"])
        except ValueError:
            msg = "Bad Request: Expected int type for limit"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    marker = None
    if "Marker" in params:
        marker = params["Marker"]

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app["allow_noauth"]:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    await validateAction(app, domain, group_id, username, "read")

    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id + "/links"

    params = {}
    if create_order:
        params["CreateOrder"] = 1
    if limit is not None:
        params["Limit"] = str(limit)
    if marker is not None:
        params["Marker"] = marker
    if bucket:
        params["bucket"] = bucket
    links_json = await http_get(app, req, params=params)
    log.debug(f"got links json from dn for group_id: {group_id}")
    links = links_json["links"]

    # mix in collection key, target and hrefs
    for link in links:
        if link["class"] == "H5L_TYPE_HARD":
            collection_name = getCollectionForId(link["id"])
            link["collection"] = collection_name
            target_uri = "/" + collection_name + "/" + link["id"]
            link["target"] = getHref(request, target_uri)
        link_uri = "/groups/" + group_id + "/links/" + link["title"]
        link["href"] = getHref(request, link_uri)

    resp_json = {}
    resp_json["links"] = links
    hrefs = []
    group_uri = "/groups/" + group_id
    href = getHref(request, group_uri + "/links")
    hrefs.append({"rel": "self", "href": href})
    href = getHref(request, "/")
    hrefs.append({"rel": "home", "href": href})
    href = getHref(request, group_uri)
    hrefs.append({"rel": "owner", "href": href})
    resp_json["hrefs"] = hrefs

    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp


async def GET_Link(request):
    """HTTP method to return JSON for a group link"""
    log.request(request)
    app = request.app

    group_id = request.match_info.get("id")
    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(group_id, obj_class="Group"):
        msg = f"Invalid group id: {group_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    link_title = request.match_info.get("title")
    validateLinkName(link_title)

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app["allow_noauth"]:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    await validateAction(app, domain, group_id, username, "read")

    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id + "/links"
    log.debug("get LINK: " + req)
    params = {}
    if bucket:
        params["bucket"] = bucket

    link_json = await getLink(app, group_id, link_title, bucket=bucket)

    resp_link = {}
    resp_link["title"] = link_title
    link_class = link_json["class"]
    resp_link["class"] = link_class
    if link_class == "H5L_TYPE_HARD":
        resp_link["id"] = link_json["id"]
        resp_link["collection"] = getCollectionForId(link_json["id"])
    elif link_class == "H5L_TYPE_SOFT":
        resp_link["h5path"] = link_json["h5path"]
    elif link_class == "H5L_TYPE_EXTERNAL":
        resp_link["h5path"] = link_json["h5path"]
        resp_link["h5domain"] = link_json["h5domain"]
    else:
        log.warn(f"Unexpected link class: {link_class}")
    resp_json = {}
    resp_json["link"] = resp_link
    resp_json["created"] = link_json["created"]
    # links don't get modified, so use created timestamp as lastModified
    resp_json["lastModified"] = link_json["created"]

    hrefs = []
    group_uri = "/groups/" + group_id
    href = getHref(request, f"{group_uri}/links/{link_title}")
    hrefs.append({"rel": "self", "href": href})
    href = getHref(request, "/")
    hrefs.append({"rel": "home", "href": href})
    href = getHref(request, group_uri)
    hrefs.append({"rel": "owner", "href": href})
    if link_json["class"] == "H5L_TYPE_HARD":
        target = "/" + resp_link["collection"] + "/" + resp_link["id"]
        href = getHref(request, target)
        hrefs.append({"rel": "target", "href": href})

    resp_json["hrefs"] = hrefs

    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp


async def PUT_Link(request):
    """HTTP method to create a new link"""
    log.request(request)
    app = request.app

    group_id = request.match_info.get("id")
    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    link_title = request.match_info.get("title")
    log.info(f"PUT Link_title: [{link_title}]")

    username, pswd = getUserPasswordFromRequest(request)
    # write actions need auth
    await validateUserPassword(app, username, pswd)

    if not request.has_body:
        msg = "PUT Link with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    try:
        body = await request.json()
    except JSONDecodeError:
        msg = "Unable to load JSON body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    await validateAction(app, domain, group_id, username, "create")
    # putLink will validate these arguments
    kwargs = {"bucket": bucket}
    kwargs["tgt_id"] = body.get("id")
    kwargs["h5path"] = body.get("h5path")
    kwargs["h5domain"] = body.get("h5domain")

    status = await putLink(app, group_id, link_title, **kwargs)

    hrefs = []  # TBD
    req_rsp = {"hrefs": hrefs}
    # link creation successful
    # returns 201 if new link was created, 200 if this is a duplicate
    # of an existing link
    resp = await jsonResponse(request, req_rsp, status=status)
    log.response(request, resp=resp)
    return resp


async def DELETE_Links(request):
    """HTTP method to delete multiple links """
    log.request(request)
    app = request.app
    params = request.rel_url.query
    group_id = request.match_info.get("id")
    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(group_id, obj_class="Group"):
        msg = f"Invalid group id: {group_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if "titles" not in params:
        msg = "expected titles params"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    titles_param = params["titles"]
    if "separator" in params:
        separator = params["separator"]
    else:
        separator = "/"
    titles = titles_param.split(separator)

    for title in titles:
        validateLinkName(title)

    username, pswd = getUserPasswordFromRequest(request)
    await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    bucket = getBucketForDomain(domain)

    await validateAction(app, domain, group_id, username, "delete")

    await deleteLinks(app, group_id, titles=titles, bucket=bucket)

    rsp_json = {}
    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp


async def POST_Links(request):
    """HTTP method to get multiple links """
    log.request(request)
    app = request.app
    params = request.rel_url.query
    log.info("POST_Links")
    req_id = request.match_info.get("id")

    if params.get("follow_links"):
        follow_links = True
    else:
        follow_links = False

    if not request.has_body:
        msg = "POST Links with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    try:
        body = await request.json()
    except JSONDecodeError:
        msg = "Unable to load JSON body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if "titles" in body:
        titles = body["titles"]
        if not isinstance(titles, list):
            msg = f"expected list for titles but got: {type(titles)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        titles = None

    if "group_ids" in body:
        group_ids = body["group_ids"]
    else:
        group_ids = None

    if titles is None and group_ids is None:
        msg = "expected body to contain one of titles, group_ids keys"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    # construct an item list from titles and group_ids
    items = {}
    if group_ids is None:
        if not req_id:
            msg = "no object id in request"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        items[req_id] = titles
    elif isinstance(group_ids, list):
        if titles is None:
            msg = "no titles - will return all links for each object"
            log.debug(msg)
        for group_id in group_ids:
            items[group_id] = None
    elif isinstance(group_ids, dict):
        if titles is not None:
            msg = "titles must not be provided if obj_ids is a dict"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        for group_id in group_ids:
            names_for_id = group_ids[group_id]
            if not isinstance(names_for_id, list):
                msg = "expected list of titles"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            items[group_id] = names_for_id

    log.debug(f"POST Links items: {items}")

    # do a check that everything is as it should with the item list
    for group_id in items:
        if not isValidUuid(group_id, obj_class="Group"):
            msg = f"Invalid group id: {group_id}"
            log.warn(msg)

        titles = items[group_id]

        if titles is None:
            log.debug(f"getting all links for {group_id}")
        elif isinstance(titles, list):
            for title in titles:
                validateLinkName(title)  # raises HTTPBadRequest if invalid
        else:
            msg = f"expected list for titles but got: {type(titles)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app["allow_noauth"]:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain value: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    verifyRoot(domain_json)

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, req_id, username, "read")

    resp_json = {}

    if len(items) == 0:
        msg = "no group ids specified for POST Links"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    elif len(items) == 1 and not follow_links:
        # just make a request to the datanode
        group_id = list(items.keys())[0]
        titles = items[group_id]
        links = await getLinks(app, group_id, titles=titles, bucket=bucket)

        resp_json["links"] = links
    else:
        # Use DomainCrawler to fetch links from multiple object.
        # set the follow_links and bucket params
        crawler_params = {"follow_links": follow_links, "bucket": bucket}

        kwargs = {"action": "get_link", "raise_error": True, "params": crawler_params}
        crawler = DomainCrawler(app, items, **kwargs)
        # will raise exception on NotFound, etc.
        await crawler.crawl()

        msg = f"DomainCrawler returned: {len(crawler._obj_dict)} objects"
        log.info(msg)
        links = crawler._obj_dict

        log.debug(f"got {len(links)} links")
        resp_json["links"] = links

    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp


async def DELETE_Link(request):
    """HTTP method to delete one or more links """
    log.request(request)
    app = request.app

    group_id = request.match_info.get("id")
    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(group_id, obj_class="Group"):
        msg = f"Invalid group id: {group_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    link_title = request.match_info.get("title")
    validateLinkName(link_title)

    username, pswd = getUserPasswordFromRequest(request)
    await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    bucket = getBucketForDomain(domain)

    await validateAction(app, domain, group_id, username, "delete")

    await deleteLinks(app, group_id, titles=[link_title, ], bucket=bucket)

    rsp_json = {}
    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp
