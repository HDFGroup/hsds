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

from h5json.objid import isValidUuid, getCollectionForId

from .util.nodeUtil import getDataNodeUrl
from .util.httpUtil import getHref, getBooleanParam
from .util.httpUtil import jsonResponse
from .util.globparser import globmatch
from .util.authUtil import getUserPasswordFromRequest, validateUserPassword
from .util.domainUtil import getDomainFromRequest, isValidDomain, verifyRoot
from .util.domainUtil import getBucketForDomain
from .util.linkUtil import validateLinkName, getLinkClass
from .servicenode_lib import getDomainJson, validateAction
from .servicenode_lib import getLink, putLink, putLinks, getLinks, deleteLinks
from .domain_crawl import DomainCrawler
from . import hsds_logger as log


async def GET_Links(request):
    """HTTP method to return JSON for link collection"""
    log.request(request)
    app = request.app
    params = request.rel_url.query
    log.debug(f"GET_Links params: {params}")

    group_id = request.match_info.get("id")

    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(group_id, obj_class="groups"):
        msg = f"Invalid group id: {group_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

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

    follow_links = getBooleanParam(params, "follow_links")

    if "pattern" in params and params["pattern"]:
        pattern = params["pattern"]
        try:
            globmatch("abc", pattern)
        except ValueError:
            msg = f"invlaid pattern: {pattern} for  link matching"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        log.debug(f"using pattern: {pattern} for GET_Links")
    else:
        pattern = None

    create_order = getBooleanParam(params, "CreateOrder")

    limit = None
    if "Limit" in params:
        try:
            limit = int(params["Limit"])
        except ValueError:
            msg = "Bad Request: Expected int type for limit"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    if "Marker" in params:
        marker = params["Marker"]
    else:
        marker = None

    if follow_links:
        # Use DomainCrawler to fetch links from multiple objects.
        # set the follow_links and bucket params
        log.debug(f"GET_Links - following links starting with {group_id}")

        kwargs = {"action": "get_link", "bucket": bucket, "follow_links": True}
        kwargs["include_links"] = True
        if limit:
            kwargs["limit"] = limit
        items = [group_id, ]
        crawler = DomainCrawler(app, items, **kwargs)

        # will raise exception on NotFound, etc.
        await crawler.crawl()

        msg = f"DomainCrawler returned: {len(crawler._obj_dict)} objects"
        log.info(msg)
        links = crawler._obj_dict
        if pattern:
            for grp_id in links.keys():
                grp_links = links[grp_id]
                ret_links = []
                for link in grp_links:
                    title = link["title"]
                    if globmatch(title, pattern):
                        ret_links.append(link)
                links[grp_id] = ret_links
                msg = f"getLinks for {grp_id}, matched {len((ret_links))} links "
                msg += f"from {len(grp_links)} links with pattern {pattern}"
                log.debug(msg)
    else:
        kwargs = {"bucket": bucket}
        if create_order:
            kwargs["create_order"] = True
        if limit:
            kwargs["limit"] = limit
        if marker:
            kwargs["marker"] = marker
        if pattern:
            kwargs["pattern"] = pattern

        links = await getLinks(app, group_id, **kwargs)

        log.debug(f"got {len(links)} links json from dn for group_id: {group_id}")

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
    if not isValidUuid(group_id, obj_class="groups"):
        msg = f"Invalid group id: {group_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    link_title = request.match_info.get("title")
    try:
        validateLinkName(link_title)
    except ValueError:
        raise HTTPBadRequest(reason="invalid link name")

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


async def PUT_Links(request):
    """HTTP method to create a new links """
    log.request(request)
    params = request.rel_url.query
    app = request.app
    status = None

    log.debug("PUT_Links")

    username, pswd = getUserPasswordFromRequest(request)
    # write actions need auth
    await validateUserPassword(app, username, pswd)

    if not request.has_body:
        msg = "PUT_Links with no body"
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
    log.debug(f"got bucket: {bucket}")
    replace = getBooleanParam(params, "replace")

    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    verifyRoot(domain_json)

    req_grp_id = request.match_info.get("id")
    if not req_grp_id:
        req_grp_id = domain_json["root"]

    if "links" in body:
        link_items = body["links"]
        if not isinstance(link_items, dict):
            msg = f"PUT_Links expected dict for for links body, but got: {type(link_items)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        # validate the links
        for title in link_items:
            try:
                validateLinkName(title)
                link_item = link_items[title]
                getLinkClass(link_item)
            except ValueError:
                raise HTTPBadRequest(reason="invalid link item")
    else:
        link_items = None

    if link_items:
        log.debug(f"PUT Links {len(link_items)} links to add")
    else:
        log.debug("no links defined yet")

    # next, sort out where these attributes are going to

    grp_ids = {}
    if "grp_ids" in body:
        body_ids = body["grp_ids"]
        if isinstance(body_ids, list):
            # multi cast the links - each link  in link_items
            # will be written to each of the objects identified by obj_id
            if not link_items:
                msg = "no links provided"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            else:
                for grp_id in body_ids:
                    if not isValidUuid(grp_id):
                        msg = f"Invalid object id: {grp_id}"
                        log.warn(msg)
                        raise HTTPBadRequest(reason=msg)
                    grp_ids[grp_id] = link_items

                msg = f"{len(link_items)} links will be multicast to "
                msg += f"{len(grp_ids)} objects"
                log.info(msg)
        elif isinstance(body_ids, dict):
            # each value is body_ids is a set of links to write to the object
            # unlike the above case, different attributes can be written to
            # different objects
            if link_items:
                msg = "links defined outside the group_ids dict"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            else:
                for grp_id in body_ids:
                    if not isValidUuid(grp_id):
                        msg = f"Invalid object id: {grp_id}"
                        log.warn(msg)
                        raise HTTPBadRequest(reason=msg)
                    id_json = body_ids[grp_id]

                    if "links" not in id_json:
                        msg = f"PUT_links with no links for grp_id: {grp_id}"
                        log.warn(msg)
                        raise HTTPBadRequest(reason=msg)
                    link_items = id_json["links"]
                    if not isinstance(link_items, dict):
                        msg = f"PUT_Links expected dict for grp_id {grp_id}, "
                        msg += f"but got: {type(link_items)}"
                        log.warn(msg)
                        raise HTTPBadRequest(reason=msg)
                    # validate link items
                    for title in link_items:
                        try:
                            validateLinkName(title)
                            link_item = link_items[title]
                            getLinkClass(link_item)
                        except ValueError:
                            raise HTTPBadRequest(reason="invalid link item")
                    grp_ids[grp_id] = link_items

                # write different attributes to different objects
                msg = f"PUT_Links over {len(grp_ids)} objects"
        else:
            msg = f"unexpected type for grp_ids: {type(grp_ids)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        # use the object id from the request
        grp_id = request.match_info.get("id")
        if not grp_id:
            msg = "Missing object id"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        grp_ids[grp_id] = link_items  # make it look like a list for consistency

    log.debug(f"got {len(grp_ids)} grp_ids")

    await validateAction(app, domain, req_grp_id, username, "create")

    count = len(grp_ids)
    if count == 0:
        msg = "no grp_ids defined"
        log.warn(f"PUT_Attributes: {msg}")
        raise HTTPBadRequest(reason=msg)
    elif count == 1:
        # just send one PUT Attributes request to the dn
        kwargs = {"bucket": bucket}
        if replace:
            kwargs["replace"] = True
        grp_id = list(grp_ids.keys())[0]
        link_json = grp_ids[grp_id]
        log.debug(f"got link_json: {link_json}")

        status = await putLinks(app, grp_id, link_json, **kwargs)

    else:
        # put multi obj
        kwargs = {"action": "put_link", "bucket": bucket}
        if replace:
            kwargs["replace"] = True

        crawler = DomainCrawler(app, grp_ids, **kwargs)

        # will raise exception on not found, server busy, etc.
        await crawler.crawl()

        status = crawler.get_status()

        log.info("DomainCrawler done for put_links action")

    # link creation successful
    log.debug(f"PUT_Links returning status: {status}")
    req_rsp = {}
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
    if not isValidUuid(group_id, obj_class="groups"):
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
        try:
            validateLinkName(title)
        except ValueError:
            raise HTTPBadRequest(reason="invalid link name")

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
    log.debug(f"POST_Links params: {params}")
    log.info("POST_Links")
    req_id = request.match_info.get("id")

    follow_links = getBooleanParam(params, "follow_links")

    create_order = getBooleanParam(params, "CreateOrder")

    limit = None
    if "Limit" in params:
        try:
            limit = int(params["Limit"])
        except ValueError:
            msg = "Bad Request: Expected int type for limit"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    if "pattern" in params:
        pattern = params["pattern"]
    else:
        pattern = None

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

    if "grp_ids" in body:
        group_ids = body["grp_ids"]
    else:
        group_ids = None

    if titles is None and group_ids is None:
        msg = "expected body to contain one of titles, group_ids keys"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if follow_links and titles:
        msg = "titles list can not be used with follow_links"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if limit and titles:
        msg = "Limit parameter can not be used with titles list"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if create_order and titles:
        msg = "CreateOrder parameter can not be used with titles"
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
            msg = "titles must not be provided if group_ids is a dict"
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
        if not isValidUuid(group_id, obj_class="groups"):
            msg = f"Invalid group id: {group_id}"
            log.warn(msg)

        if (group_ids is not None) and isinstance(group_ids, dict):
            titles = items[group_id]

        if titles is None:
            log.debug(f"getting all links for {group_id}")
        elif isinstance(titles, list):
            for title in titles:
                try:
                    validateLinkName(title)
                except ValueError:
                    raise HTTPBadRequest(reason="invalid link name")
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

    await validateAction(app, domain, req_id, username, "read")

    resp_json = {}

    if len(items) == 0:
        msg = "no group ids specified for POST Links"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    elif len(items) == 1 and not follow_links:
        # just make a request to the datanode
        group_id = list(items.keys())[0]
        kwargs = {"bucket": bucket}

        titles = items[group_id]
        if titles:
            kwargs["titles"] = titles
        else:
            if limit:
                kwargs["limit"] = limit
        if create_order:
            kwargs["create_order"] = True
        if pattern:
            kwargs["pattern"] = pattern
        links = await getLinks(app, group_id, **kwargs)

        resp_json["links"] = links
    else:
        # Use DomainCrawler to fetch links from multiple object.
        # set the follow_links and bucket params
        kwargs = {"action": "get_link", "bucket": bucket, "include_links": True}
        if follow_links:
            kwargs["follow_links"] = True
        if create_order:
            kwargs["create_order"] = True
        if limit:
            kwargs["limit"] = limit
        if pattern:
            kwargs["pattern"] = pattern

        # If retrieving same link names from multiple groups, map each UUID to all links provided
        if isinstance(group_ids, list) and titles:
            for i in items:
                items[i] = titles

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
    if not isValidUuid(group_id, obj_class="groups"):
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
