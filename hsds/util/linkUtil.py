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
# linkdUtil:
# link related functions
#
import time

from .. import hsds_logger as log


def validateLinkName(name):
    """ verify the link name is valid """
    if not isinstance(name, str):
        msg = "Unexpected type for link name"
        log.warn(msg)
        raise ValueError(msg)
    if name.find("/") >= 0:
        msg = "link name contains slash"
        log.warn(msg)
        raise ValueError(msg)


def getLinkClass(link_json):
    """ verify this is a valid link
        returns the link class """
    log.debug(f"getLinkClass({link_json})")
    if "class" in link_json:
        link_class = link_json["class"]
    else:
        link_class = None
    if "h5path" in link_json and "id" in link_json:
        msg = "link tgt_id and h5path both set"
        log.warn(msg)
        raise ValueError(msg)
    if "id" in link_json:
        tgt_id = link_json["id"]
        if not isinstance(tgt_id, str) or len(tgt_id) < 38:
            msg = f"link with invalid id: {tgt_id}"
            log.warn(msg)
            raise ValueError(msg)
        if tgt_id[:2] not in ("g-", "t-", "d-"):
            msg = "link tgt must be group, datatype or dataset uuid"
            log.warn(msg)
            raise ValueError(msg)
        if link_class:
            if link_class != "H5L_TYPE_HARD":
                msg = f"expected link class to be H5L_TYPE_HARD but got: {link_class}"
                log.warn(msg)
                raise ValueError(msg)
        else:
            link_class = "H5L_TYPE_HARD"
    elif "h5path" in link_json:
        h5path = link_json["h5path"]
        log.debug(f"link path: {h5path}")
        if "h5domain" in link_json:
            if link_class:
                if link_class != "H5L_TYPE_EXTERNAL":
                    msg = f"expected link class to be H5L_TYPE_EXTERNAL but got: {link_class}"
                    log.warn(msg)
                    raise ValueError(msg)
            else:
                link_class = "H5L_TYPE_EXTERNAL"
        else:
            if link_class:
                if link_class != "H5L_TYPE_SOFT":
                    msg = f"expected link class to be H5L_TYPE_SOFT but got: {link_class}"
                    log.warn(msg)
                    raise ValueError(msg)
            else:
                link_class = "H5L_TYPE_SOFT"
    else:
        msg = "link with no id or h5path"
        log.warn(msg)
        raise ValueError(msg)

    return link_class


def isEqualLink(link1, link2):
    """ Return True if the two links are the same """

    for obj in (link1, link2):
        if not isinstance(obj, dict):
            raise TypeError(f"unexpected type: {type(obj)}")
        if "class" not in obj:
            raise TypeError("expected class key for link")
    if link1["class"] != link2["class"]:
        return False  # different link types
    link_class = link1["class"]
    if link_class == "H5L_TYPE_HARD":
        for obj in (link1, link2):
            if "id" not in obj:
                raise TypeError(f"expected id key for link: {obj}")
        if link1["id"] != link2["id"]:
            return False
    elif link_class == "H5L_TYPE_SOFT":
        for obj in (link1, link2):
            if "h5path" not in obj:
                raise TypeError(f"expected h5path key for link: {obj}")
        if link1["h5path"] != link2["h5path"]:
            return False
    elif link_class == "H5L_TYPE_EXTERNAL":
        for obj in (link1, link2):
            for k in ("h5path", "h5domain"):
                if k not in obj:
                    raise TypeError(f"expected {k} key for link: {obj}")
        if link1["h5path"] != link2["h5path"]:
            return False
        if link1["h5domain"] != link2["h5domain"]:
            return False
    else:
        raise TypeError(f"unexpected link class: {link_class}")
    return True


def h5Join(path, paths):
    h5path = path
    if not paths:
        return h5path
    if isinstance(paths, str):
        paths = (paths,)
    for s in paths:
        if h5path[-1] != "/":
            h5path += "/"
        h5path += s
    return h5path


def getRequestLink(title, link_json, predate_max_time=0.0):
    """ return normalized link from request json
        Throw value error if badly formatted """

    if not isinstance(link_json, dict):
        msg = f"expected dict for for links, but got: {type(link_json)}"
        log.warn(msg)
        raise ValueError(msg)

    log.debug(f"getRequestLink title: {title} link_json: {link_json}")
    link_item = {}  # normalized link item to return

    now = time.time()

    validateLinkName(title)  # will raise ValueError is invalid

    link_class = getLinkClass(link_json)
    if "class" in link_item:
        if link_class != link_json["class"]:
            msg = f"expected link class of: {link_class} but got {link_json}"
            log.warn(msg)
            raise ValueError(msg)

    link_item = {"class": link_class}

    if link_class == "H5L_TYPE_HARD":
        if "id" not in link_json:
            msg = "expected id key for hard link"
            log.warn(msg)
            raise ValueError
        link_item["id"] = link_json["id"]
    else:
        if link_class in ("H5L_TYPE_SOFT", "H5L_TYPE_EXTERNAL"):
            if "h5path" not in link_json:
                msg = "expected h5path key for soft link"
                log.warn(msg)
                raise ValueError(msg)
            link_item["h5path"] = link_json["h5path"]

        if link_class == "H5L_TYPE_EXTERNAL":
            if "h5domain" not in link_json:
                msg = "expected h5domain key for external link"
                log.warn(msg)
                raise ValueError(msg)

    if "created" in link_json:
        created = link_json["created"]
        # allow "pre-dated" attributes if recent enough
        if now - created < predate_max_time:
            link_item["created"] = created
        else:
            log.warn("stale created timestamp for link, ignoring")
    if "created" not in link_item:
        link_item["created"] = now

    return link_item


def getRequestLinks(links_json, predate_max_time=0.0):
    """ return list of normalized links from request json
        Throw value error if any is badly formatted """

    if not isinstance(links_json, dict):
        msg = f"POST_Groups expected dict for for links, but got: {type(links_json)}"
        log.warn(msg)
        raise ValueError(msg)

    links = {}  # normalized link items to return
    kwargs = {"predate_max_time": predate_max_time}

    for title in links_json:
        links[title] = getRequestLink(title, links_json[title], **kwargs)

    return links
