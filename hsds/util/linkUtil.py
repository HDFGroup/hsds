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
from h5json.time_util import getNow
from h5json.link_util import validateLinkName, getLinkClass, getLinkPath, getLinkFilePath

from .. import hsds_logger as log


def getRequestLink(title, link_json, predate_max_time=0.0):
    """ return normalized link from request json
        Throw value error if badly formatted """

    if not isinstance(link_json, dict):
        msg = f"expected dict for for links, but got: {type(link_json)}"
        log.warn(msg)
        raise ValueError(msg)

    log.debug(f"getRequestLink title: {title} link_json: {link_json}")
    link_item = {}  # normalized link item to return

    now = getNow()

    validateLinkName(title)  # will raise ValueError is invalid

    link_class = getLinkClass(link_json)

    link_item = {"class": link_class}

    if link_class == "H5L_TYPE_HARD":
        if "id" not in link_json:
            msg = "expected id key for hard link"
            log.warn(msg)
            raise ValueError
        link_item["id"] = link_json["id"]
    else:
        if link_class in ("H5L_TYPE_SOFT", "H5L_TYPE_EXTERNAL"):
            link_item["h5path"] = getLinkPath(link_json)

        if link_class == "H5L_TYPE_EXTERNAL":
            link_item["file"] = getLinkFilePath(link_json)

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
