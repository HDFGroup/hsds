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
# attribute related utilities
#

from aiohttp.web_exceptions import HTTPBadRequest, HTTPInternalServerError

from .. import hsds_logger as log


def getRequestCollectionName(request):
    """request is in the form:
        /(datasets|groups|datatypes)/<id>/attributes(/<name>),
    return: "datasets" | "groups" | "types"
    """
    uri = request.path

    npos = uri.find("/")
    if npos < 0:
        msg = "bad request uri"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    npos += 1
    uri = uri[npos:]
    npos = uri.find("/")  # second '/'
    col_name = uri[:npos]

    log.debug(f"got collection name: [{col_name}]")
    if col_name not in ("datasets", "groups", "datatypes"):
        msg = f"Error: collection name unexpected: {col_name}"
        log.error(msg)
        # shouldn't get routed here in this case
        raise HTTPInternalServerError()

    return col_name


def validateAttributeName(name):
    """verify that the attribute name is valid"""
    if not isinstance(name, str):
        msg = f"attribute name must be a string, but got: {type(name)}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)


def isEqualAttr(attr1, attr2):
    """ compare to attributes, return True if the same, False if differnt """
    for obj in (attr1, attr2):
        if not isinstance(obj, dict):
            raise TypeError(f"unexpected type: {type(obj)}")
        if "type" not in obj:
            raise TypeError("expected type key for attribute")
        if "shape" not in obj:
            raise TypeError("expected shape key for attribute")
        # value is optional (not set for null space attributes)
    if attr1["type"] != attr2["type"]:
        return False
    if attr1["shape"] != attr2["shape"]:
        return False
    shape_class = attr1["shape"].get("class")
    if shape_class == "H5S_NULL":
        return True  # nothing else to compare
    for obj in (attr1, attr2):
        if "value" not in obj:
            raise TypeError("expected value key for attribute")
    return attr1["value"] == attr2["value"]

    if not isinstance(attr1, dict):
        raise TypeError(f"unexpected type: {type(attr1)}")
        return True
    if not attr1 and not attr2:
        return True
