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
    """ request is in the form /(datasets|groups|datatypes)/<id>/attributes(/<name>),
    return datasets | groups | types
    """
    uri = request.path

    npos = uri.find('/')
    if npos < 0:
        msg = "bad request uri"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    uri = uri[(npos+1):]
    npos = uri.find('/')  # second '/'
    col_name = uri[:npos]


    log.debug('got collection name: [' + col_name + ']')
    if col_name not in ('datasets', 'groups', 'datatypes'):
        msg = "Error: collection name unexpected: {}".format(col_name)
        log.error(msg)
        # shouldn't get routed here in this case
        raise HTTPInternalServerError()

    return col_name

def validateAttributeName(name):
    """ verify that the attribute name is valid
    """
    if not isinstance(name, str):
        msg = "attribute name must be a string, but got: {}".format(type(name))
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if name.find('/') > -1:
        msg = "attribute names cannot contain slashes"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    # TBD - add any other restrictions
