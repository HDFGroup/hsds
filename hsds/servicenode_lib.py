
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
 
from aiohttp.errors import  ClientError
from aiohttp import HttpProcessingError 

from util.idUtil import   getDataNodeUrl
from util.httpUtil import   http_get_json
from util.domainUtil import getS3KeyForDomain

import hsds_logger as log


async def getDomainJson(app, domain):
    """ Return domain JSON from cache or fetch from DN if not found
        Note: only call from sn!
    """
    log.info("getDomainJson({})".format(domain))
    if app["node_type"] != "sn":
        log.error("wrong node_type")
        raise HttpProcessingError("Unexpected error", code=500)

    domain_cache = app["domain_cache"]
    #domain = getDomainFromRequest(request)

    if domain in domain_cache:
        log.info("returning domain_cache value")
        return domain_cache[domain]

    domain_json = { }
    req = getDataNodeUrl(app, domain)
    req += "/domains/" + domain 
    log.info("sending dn req: {}".format(req))
    try:
        domain_json = await http_get_json(app, req)
    except ClientError as ce:
        msg="Error getting domain state -- " + str(ce)
        log.warn(msg)
        raise HttpProcessingError(message=msg, code=503)
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HttpProcessingError("Unexpected error", code=500)

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HttpProcessingError("Unexpected error", code=500)

    domain_cache[domain] = domain_json  # add to cache
    return domain_json
