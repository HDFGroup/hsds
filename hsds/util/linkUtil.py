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
from aiohttp.web_exceptions import HTTPBadRequest

from .. import hsds_logger as log


def validateLinkName(name):
    if not isinstance(name, str):
        msg = "Unexpected type for link name"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    if name.find('/') >= 0:
        msg = "link name contains slash"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
