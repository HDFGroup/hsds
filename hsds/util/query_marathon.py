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
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError

from aiobotocore import get_session

from .. import hsds_logger as log
from .httpUtil import http_get

import os

# This is a utility to interrogate a Marathon HSDS configuration
# See http://mesosphere.github.io/marathon/api-console/index.html


class MarathonClient:
    """
     Utility class for access the parts of DCOS Marathon configurations we
     need to coordinate the HSDS nodes
    """

    def __init__(self, app):
        if "session" not in app:
            session = get_session()
            app["session"] = session

        self._app = app

    async def getSNInstances(self):
        if "is_dcos" not in self._app:
            msg = "cannot use the MarathonClient in a non-DCOS context"
            log.error(msg)
            raise HTTPInternalServerError()

        if "DCOS_PATH_SERVICE_NODE" in os.environ:
            hsds_sn_node = os.environ["DCOS_PATH_SERVICE_NODE"]
        else:
            msg = "Must set DCOS_PATH_SERVICE_NODE environment variable to "
            msg += "Marathon path to service node n order to query the "
            msg += "correct marathon config"
            log.error(msg)
            return -1

        req = f"http://master.mesos/marathon/v2/apps/{hsds_sn_node}"

        try:
            instancesJSON = await http_get(self._app, req)
        except HTTPNotFound:
            log.warn("Could not retrieve marathon app instance information.")
            return -1

        if instancesJSON is None or not isinstance(instancesJSON, dict):
            log.warn("invalid marathon query response")
        else:
            instances = None
            if instancesJSON["app"] is not None:
                if instancesJSON["app"]["instances"] is not None:
                    instances = instancesJSON["app"]["instances"]

            if instances:
                msg = f"SN instances {instances}"
                log.debug()
                return instances
            else:
                log.warn("Incomplete or malformed JSON returned from SN node.")
                return -1

    async def getDNInstances(self):
        if "is_dcos" not in self._app:
            msg = "cannot use the MarathonClient in a non-DCOS context"
            log.error(msg)
            raise HTTPInternalServerError()

        if "DCOS_PATH_DATA_NODE" in os.environ:
            hsds_data_node = os.environ["DCOS_PATH_DATA_NODE"]
        else:
            msg = "Must set DCOS_PATH_DATA_NODE environment variable to "
            msg += "Marathon path to service node n order to query the "
            msg += "correct marathon config"
            log.error(msg)
            raise HTTPInternalServerError()

        req = "http://master.mesos/marathon/v2/apps/%s" % hsds_data_node

        try:
            instancesJSON = await http_get(self._app, req)
        except HTTPNotFound:
            log.warn("Could not retrieve marathon app instance information.")
            return -1

        if instancesJSON is None or not isinstance(instancesJSON, dict):
            log.warn("invalid marathon query response")
        else:
            instances = None
            if "app" in instancesJSON:
                app_instances = instancesJSON["app"]
                if "instances" in app_instances:
                    instances = app_instances["instances"]

            if instances is not None:
                log.debug(f"DN instances {instancesJSON['app']['instances']}")
                return instancesJSON["app"]["instances"]
            else:
                log.warn("Incomplete or malformed JSON returned from DN node.")
                return -1
