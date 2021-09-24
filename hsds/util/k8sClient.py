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
# Kubernetes utility functions
#


import ssl
import aiohttp
from asyncio import CancelledError
from aiohttp.web_exceptions import HTTPForbidden, HTTPNotFound, HTTPBadRequest
from aiohttp.web_exceptions import HTTPServiceUnavailable, HTTPInternalServerError
from aiohttp.client_exceptions import ClientError
import urllib3
from .. import hsds_logger as log

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

APISERVER = "https://kubernetes.default.svc"
SERVICEACCOUNT = "/var/run/secrets/kubernetes.io/serviceaccount"

def _k8sGetNamespace():
    """ Return namespace of current pod """
    namespace = None
    try:
        with open(SERVICEACCOUNT+"/namespace") as f:
            s = f.read()
            if s:
                namespace = s.strip()
    except FileNotFoundError:
        pass
    
    if not namespace:
        log.error("Unable to read namespace - not running in Kubernetes?")
        raise ValueError("Kubernetes namespace could not be determined")

def _k8sGetBearerToken():
    """ Return kubernetes bearer token """
    token = None
    try:
        with open(SERVICEACCOUNT+"/token") as f:
            s = f.read()
            if s:
                token = s.strip()
    except FileNotFoundError:
        pass
    
    if not token:
        log.error("Unable to read token - not running in Kubernetes?")
        raise ValueError("Could not get Kubernetes auth token")

    return "Bearer " + token


async def _k8sListPod():
    """ Make http request to k8s to get info on all pods in 
      the current namespace.  Return json dictionary """
    namespace = _k8sGetNamespace()
    cafile = SERVICEACCOUNT+"/ca.crt"
    ssl_ctx = ssl.create_default_context(cafile=cafile)
    token = _k8sGetBearerToken()
    headers = {"Authorization": token}
    conn = aiohttp.TCPConnector(ssl_context=ssl_ctx)
    # TBD - save session for re-use

    status_code = None
    timeout = 0.5
    url = f"{APISERVER}/api/v1/namespaces{namespace}/pods"
    # TBD: use read_bufsize parameter to optimize read for large responses
    try:
        async with conn.get(url, headeers=headers, timeout=timeout) as rsp:
            log.info(f"http_get status for k8s pods: {rsp.status} for req: {url}")
            status_code = rsp.status
            if rsp.status == 200:
                # 200, so read the response
                log.info(f"got podlist resonse: {rsp.json}")
            elif status_code == 400:
                log.warn(f"BadRequest to {url}")
                raise HTTPBadRequest()
            elif status_code == 403:
                log.warn(f"Forbiden to access {url}")
                raise HTTPForbidden()
            elif status_code == 404:
                log.warn(f"Object: {url} not found")
                raise HTTPNotFound()
            elif status_code == 503:
                log.warn(f"503 error for http_get_Json {url}")
                raise HTTPServiceUnavailable()
            else:
                log.error(f"request to {url} failed with code: {status_code}")
                raise HTTPInternalServerError()

    except ClientError as ce:
        log.debug(f"ClientError: {ce}")
        raise HTTPInternalServerError()
    except CancelledError as cle:
        log.error(f"CancelledError for http_get({url}): {cle}")
        raise HTTPInternalServerError()

    return ["127.0.0.1",]  # for test


async def getPodIps(k8s_app_label):
    log.info(f"getPodIps({k8s_app_label})")
    pod_ips = await _k8sListPod()
  
    return pod_ips   
    
 
    
    
 