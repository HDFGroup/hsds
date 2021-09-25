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
    log.debug(f"k8s namespace: [{namespace}]")
    return namespace

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
    session = aiohttp.ClientSession(connector=conn)
    pod_json = None
    # TBD - save session for re-use

    status_code = None
    timeout = 0.5
    url = f"{APISERVER}/api/v1/namespaces/{namespace}/pods"
    # TBD: use read_bufsize parameter to optimize read for large responses
    try:
        async with session.get(url, headers=headers, timeout=timeout) as rsp:
            log.info(f"http_get status for k8s pods: {rsp.status} for req: {url}")
            status_code = rsp.status
            if rsp.status == 200:
                # 200, so read the response
                pod_json = await rsp.json()
                log.debug(f"got podlist resonse: {pod_json}")
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

    return pod_json

def _k8sGetPodIPs(pod_json, k8s_app_label):
    if not isinstance(pod_json, dict):
        msg = f"_k8sGetPodIPs - unexpected type: {type(pod_json)}"
        log.error(msg)
        raise TypeError(msg)
    if "items" not in pod_json:
        msg = "_k98sGetPodIPS - no items key"
        log.error(msg)
        raise KeyError(msg)
    items = pod_json["items"]
    
    for item in items:
        if "metadata" not in item:
            msg = "_k8sGetPodIPs - expected to find metadata key"
            log.warn(msg)
            continue
        metadata = item["metaadata"]
        log.debug(f"pod metadata: {metadata}")
        if "labels" not in metadata:
            msg = "_k8sGetPodIPs - expected to labels key in metadata"
            log.warn(msg)
            continue
        labels = metadata["labels"]
        if "app" not in labels:
            msg = "_k8sGetPodIPs - no app label"
            log.warn(msg)
            continue
        app_label = labels["app"]
        if app_label != k8s_app_label:
            msg = f"_k8sGetPodIPs - app_label: {app_label} not equal to: "
            msg += f"{k8s_app_label}, skipping"
            log.debug(msg)
            continue
    return ["127.0.0.1",] # test
        

async def getPodIps(k8s_app_label):
    log.info(f"getPodIps({k8s_app_label})")
    pod_json = await _k8sListPod()
    pod_ips = _k8sGetPodIPs(pod_json, k8s_app_label)

    return pod_ips   
    
 
    
    
 
