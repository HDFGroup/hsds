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

import asyncio
import ssl
import aiohttp
from asyncio import CancelledError
from aiohttp.web_exceptions import HTTPForbidden, HTTPNotFound, HTTPBadRequest
from aiohttp.web_exceptions import HTTPServiceUnavailable, HTTPInternalServerError
from aiohttp.client_exceptions import ClientError
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

APISERVER = "https://kubernetes.default.svc"
SERVICEACCOUNT = "/var/run/secrets/kubernetes.io/serviceaccount"


def log_error(msg):
    print(msg)


def log_warn(msg):
    print(msg)


def log_info(msg):
    print(msg)


def log_debug(msg):
    print(msg)


def _k8sGetNamespace():
    """Return namespace of current pod"""
    namespace = None
    try:
        with open(SERVICEACCOUNT + "/namespace") as f:
            s = f.read()
            if s:
                namespace = s.strip()
    except FileNotFoundError:
        pass

    if not namespace:
        log_error("Unable to read namespace - not running in Kubernetes?")
        raise ValueError("Kubernetes namespace could not be determined")
    log_debug(f"k8s namespace: [{namespace}]")
    return namespace


def _k8sGetBearerToken():
    """Return kubernetes bearer token"""
    token = None
    try:
        with open(SERVICEACCOUNT + "/token") as f:
            s = f.read()
            if s:
                token = s.strip()
    except FileNotFoundError:
        pass

    if not token:
        log_error("Unable to read token - not running in Kubernetes?")
        raise ValueError("Could not get Kubernetes auth token")

    return "Bearer " + token


def getIPKeys(metadata):
    KEY_PATH = ("fieldsV1", "f:status", "f:podIPs")
    pod_ips = []
    if not isinstance(metadata, dict):
        log_warn(f"getIPKeys - expected list but got: {type(metadata)}")
        return pod_ips
    if "managedFields" not in metadata:
        log_warn(f"getIPKeys - expected managedFields key but got: {metadata.keys()}")
        return pod_ips
    managedFields = metadata["managedFields"]
    if not isinstance(managedFields, list):
        log_warn(f"expected managedFields to be list but got: {type(managedFields)}")
        return pod_ips
    # log.debug(f"mangagedFields - {len(managedFields)} items")
    for item in managedFields:
        if not isinstance(item, dict):
            log_warn(f"ignoring item type {type(item)}: {item}")
            continue
        for key in KEY_PATH:
            # log.debug(f"using key: {key}")
            if key not in item:
                # key not found, move on to next managedField
                msg = f"getIPKeys - looking for {key} key but not present"
                log_debug(msg)
                break
            item = item[key]
            # log.debug(f"got obj type: {type(item)}")
            # log.debug(f"item: {item}")
            if not isinstance(item, dict):
                log_warn("not a dict")
                break
        # item should be a dict that looks like:
        # {'.': {}, 'k:{"ip":"192.168.17.20"}': {'.': {}, 'f:ip': {}}}
        # ip is burried in the key that starts with "k:"
        if not isinstance(item, dict):
            log_warn(f"expected podIPs to be dict but got: {type(item)}")
            continue
        for k in item:
            # log.debug(f"got podIPs key: {k}")
            if k.startswith('k:{"ip":"'):
                n = len('k:{"ip":"')
                s = k[n:]
                m = s.find('"')
                if m < 0:
                    log_warn(f"unexpected key: {k}")
                    continue
                ip = s[:m]
                log_debug(f"found pod ip: {ip}")
                pod_ips.append(ip)
    log_debug(f"getIPKeys  done: returning {pod_ips}")
    return pod_ips


def _k8sGetPodIPs(pod_json, k8s_app_label):
    if not isinstance(pod_json, dict):
        msg = f"_k8sGetPodIPs - unexpected type: {type(pod_json)}"
        log_error(msg)
        raise TypeError(msg)
    if "items" not in pod_json:
        msg = "_k98sGetPodIPS - no items key"
        log_error(msg)
        raise KeyError(msg)
    items = pod_json["items"]
    ipKeys = []

    for item in items:
        if "metadata" not in item:
            msg = "_k8sGetPodIPs - expected to find metadata key"
            log_warn(msg)
            continue
        metadata = item["metadata"]
        # log.debug(f"pod metadata: {metadata}")
        ipKeys.extend(getIPKeys(metadata))
    return ipKeys


async def _k8sListPod(k8s_label_selector):
    """Make http request to k8s to get info on all pods in
    the current namespace.  Return json dictionary"""
    namespace = _k8sGetNamespace()
    cafile = SERVICEACCOUNT + "/ca.crt"
    ssl_ctx = ssl.create_default_context(cafile=cafile)
    token = _k8sGetBearerToken()
    headers = {"Authorization": token}
    params = {"labelSelector": k8s_label_selector}
    conn = aiohttp.TCPConnector(ssl_context=ssl_ctx)
    pod_json = None
    # TBD - save session for re-use

    status_code = None
    url = f"{APISERVER}/api/v1/namespaces/{namespace}/pods"
    async with aiohttp.ClientSession(connector=conn) as session:
        # TBD: use read_bufsize parameter to optimize read for large responses
        try:
            async with session.get(url, headers=headers, params=params) as rsp:
                log_info(f"http_get status for k8s pods: {rsp.status} for req: {url}")
                status_code = rsp.status
                if rsp.status == 200:
                    # 200, so read the response
                    pod_json = await rsp.json()
                    # log.debug(f"got podlist resonse: {pod_json}")
                elif status_code == 400:
                    log_warn(f"BadRequest to {url}")
                    raise HTTPBadRequest()
                elif status_code == 403:
                    log_warn(f"Forbiden to access {url}")
                    raise HTTPForbidden()
                elif status_code == 404:
                    log_warn(f"Object: {url} not found")
                    raise HTTPNotFound()
                elif status_code == 503:
                    log_warn(f"503 error for http_get_Json {url}")
                    raise HTTPServiceUnavailable()
                else:
                    log_error(f"request to {url} failed with code: {status_code}")
                    raise HTTPInternalServerError()

        except ClientError as ce:
            log_debug(f"ClientError: {ce}")
            raise HTTPInternalServerError()
        except CancelledError as cle:
            log_error(f"CancelledError for http_get({url}): {cle}")
            raise HTTPInternalServerError()

    return pod_json


async def getPodIps(k8s_label_selector):
    log_debug(f"getPodIps({k8s_label_selector})")
    pod_json = await _k8sListPod(k8s_label_selector)
    pod_ips = _k8sGetPodIPs(pod_json)
    log_info(f"gotPodIps: {pod_ips}")

    return pod_ips


async def main():
    print("main")
    pod_ips = await getPodIps("app=hsds")
    print("pod_ips:", pod_ips)


#
# main
#
asyncio.run(main())
