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
# common node methods of hsds cluster
#
import asyncio
import sys
import os
from asyncio import TimeoutError
import time
import random
import psutil
#from copy import copy

from aiohttp.web import Application
from aiohttp.web_exceptions import HTTPNotFound, HTTPGone, HTTPInternalServerError
from aiohttp.client_exceptions import ClientError
from asyncio import CancelledError


from . import config
from .util.httpUtil import http_get, http_post, jsonResponse 
from .util.idUtil import createNodeId, getNodeNumber, getNodeCount
from .util.authUtil import getUserPasswordFromRequest, validateUserPassword
from . import hsds_logger as log
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config

HSDS_VERSION = "0.6.2"

def getVersion():
    return HSDS_VERSION

def getHeadUrl(app):
    head_url = None

    if head_url in app:
        head_url = app["head_url"]
    elif config.get("head_endpoint"):
        head_url = config.get("head_endpoint")
    else:
        head_port = config.get("head_port")
        head_url = f"http://hsds_head:{head_port}"
    log.debug(f"head_url: {head_url}")
    return head_url


async def get_info(app, url):
    """
    Invoke the /info request on the indicated url and return the response
    """
    req = url + "/info"
    log.info(f"get_info({url})")
    try:
        log.debug("about to call http_get")
        rsp_json = await http_get(app, req)
        log.debug("called http_get")
        if "node" not in rsp_json:
            log.error("Unexpected response from node")
            return None

    except OSError as ose:
        log.warn(f"OSError for req: {req}: {ose}")
        return None

    except HTTPInternalServerError as hpe:
        log.warn(f"HTTPInternalServerError for req {req}: {hpe}")
        # node has gone away?
        return None

    except HTTPNotFound as nfe:
        log.warn(f"HTTPNotFound error for req {req}: {nfe}")
        # node has gone away?
        return None

    except TimeoutError as toe:
        log.warn(f"Timeout error for req: {req}: {toe}")
        # node has gone away?
        return None
    except HTTPGone as hg:
        log.warn("Timeout error for req: {}: {}".format(req, str(hg)))
        # node has gone away?
        return None
    except:
        log.warn("uncaught exception in get_info")

    return rsp_json

async def oio_get_dn_urls(app):
    """ talk to conscience to get DN info """ 
    oio_proxy = app["oio_proxy"]
    node_ip = app["node_ip"]
    if not node_ip:
        log.error("node_ip not set")
        return
    node_type = app["node_type"]
    if node_type not in ("sn", "dn"):
        log.error("unexpected node type")
        return
    service_name = "hdf" + node_type
    req = oio_proxy + "/v3.0/OPENIO/conscience/register"

    body = {
        "addr": node_ip + ":" + str(app["node_port"]),
        "tags": { "stat.cpu": 100, "tag.up": True},
        "type": service_name
    }
    log.debug(f"conscience register: body: {body}")
    try:
        await http_post(app, req, data=body)
    except ClientError as client_exception:
        log.error(f"got ClientError registering with oio_proxy: {client_exception} and body {body}")
        return
    except CancelledError as cancelled_exception:
        log.error(f"got CancelledError registering with oio_proxy: {cancelled_exception} and body {body}")
        return
    log.info("oio registration successful")

    # get list of DN containers
    req = oio_proxy + "/v3.0/OPENIO/conscience/list?type=hdfdn"
    try:
        dn_node_list = await http_get(app, req)
    except ClientError as client_exception:
        log.error(f"got ClientError listing dn nodes with oio_proxy: {client_exception}")
        return
    except CancelledError as cancelled_exception:
        log.error(f"got CancelledError listing dn nodes with oio_proxy: {cancelled_exception}")
        return
    except BaseException as error:
        log.error(f"A BaseException occurred: {error}")
        return
    log.info(f"got {len(dn_node_list)} conscience list items")
    # create map keyed by dn addr
    dn_node_map = {}
    dn_urls = []
    for dn_node in dn_node_list:
        log.debug(f"checking dn conscience list item: {dn_node}")
        if "addr" not in dn_node:
            log.warn(f"conscience list item with no addr: {dn_node}")
            continue
        addr = dn_node["addr"]
        if "score" not in dn_node:
            log.warn(f'conscience list item with no score key: {dn_node}')
            continue
        if dn_node["score"] <= 0:
            log.debug(f"zero score - skipping conscience list addr: {addr}")
            continue
        if addr in dn_node_map:
            # shouldn't ever get this?
            log.warn(f"duplicate entry for node: {dn_node}")
            continue
        log.debug(f"oio_get_dn_urls - adding address: {addr}")
        dn_urls.append("http://" + addr)
         
    log.info(f"done with oio_get_dn_urls, got: {len(dn_urls)} dn urls")
    return dn_urls

def k8s_get_dn_urls(app):
    """ get http urls by querying k8s api """
    log.info("k8s_get_dn_urls")
    if not app["node_ip"]:
        log.error("node_ip value not set")
        dn_urls = []  # this will block going into ready state
        return dn_urls
    # TBD - find more elegant way to avoid this warning
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    k8s_config.load_incluster_config() #get the config from within the cluster and set it as the default config for all new clients
    c=k8s_client.Configuration() #go and get a copy of the default config
    c.verify_ssl=False #set verify_ssl to false in that config
    k8s_client.Configuration.set_default(c) #make that config the default for all new clients
    v1 = k8s_client.CoreV1Api()
    # TBD - use the async version
    k8s_app_label = config.get("k8s_app_label")
    ret = v1.list_pod_for_all_namespaces(watch=False)
    pod_ips = []
    dn_urls = []
    for i in ret.items:
        pod_ip = i.status.pod_ip
        if not pod_ip:
            continue
        labels = i.metadata.labels
        if labels and "app" in labels and labels["app"] == k8s_app_label:
            log.info(f"found hsds pod with app label: {k8s_app_label} - ip: {pod_ip}")
            pod_ips.append(pod_ip)
    if not pod_ips:
        log.error("Expected to find at least one hsds pod")
        return
    pod_ips.sort()  # for assigning node numbers
    dn_port = config.get("dn_port")
    for pod_ip in pod_ips:
        dn_urls.append(f"http://{pod_ip}:{dn_port}")
    return dn_urls

async def docker_get_dn_urls(app):
    """ get list of dn_urls by making request to head node """
    head_url = getHeadUrl(app)
    if not head_url:
        log.warn("head_url is not set, can not register yet")
        return
    req_reg = head_url + "/register"
    log.info(f"register: {req_reg}")

    body = {"id": app["id"], "port": app["node_port"], "node_type": app["node_type"]}

    app['register_time'] = int(time.time())
    dn_urls = []
    try:
        log.info(f"register req: {req_reg} body: {body}")
        rsp_json = await http_post(app, req_reg, data=body)
        if rsp_json is not None:
            log.info(f"register response: {rsp_json}")
            dn_urls = rsp_json["dn_urls"]
            dn_urls.sort()
        if not app["node_ip"]:
            req_ip = rsp_json["req_ip"]
            log.info(f"setting node ip to: {req_ip}")
            app["node_ip"] = req_ip

    except HTTPInternalServerError:
        log.error("HEAD node seems to be down.")
    except OSError:
        log.error("failed to register")
    return dn_urls


async def get_dn_urls(app):
    """ get http urls for each dn node """
    if "oio_proxy" in app:
        #  Using OpenIO consicience daemons
        dn_urls = await oio_get_dn_urls(app)
    elif "is_k8s" in app:  
        dn_urls = k8s_get_dn_urls(app)
    else:
        # docker
        dn_urls = await docker_get_dn_urls(app)
    return dn_urls

def updateReadyState(app, dn_urls):
    """ update node state (and node_number and node_count) based on number of dn_urls available """
    log.debug(f"updateNodeState for dn_urls: {dn_urls}")
    if len(dn_urls) == 0:
        if app["node_type"] == "dn":
            log.error("no dn_urls returned from dn node!")
        if app["node_state"] != "INITIALIZING":
            log.info(f"setting node_state from {app['node_state']} to INITIALIZING since there are no dn nodes")
            app["node_state"] = "INITIALIZING"
    elif app["node_type"] == "dn":
        node_number = getNodeNumber(app, dn_urls)
        if app["node_number"] != node_number:
            old_number = app["node_number"]
            log.info(f"node_number has changed - old value was {old_number} new number is {node_number}")
            meta_cache = app["meta_cache"]
            chunk_cache = app["chunk_cache"]
            dirty_cache_count = meta_cache.dirtyCount + chunk_cache.dirtyCount
            if dirty_cache_count > 0:
                # set the node state to waiting till the chunk cache have been flushed
                log.info(f"Waiting on {dirty_cache_count} cache items to be flushed")
                if app["node_state"] == "READY":
                    log.info("Setting node_state to WAITING (was READY)")
                    app["node_state"] = "WAITING"
            else:
                # flush remaining items from cache
                meta_cache.clearCache()
                chunk_cache.clearCache()
                log.info(f"setting node_number to: {node_number}, node_state to READY")
                app["node_number"] = node_number
                app["node_state"] = "READY"
    else:
        # sn node with at least one dn node
        old_count = getNodeCount(app)
        new_count = len(dn_urls)
        if old_count != new_count:
            log.info(f"number of dn nodes has changed from {old_count} to {new_count}")
        if app["node_state"] != "READY":
            log.info(f"setting node_state from {app['node_state']} to READY")
            app["node_state"] = "READY"
    app["dn_urls"] = dn_urls

async def healthCheck(app):
    """ Periodic method that either registers with headnode (if state in INITIALIZING) or
    calls headnode to verify vitals about this node (otherwise)"""

    # let the server event loop startup before starting the health check
    await asyncio.sleep(1)
    log.info("health check start")
    sleep_secs = config.get("node_sleep_time")
    chaos_die = config.get("chaos_die")

    while True:
        node_state = app["node_state"]
        if node_state == "READY" and chaos_die > 0:
            if random.randint(0, chaos_die) == 0:
                log.error("chaos die - suicide!")
                sys.exit(1)
            else:
                log.info("chaos die - still alive")
        log.info(f"healthCheck - node_state: {node_state}")
        if node_state != "TERMINATING":
            dn_urls = await get_dn_urls(app)
            updateReadyState(app, dn_urls)
          
        svmem = psutil.virtual_memory()
        num_tasks = len(asyncio.Task.all_tasks())
        active_tasks = len([task for task in asyncio.Task.all_tasks() if not task.done()])
        log.debug(f"health check sleep: {sleep_secs}, vm: {svmem.percent} num tasks: {num_tasks} active tasks: {active_tasks}")
        await asyncio.sleep(sleep_secs)

async def preStop(request):
    """ HTTP Method used by K8s to signal the container is shutting down """

    log.request(request)
    app = request.app
    app["node_state"] = "TERMINATING"
    log.warn("preStop request setting node_state to TERMINATING")

    resp = await jsonResponse(request, {})
    log.response(request, resp=resp)
    return resp

async def about(request):
    """ HTTP Method to return general info about the service """
    log.request(request)

    app = request.app
    (username, pswd) = getUserPasswordFromRequest(request)
    if username:
        await validateUserPassword(app, username, pswd)
    answer = {}
    answer['start_time'] =  app["start_time"]
    answer['state'] = app['node_state']
    answer["hsds_version"] = getVersion()
    answer["name"] = config.get("server_name")
    answer["greeting"] = config.get("greeting")
    answer["about"] = config.get("about")
    answer["node_count"] = getNodeCount(app)

    resp = await jsonResponse(request, answer)
    log.response(request, resp=resp)
    return resp

async def info(request):
    """HTTP Method to return node state to caller"""
    log.request(request)
    app = request.app
    answer = {}
    # copy relevant entries from state dictionary to response
    node = {}
    node['id'] = request.app['id']
    node['type'] = app['node_type']
    node['start_time'] =  app["start_time"] #unixTimeToUTC(app['start_time'])
    node['state'] = app['node_state']
    if app['node_type'] == 'dn':
        node['node_number'] = app['node_number']
    node['node_count'] = getNodeCount(app)

    answer["node"] = node
    # psutil info
    # see: http://pythonhosted.org/psutil/ for description of different fields
    cpu = {}
    cpu["percent"] = psutil.cpu_percent()
    cpu["cores"] = psutil.cpu_count()
    answer["cpu"] = cpu
    diskio = psutil.disk_io_counters()
    disk_stats = {}
    disk_stats["read_count"] = diskio.read_count
    disk_stats["read_time"] = diskio.read_time
    disk_stats["read_bytes"] = diskio.read_bytes
    disk_stats["write_count"] = diskio.write_count
    disk_stats["write_time"] = diskio.write_time
    disk_stats["write_bytes"] = diskio.write_bytes
    answer["diskio"] = disk_stats
    netio = psutil.net_io_counters()
    net_stats = {}
    net_stats["bytes_sent"] = netio.bytes_sent
    net_stats["bytes_sent"] = netio.bytes_recv
    net_stats["packets_sent"] = netio.packets_sent
    net_stats["packets_recv"] = netio.packets_recv
    net_stats["errin"] = netio.errin
    net_stats["errout"] = netio.errout
    net_stats["dropin"] = netio.dropin
    net_stats["dropout"] = netio.dropout
    answer["netio"] = net_stats
    mem_stats = {}
    svmem = psutil.virtual_memory()
    mem_stats["phys_total"] = svmem.total
    mem_stats["phys_available"] = svmem.available
    sswap = psutil.swap_memory()
    mem_stats["swap_total"] = sswap.total
    mem_stats["swap_used"] = sswap.used
    mem_stats["swap_free"] = sswap.free
    mem_stats["percent"] = sswap.percent
    answer["memory"] = mem_stats
    disk_stats = {}
    sdiskusage = psutil.disk_usage('/')
    disk_stats["total"] = sdiskusage.total
    disk_stats["used"] = sdiskusage.used
    disk_stats["free"] = sdiskusage.free
    disk_stats["percent"] = sdiskusage.percent
    answer["disk"] = disk_stats
    answer["log_stats"] = app["log_count"]
    answer["req_count"] = app["req_count"]
    if "s3_stats" in app:
        answer["s3_stats"] = app["s3_stats"]
    elif "azure_stats" in app:
        answer["azure_stats"] = app["azure_stats"]
    mc_stats = {}
    if "meta_cache" in app:
        mc = app["meta_cache"]  # only DN nodes have this
        mc_stats["count"] = len(mc)
        mc_stats["dirty_count"] = mc.dirtyCount
        mc_stats["utililization_per"] = mc.cacheUtilizationPercent
        mc_stats["mem_used"] = mc.memUsed
        mc_stats["mem_target"] = mc.memTarget
    answer["meta_cache_stats"] = mc_stats
    cc_stats = {}
    if "chunk_cache" in app:
        cc = app["chunk_cache"]  # only DN nodes have this
        cc_stats["count"] = len(cc)
        cc_stats["dirty_count"] = cc.dirtyCount
        cc_stats["utililization_per"] = cc.cacheUtilizationPercent
        cc_stats["mem_used"] = cc.memUsed
        cc_stats["mem_target"] = cc.memTarget
    answer["chunk_cache_stats"] = cc_stats
    dc_stats = {}
    if "domain_cache" in app:
        dc = app["domain_cache"]  # only DN nodes have this
        dc_stats["count"] = len(dc)
        dc_stats["dirty_count"] = dc.dirtyCount
        dc_stats["utililization_per"] = dc.cacheUtilizationPercent
        dc_stats["mem_used"] = dc.memUsed
        dc_stats["mem_target"] = dc.memTarget
    answer["domain_cache_stats"] = dc_stats

    resp = await jsonResponse(request, answer)
    log.response(request, resp=resp)
    return resp


def baseInit(node_type):
    """Intitialize application and return app object"""

    log.info("Application baseInit")
    app = Application() 

    # set a bunch of global state
    node_id = createNodeId(node_type)
    app["id"] = node_id
    app["node_state"] = "INITIALIZING"
    app["node_type"] = node_type
    app["node_number"] = -1
    app["start_time"] = int(time.time())  # seconds after epoch
    app['register_time'] = 0
    bucket_name = config.get("bucket_name")
    if bucket_name:
        log.info(f"using bucket: {bucket_name}")
    else:
        log.info("no default bucket defined")
    app["bucket_name"] = bucket_name
    app["dn_urls"] = []
    counter = {}
    counter["GET"] = 0
    counter["PUT"] = 0
    counter["POST"] = 0
    counter["DELETE"] = 0
    counter["num_tasks"] = 0
    app["req_count"] = counter
    counter = {}
    counter["DEBUG"] = 0
    counter["INFO"] = 0
    counter["WARN"] = 0
    counter["ERROR"] = 0
    app["log_count"] = counter

    # check to see if we are running in a DCOS cluster
    if "MARATHON_APP_ID" in os.environ:
        log.info("Found MARATHON_APP_ID environment variable, setting is_dcos to True")
        app["is_dcos"] = True
    
    if "OIO_PROXY" in os.environ:
        app["oio_proxy"] = os.environ["OIO_PROXY"]
    if "HOST_IP" in os.environ: 
        app["node_ip"] = os.environ["HOST_IP"]
    elif "is_dcos" in app:
        # see https://github.com/mesosphere/marathon/issues/1198
        if "LIBPROCESS_IP" not in os.environ:
            msg = "Expected LIBPROCESS_IP environment variable for DCOS"
            log.error(msg)
            node_ip = "127.0.0.1"
        else:
            node_ip = os.environ['LIBPROCESS_IP']
    else:
        node_ip = ""  # will be set at register time
    if node_ip:
        log.info(f"Setting node_ip to: {node_ip}")
    app["node_ip"] = node_ip

    if "is_dcos" in app:
        if "PORT0" not in os.environ:
            msg = "Expected PORT0 environment variable for DCOS"
            log.error(msg)
            node_port = config.get(node_type + "_port")
        else: 
            node_port = os.environ['PORT0']
    else:
        node_port = config.get(node_type + "_port")
    app["node_port"] = node_port

    app["custer_state"] = "WAITING"

    # check to see if we are running in a k8s cluster
    try:
        k8s_app_label = config.get("k8s_app_label")
        if "KUBERNETES_SERVICE_HOST" in os.environ: 
            log.info("running in kubernetes")
            if k8s_app_label:
                log.info("setting is_k8s to True")
                app["is_k8s"] = True
            else:
                log.info("k8s_app_label not set, running in k8s single pod")
    except KeyError:
        # guard against KeyError since k8s_app_label is a recent key
        log.warn("expected to find key k8s_app_label in config")

    if "is_k8s" in app:
        # get the pod ip
        if "POD_IP" not in os.environ:
            log.error("Runing in Kubernetes by POD_IP environment not set")
        else:
            pod_ip = os.environ["POD_IP"]
            log.info(f"Kubernetes - using pod_ip: {pod_ip}")
            app["node_ip"] = pod_ip
    try:
        aws_iam_role = config.get("aws_iam_role")
        log.info(f"aws_iam_role set to: {aws_iam_role}")
    except KeyError:
        log.info("aws_iam_role not set")
    try:
        aws_secret_access_key = config.get("aws_secret_access_key")
        if aws_secret_access_key == "xxx":
            log.info("aws_secret_access_key not set")
        else:
            log.info("aws_secret_access_key set")
    except KeyError:
        log.info("aws_secret_access_key not set")
    try:
        aws_access_key_id = config.get("aws_access_key_id")
        if aws_access_key_id == "xxx":
            log.info("aws_access_key_id not set")
        else:
            log.info("aws_access_key_id set")
    except KeyError:
        log.info("aws_access_key_id not set")
    try:
        aws_region = config.get("aws_region")
        log.info(f"aws_region set to: {aws_region}")
    except KeyError:
        log.info("aws_region not set")


    if not config.get('standalone_app'):
        log.app = app

    app.router.add_get('/info', info)
    app.router.add_get('/about', about)



    return app
