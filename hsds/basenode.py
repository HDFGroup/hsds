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
import time
import psutil
from copy import copy

from aiohttp.web import Application
from aiohttp.web_exceptions import HTTPNotFound, HTTPGone, HTTPInternalServerError

from aiohttp.client_exceptions import ClientError
from aiobotocore import get_session
from asyncio import CancelledError


import config
from util.httpUtil import http_get, http_post, jsonResponse
from util.idUtil import createNodeId
from util.s3Util import getInitialS3Stats 
from util.authUtil import getUserPasswordFromRequest, validateUserPassword
import hsds_logger as log
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config

HSDS_VERSION = "0.4"

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

async def register(app):
    """ register node with headnode
    OK to call idempotently (e.g. if the headnode seems to have forgotten us)"""
    head_url = getHeadUrl(app)
    if not head_url:
        log.warn("head_url is not set, can not register yet")
        return
    req_reg = head_url + "/register"
    log.info("register: {}".format(req_reg))
   
    body = {"id": app["id"], "port": app["node_port"], "node_type": app["node_type"]}
    app['register_time'] = int(time.time())
    try:
        log.debug("register req: {} body: {}".format(req_reg, body))
        rsp_json = await http_post(app, req_reg, data=body)     
        if rsp_json is not None:
            log.debug("register response: {}".format(rsp_json))
            app["node_number"] = rsp_json["node_number"]
            app["node_count"] = rsp_json["node_count"]
            log.info("setting node_state to WAITING")
            app["node_state"] = "WAITING"  # wait for other nodes to be active
    except OSError:
        log.error("failed to register")

async def get_info(app, url):
    """
    Invoke the /info request on the indicated url and return the response
    """
    req = url + "/info"
    log.info(f"get_info({url})")
    try:
        rsp_json = await http_get(app, req)
        if "node" not in rsp_json:
            log.error("Unexpected response from node")
            return None
                
    except OSError as ose:
        log.warn("OSError for req: {}: {}".format(req, str(ose)))
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
        log.warn("Timeout error for req: {}: {}".format(req, str(toe)))
        # node has gone away?
        return None
    return rsp_json
                 

async def oio_register(app):
    """ register with oio conscience 
    """
    log.info("oio_register")
    
    oio_proxy = app["oio_proxy"]
    host_ip = app["host_ip"]
    if not host_ip:
        log.error("host ip not set")
        return
    node_type = app["node_type"]
    if node_type not in ("sn", "dn"):
        log.error("unexpected node type")
        return
    service_name = "hdf" + node_type
    req = oio_proxy + "/v3.0/OPENIO/conscience/register"
    
    body = {
        "addr": host_ip + ":" + str(app["node_port"]),
        "tags": { "stat.cpu": 100, "tag.up": True},
        "type": service_name
    }
    log.debug(f"conscience register: body: {body}")
    try:
        await http_post(app, req, data=body)
    except ClientError as client_exception:
        log.error(f"got ClientError registering with oio_proxy: {client_exception}")
        return
    except CancelledError as cancelled_exception:
        log.error(f"got CanceeledError registering with oio_proxy: {cancelled_exception}")
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
        log.error(f"got CanceeledError listing dn nodes with oio_proxy: {cancelled_exception}")
        return
    log.info(f"got {len(dn_node_list)} conscience list items")
    # create map keyed by dn addr
    dn_node_map = {}
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
        # send an info request to the node
        info_rsp = await get_info(app, "http://" + addr)
        if not info_rsp:
            # timeout or other failure
            continue
        if "node" not in info_rsp:
            log.error("expecteed to find node key in info resp")
            continue
        info_node = info_rsp["node"]
        log.debug(f"got info resp: {info_node}")
        for key in ("type", "id", "node_number", "node_count"):
            if key not in info_node:
                log.error(f"unexpected node type in node state, expected to find key: {key}")
                continue
        if info_node["type"] != "dn":
            log.error(f"expecteed node_type to be dn")
            continue
        # mix in node id, node number, node_count to the conscience info
        dn_node["node_id"] = info_node["id"]
        dn_node["node_number"] = info_node["node_number"]
        dn_node["node_count"] = info_node["node_count"]

        dn_node_map["addr"] = dn_node

    log.info(f"done with dn_node_list, got: {len(dn_node_map)} active nodes")
    if len(dn_node_map) == 0:
        if app["node_state"] != "INITIALIZING":
            log.info("no active DN nodes, setting cluster state to INITIALIZING")
            app["node_state"] = "INITIALIZING"
        return

    # sort map by address
    addrs = list(dn_node_map.keys())
    addrs.sort()

    # check that node number is set and is the expected value for each node key
    invalid_count = 0
    node_index = 0
    node_count = len(addrs)
    dn_urls = {}
    this_node_found = False
    this_node_id = app["id"]
    for addr in addrs:
        dn_node = dn_node_map[addr]
        log.debug(f"dn_node for index {node_index}: {dn_node}")
        node_id = dn_node["node_id"]
        if node_id == this_node_id:
            this_node_found = True
        node_number = dn_node["node_number"]
        dn_urls[node_number] = "http://" + dn_node["addr"]
        if node_index != node_number or dn_node["node_count"] != node_count:
            if node_number == -1:
                log.info(f"node {node_index} not yet initialized")
            elif node_index != node_number:
                log.warn(f"node_id {node_id}, expected node_number of {node_index} but found {node_number}")
            invalid_count += 1
            if node_id == app["id"]:
                # this is us, update our node_number, node_count
                if app["node_number"] != node_index:
                    # TBD - clean cache items
                    log.info(f"setting node_number for this node to: {node_index}")
                    app["node_number"] = node_index
                if app["node_count"] != node_count:
                    # TBD - clean cache items
                    log.info(f"setting node_count for this node to: {node_count}")
                    app["node_count"] = node_count
            invalid_count += 1
        else:
            log.debug(f"node {node_id} node number is correct")
        node_index += 1

    if invalid_count == 0:
        log.debug("no invalid nodes!")
        if app["node_state"] != "READY":
            if app["node_type"] == "dn" and not this_node_found:
                # don't go to READY unless this node shows up
                log.info(f"node {this_node_id} not yet showing in proxy list, stay in INITIALIZING")
            else:
                log.info("setting node state to READY")
                app["node_state"] = "READY"
                if app["node_type"] == "sn" and app["node_number"] == -1:
                    # node number shouldn't matter for SN nodes, so set to 1
                    app["node_number"] = 1
        if app["node_count"] != node_count:
            log.info(f"setting node_count to: {node_count}")
            app["node_count"] = node_count
        app["dn_urls"] = dn_urls
    else:
        log.debug(f"number invalid nodes: {invalid_count}")
        if app["node_state"] == "READY":
            log.warn("invalid nodes found, setting node state to INITIALIZING")
            app["node_state"] = "INITIALIZING"

    log.info("oio_register done")

async def k8s_register(app):
    log.info("k8s_register")
    # TBD - find more elegant way to avoid this warning
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    k8s_config.load_incluster_config() #get the config from within the cluster and set it as the default config for all new clients
    c=k8s_client.Configuration() #go and get a copy of the default config
    c.verify_ssl=False #set verify_ssl to false in that config
    k8s_client.Configuration.set_default(c) #make that config the default for all new clients
    v1 = k8s_client.CoreV1Api()
    # TBD - use the async version
    ret = v1.list_pod_for_all_namespaces(watch=False)
    pod_ips = []
    sn_urls = {}
    dn_urls = {}
    for i in ret.items:
        pod_ip = i.status.pod_ip
        if not pod_ip:
            continue
        labels = i.metadata.labels
        if "app" in labels and labels["app"] == "hsds":
            log.info(f"hsds pod - ip: {pod_ip}")
            pod_ips.append(pod_ip)
    if not pod_ips:
        log.error("Expected to find at least one hsds pod")
        return
    pod_ips.sort()  # for assigning node numbers
    node_count = len(pod_ips)
    ready_count = 0
    this_node_id = app["id"]
    sn_port = config.get("sn_port")
    dn_port = config.get("dn_port")
    for node_number in range(node_count):
        for port in (sn_port, dn_port):
            # send an info request to the node
            pod_ip = pod_ips[node_number]
            url = f"http://{pod_ip}:{port}"
            if port == sn_port:
                sn_urls[node_number] = url
            else:
                dn_urls[node_number] = url

            info_rsp = await get_info(app, url)
            if not info_rsp:
                # timeout or other failure
                continue
            if "node" not in info_rsp:
                log.error("expecteed to find node key in info resp")
                continue
      
            node_rsp = info_rsp["node"]
            log.debug(f"got info resp: {node_rsp}")
            for key in ("type", "id", "node_number", "node_count"):
                if key not in node_rsp:
                    log.error(f"unexpected node type in node state, expected to find key: {key}")
                    continue
            if node_rsp["type"] not in ("sn", "dn"):
                log.error(f"expecteed node_type to be sn or dn")
                continue
            node_id = node_rsp["id"]
            if node_id == this_node_id:
                # set node_number and node_count
                if app["node_number"] != node_number:
                    old_number = app["node_number"]
                    # TBD - invalidate cache state for dn nodes
                    log.info(f"node number was: {old_number} setting to: {node_number}")
                    app["node_number"] = node_number
                    app['register_time'] = time.time()
                if app["node_count"] != node_count:
                    old_count = app["node_count"]
                    log.info(f"node count was: {old_count} setting to: {node_count}")
                    app["node_count"] = node_count
            if node_number == node_rsp["node_number"] and node_count == node_rsp["node_count"]:
                ready_count += 1

    if ready_count == node_count*2:
        if app["node_state"] != "READY":
            log.info("setting node state to READY")
            app["node_state"] = "READY"
        app["node_count"] = node_count
        app["sn_urls"] = sn_urls
        app["dn_urls"] = dn_urls
    else:
        log.info(f"ready_count: {ready_count}/{node_count}")
        if app["node_state"] == "READY":
            log.info("setting node state to INITIALIZING")
            app["node_state"] = "INITIALIZING"
        

async def healthCheck(app):
    """ Periodic method that either registers with headnode (if state in INITIALIZING) or 
    calls headnode to verify vitals about this node (otherwise)"""
    
    # let the server event loop startup before sarting the health check
    await asyncio.sleep(1)
    log.info("health check start")
    sleep_secs = config.get("node_sleep_time")

    while True:
        print("node_state:", app["node_state"])
        if "oio_proxy" in app:
            # for OIO post registration request every time interval
            await oio_register(app)
        elif "is_k8s" in app:
            await k8s_register(app) 

        elif app["node_state"] == "INITIALIZING" or (app["node_state"] == "WAITING" and app["node_number"] < 0):
            # startup docker registration
            await register(app)
        else:
            # check in with the head node and make sure we are still active
            head_url = getHeadUrl(app)
            req_node = "{}/nodestate".format(head_url)
            log.debug("health check req {}".format(req_node))
            try:
                rsp_json = await http_get(app, req_node)
                if rsp_json is None or not isinstance(rsp_json, dict):
                    log.warn("invalid health check response: type: {} text: {}".format(type(rsp_json), rsp_json))
                else:
                    log.debug("cluster_state: {}".format(rsp_json["cluster_state"]))
                    if rsp_json["cluster_state"] != "READY" and app["node_state"] == "READY":
                        log.info("changing node_state to WAITING")
                        app["node_state"] = "WAITING"

                    #print("rsp_json: ", rsp_json)
                    # save the url's to each of the active nodes'
                    sn_urls = {}
                    dn_urls = {}
                    #  or rsp_json["host"] is None or rsp_json["id"] != app["id"]
                    this_node = None
                    for node in rsp_json["nodes"]:
                        if node["node_type"] == app["node_type"] and node["node_number"] == app["node_number"]:
                            # this should be this node
                           
                            if node["id"] != app["id"]:
                                # flag - to re-register
                                log.warn("mis-match node ids, app: {} vs head: {} - re-initializing".format(node["id"], app["id"]))
                                app["node_state"] == "INITIALIZING"
                                app["node_number"] = -1
                                break
                            if not node["host"]:
                                # flag - to re-register
                                log.warn("host not set for this node  - re-initializing".format(node["id"], app["id"]))
                                app["node_state"] == "INITIALIZING"
                                app["node_number"] = -1
                                break
                        if not node["host"]:
                            continue  # not online
                        this_node = copy(node)
                        url = "http://" + node["host"] + ":" + str(node["port"])
                        node_number = node["node_number"]
                        if node["node_type"] == "dn":
                            dn_urls[node_number] = url
                        elif node["node_type"] == "sn":
                            sn_urls[node_number] = url
                        else:
                            log.error("Unexpected node_type for node: {}".format(node))
                    app["sn_urls"] = sn_urls
                    app["dn_urls"] = dn_urls
                     
                    if this_node is None  and rsp_json["cluster_state"] != "READY":
                        log.warn("this node not found, re-initialize")
                        app["node_state"] == "INITIALIZING"
                        app["node_number"] = -1
                        
                    if app["node_state"] == "WAITING" and rsp_json["cluster_state"] == "READY" and app["node_number"] >= 0:
                        log.info("setting node_state to READY, node_number: {}".format(app["node_number"]))
                        app["node_state"]  = "READY"
                    log.info("health check ok") 
            except ClientError as ce:
                log.warn(f"ClientError: {ce} for health check")
            except HTTPInternalServerError as he:
                log.warn(f"HTTPInternalServiceError <{he.code}> for health check")
            except HTTPNotFound as hnf:
                log.warn(f"HTTPNotFound <{hnf.code}> for health check")
            except HTTPGone as hg:
                log.warn(f"HTTPGone <{hg.code}> for health heck")

        svmem = psutil.virtual_memory()
        num_tasks = len(asyncio.Task.all_tasks())
        active_tasks = len([task for task in asyncio.Task.all_tasks() if not task.done()])
        log.debug(f"health check sleep: {sleep_secs}, vm: {svmem.percent} num tasks: {num_tasks} active tasks: {active_tasks}") 
        await asyncio.sleep(sleep_secs)

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
    answer["node_count"] = app["node_count"] 
    
    resp = await jsonResponse(request, answer) 
    log.response(request, resp=resp)
    return resp

async def info(request):
    """HTTP Method to retun node state to caller"""
    log.debug("info request")
    app = request.app
    answer = {}
    # copy relevant entries from state dictionary to response
    node = {}
    node['id'] = request.app['id']
    node['type'] = request.app['node_type']
    node['start_time'] =  app["start_time"] #unixTimeToUTC(app['start_time'])
    node['state'] = app['node_state'] 
    node['node_number'] = app['node_number']
    node['node_count'] = app['node_count']
    
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
    answer["s3_stats"] = app["s3_stats"]
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


def baseInit(loop, node_type):
    """Intitialize application and return app object"""
    log.info("Application baseInit")
    app = Application(loop=loop)

    # set a bunch of global state 
    node_id = createNodeId(node_type)
    app["id"] = node_id
    app["node_state"] = "INITIALIZING"
    app["node_type"] = node_type
    node_port = config.get(node_type + "_port")
    app["node_port"] = config.get(node_type + "_port")
    log.info(f"baseInit - node_id: {node_id} node_port: {node_port}")
    app["node_number"] = -1
    app["node_count"] = -1
    app["start_time"] = int(time.time())  # seconds after epoch
    app['register_time'] = 0
    bucket_name = config.get("bucket_name")
    if bucket_name:
        log.info(f"using bucket: {bucket_name}")
    else:
        log.info("no default bucket defined")
    app["bucket_name"] = bucket_name
    app["sn_urls"] = {}
    app["dn_urls"] = {}
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
 
    app["s3_stats"] = getInitialS3Stats()

    if config.get("oio_proxy"):
        app["oio_proxy"] = config.get("oio_proxy")
    if config.get("host_ip"):
        app["host_ip"] = config.get("host_ip")
    else:
        app["host_ip"] = "127.0.0.1"

    # check to see if we are running in a k8s cluster
    #if ospath.exists("/var/run/secrets/kubernetes.io") or True:
    if config.get("KUBERNETES_SERVICE_HOST"):
        log.info("running in kubernetes")
        app["is_k8s"] = True
    
    log.app = app
    # save session object
    session = get_session(loop=loop)
    app["session"] = session
    app["loop"] = loop

    app.router.add_get('/info', info)
    app.router.add_get('/about', about)
      
    return app
