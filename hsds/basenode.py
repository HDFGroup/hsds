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


import config
from util.httpUtil import http_get, http_post, jsonResponse
from util.idUtil import createNodeId
from util.s3Util import getInitialS3Stats 
from util.authUtil import getUserPasswordFromRequest, validateUserPassword
import hsds_logger as log

HSDS_VERSION = "0.4"

def getVersion():
    return HSDS_VERSION

def getHeadUrl(app):
    head_url = None
    if "oio_proxy" in app:
        head_url = app["oio_proxy"]
    else:
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

async def oio_register(app):
    """ register with oio conscience 
    """
    log.info("oio_register")
    oio_proxy = config.get("oio_proxy")
    host_ip = config.get("host_ip")
    if not host_ip:
        log.error("host ip not set")
        return
    node_type = app["node_type"]
    if node_type not in ("sn", "dn"):
        log.error("unexpected node type")
        return
    service_name = "hdf" + node_type
    req = oio_proxy + "/v3.0/" + service_name + "/conscience/register"
    log.info(f"conscience register: {req}")
    body = {
        "addr": host_ip + ":" + str(app["node_port"]),
        "tags": { "stat.cpu": 100, "stat.idle": 100, "stat.io": 100 },
        "type": service_name
    }
    rsp_json = await http_post(app, req, data=body)
    log.info(f"got response: {rsp_json}")


async def check_conscience(app):
    oio_proxy = config.get("oio_proxy")
    if not oio_proxy:
        log.error("oio_proxy environment not set, failed to register")
        return 
    await oio_register(app, oio_proxy)


async def healthCheck(app):
    """ Periodic method that either registers with headnode (if state in INITIALIZING) or 
    calls headnode to verify vitals about this node (otherwise)"""
    log.info("health check start")
    sleep_secs = config.get("node_sleep_time")

    head_url = getHeadUrl(app)
    while True:
        print("node_state:", app["node_state"])
        if app["node_state"] == "INITIALIZING" or (app["node_state"] == "WAITING" and app["node_number"] < 0):
            if config.get("oio_proxy"):
                await oio_register(app)
            else:
                await register(app)
        elif config.get("oio_proxy"):
            log.info("todo: conscience healthcheck")
        else:
            # check in with the head node and make sure we are still active
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
    app["id"] = createNodeId(node_type)
    app["node_state"] = "INITIALIZING"
    app["node_type"] = node_type
    app["node_port"] = config.get(node_type + "_port")
    app["node_number"] = -1
    app["node_count"] = -1
    app["start_time"] = int(time.time())  # seconds after epoch
    app['register_time'] = 0
    bucket_name = config.get("bucket_name")
    if bucket_name:
        log.info("using bucket: {}".format(bucket_name))
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
    
    log.app = app
    # save session object
    session = get_session(loop=loop)
    app["session"] = session
    app["loop"] = loop

    app.router.add_get('/info', info)
    app.router.add_get('/about', about)
      
    return app
