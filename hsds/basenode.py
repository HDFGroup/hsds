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
import time
import psutil
from copy import copy

from aiohttp.web import Application, StreamResponse
from aiohttp.errors import ClientError, HttpProcessingError
from aiobotocore import get_session
 

import config
#from util.timeUtil import unixTimeToUTC, elapsedTime
from util.httpUtil import http_get_json, http_post, jsonResponse
from util.idUtil import createNodeId
from util.s3Util import getS3JSONObj, getInitialS3Stats 
from util.idUtil import getHeadNodeS3Key
from util.authUtil import getUserPasswordFromRequest, validateUserPassword
import hsds_logger as log

HSDS_VERSION = "0.1"


async def getHeadUrl(app):
    head_url = None
    if head_url in app:
        head_url = app["head_url"]
    elif config.get("head_endpoint"):
        head_url = config.get("head_endpoint")
    else:
        # pull url form headnode object in bucket
        headnode_key = getHeadNodeS3Key()
        head_state = await getS3JSONObj(app, headnode_key)
        if "head_url" not in head_state:
            msg = "head_url not found in head_state"
            log.error(msg)
        else:
            head_url = head_state["head_url"]
            app["head_url"] = head_url  # so we don't need to check S3 next time
    log.debug("head_url: {}".format(head_url))
    return head_url



async def register(app):
    """ register node with headnode
    OK to call idempotently (e.g. if the headnode seems to have forgotten us)"""
    head_url = await getHeadUrl(app)
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


async def healthCheck(app):
    """ Periodic method that either registers with headnode (if state in INITIALIZING) or 
    calls headnode to verify vitals about this node (otherwise)"""
    log.info("health check start")
    sleep_secs = config.get("node_sleep_time")
    head_url = await getHeadUrl(app)
    while True:
        if app["node_state"] == "INITIALIZING" or (app["node_state"] == "WAITING" and app["node_number"] < 0):
            await register(app)
        else:
            # check in with the head node and make sure we are still active
            req_node = "{}/nodestate".format(head_url)
            log.debug("health check req {}".format(req_node))
            try:
                rsp_json = await http_get_json(app, req_node)
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
                    an_url = None
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
                        elif node["node_type"] == "an":
                            an_url = url
                        else:
                            log.error("Unexpected node_type for node: {}".format(node))
                    app["sn_urls"] = sn_urls
                    app["dn_urls"] = dn_urls
                    app["an_url"] = an_url
                     
                    if this_node is None  and rsp_json["cluster_state"] != "READY":
                        log.warn("this node not found, re-initialize")
                        app["node_state"] == "INITIALIZING"
                        app["node_number"] = -1
                        
                    if app["node_state"] == "WAITING" and rsp_json["cluster_state"] == "READY" and app["node_number"] >= 0:
                        log.info("setting node_state to READY, node_number: {}".format(app["node_number"]))
                        app["node_state"]  = "READY"
                    log.info("health check ok") 
            except ClientError as ce:
                log.warn("ClientError: {} for health check".format(str(ce)))
            except HttpProcessingError as he:
                log.warn("HttpProcessingError <{}> for health check".format(he.code))

        svmem = psutil.virtual_memory()
        log.debug("health check sleep: {}, vm: {}".format(sleep_secs, svmem.percent))
        await asyncio.sleep(sleep_secs)

async def about(request):
    """ HTTP Method to return general info about the service """
    log.request(request) 
    
    app = request.app
    (username, pswd) = getUserPasswordFromRequest(request)
    if username:
        validateUserPassword(app, username, pswd)
    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'
    answer = {}
    answer['start_time'] =  app["start_time"] #unixTimeToUTC(app['start_time'])
    answer['state'] = app['node_state'] 
    answer["hsds_version"] = HSDS_VERSION
    answer["name"] = config.get("server_name")
    answer["greeting"] = "Welcome to HSDS!"
    answer["about"] = "HSDS is a webservice for HSDS data"
    resp = await jsonResponse(request, answer) 
    log.response(request, resp=resp)
    return resp

async def info(request):
    """HTTP Method to retun node state to caller"""
    log.request(request)
    app = request.app
    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'
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
    if not bucket_name:
        log.error("BUCKET_NAME environment variable not set")
        sys.exit()
    log.info("using bucket: {}".format(bucket_name))
    app["bucket_name"] = bucket_name
    app["sn_urls"] = {}
    app["dn_urls"] = {}
    counter = {}
    counter["GET"] = 0
    counter["PUT"] = 0
    counter["POST"] = 0
    counter["DELETE"] = 0
    app["req_count"] = counter
    counter = {}
    counter["DEBUG"] = 0
    counter["INFO"] = 0
    counter["WARN"] = 0
    counter["ERROR"] = 0
    app["log_count"] = counter
 
    app["s3_stats"] = getInitialS3Stats()
    
    log.app = app
    # save session object
    session = get_session(loop=loop)
    app["session"] = session
    app["loop"] = loop

    app.router.add_get('/info', info)
    app.router.add_get('/about', about)
      
    return app
 