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
# Head node of hsds cluster
#
import os
import asyncio
import json
import time

from aiohttp.web import Application, StreamResponse, run_app, json_response
from aiohttp.web_exceptions import HTTPBadRequest, HTTPInternalServerError, HTTPServiceUnavailable, HTTPNotFound, HTTPException

from asyncio import TimeoutError

import config
from util.timeUtil import unixTimeToUTC, elapsedTime
from util.httpUtil import http_get, getUrl
from util.idUtil import  createNodeId
import hsds_logger as log
import util.query_marathon as marathonClient

NODE_STAT_KEYS = ("cpu", "diskio", "memory", "log_stats", "disk", "netio",
    "req_count", "s3_stats", "azure_stats", "chunk_cache_stats")

async def healthCheck(app):
    """ Periodic method that pings each active node and verifies it is still healthy.
    If node doesn't respond, free up the node slot (the node can re-register if it comes back)'.
    Note that an aio deprecation against changing state after initial app startup means we might have to do something like https://github.com/aio-libs/aiohttp/issues/3397#issuecomment-440943426 for some of the items currently tracking in app."""


    nodes = app["nodes"]
    while True:
        app["last_health_check"] = int(time.time())

        # sleep for a bit
        sleep_secs = config.get("head_sleep_time")
        await  asyncio.sleep(sleep_secs)

        now = int(time.time())
        log.info("health check {}".format(unixTimeToUTC(now)))

        fail_count = 0
        # keep track of where we are in the global node list for possible deletions
        node_seq_num = -1
        HEALTH_CHECK_RETRY_COUNT = 1 # times to try before calling a node dead
        #TODO note that this fail_count actually applies to any node.  Should the fail count be a hash of node_ids to a fail count?
        for node in nodes:
            node_seq_num += 1
            if node["host"] is None:
                fail_count += 1
                log.warn("Node found with missing host information.")
                continue
            url = getUrl(node["host"], node["port"]) + "/info"
            try:
                rsp_json = await http_get(app, url)
                if "node" not in rsp_json:
                    log.error("Unexpected response from node")
                    fail_count += 1
                    continue
                node_state = rsp_json["node"]
                node_id = node_state["id"]

                if node_id != node['id']:
                    log.warn("unexpected node_id: {} (expecting: {})".format(node_id, node['id']))
                    node['host'] = None
                    node['id'] = None
                    fail_count += 1
                    continue

                if 'number' in node_state and node_state['number'] != node['node_number']:
                    msg = "unexpected node_number got {} (expecting: {})"
                    log.warn(msg.format(node_state["number"], node['node_number']))
                    node['host'] = None
                    node['id'] = None
                    fail_count += 1
                    continue

                # save off other useful info from the node
                app_node_stats = app["node_stats"]
                node_stats = {}
                for k in NODE_STAT_KEYS:
                    if k in rsp_json:
                        node_stats[k] = rsp_json[k]
                app_node_stats[node_id] = node_stats
                # mark the last time we got a response from this node
                node["healthcheck"] = unixTimeToUTC(int(time.time()))
                node["failcount"] = 0 # rest
            except OSError as ose:
                log.warn("OSError for req: {}: {}".format(url, str(ose)))
                # node has gone away?
                node["failcount"] += 1
                if node["failcount"] >= HEALTH_CHECK_RETRY_COUNT:
                    log.warn("node {}:{} not responding".format(node["host"], node["port"]))
                    fail_count += 1

            except HTTPInternalServerError as hpe:
                log.warn("HTTPInternalServerError for req: {}: {}".format(url, str(hpe)))
                # node has gone away?
                node["failcount"] += 1
                if node["failcount"] >= HEALTH_CHECK_RETRY_COUNT:
                    log.warn("removing {}:{} from active list".format(node["host"], node["port"]))
                    fail_count += 1
            except TimeoutError as toe:
                log.warn("Timeout error for req: {}: {}".format(url, str(toe)))
                # node has gone away?
                node["failcount"] += 1
                if node["failcount"] >= HEALTH_CHECK_RETRY_COUNT:
                    log.warn("removing {}:{} from active list".format(node["host"], node["port"]))
                    fail_count += 1
            except HTTPServiceUnavailable as hsu:
                log.warn("HTTPServiceUnavailable error for req: {}: {}".format(url, str(hsu)))
                # node has gone away?
                node["failcount"] += 1
                if node["failcount"] >= HEALTH_CHECK_RETRY_COUNT:
                    log.warn("removing {}:{} from active list".format(node["host"], node["port"]))
                    fail_count += 1
            except HTTPNotFound as hnf:
                log.warn("HTTPException error for req: {}: {}".format(url, str(hnf)))
                # node has gone away?
                node["failcount"] += 1
                if node["failcount"] >= HEALTH_CHECK_RETRY_COUNT:
                    log.warn("removing {}:{} from active list".format(node["host"], node["port"]))
                    fail_count += 1
            except HTTPException as he:
                log.warn("HTTPException error for req: {}: {}".format(url, str(he)))
                # node has gone away?
                node["failcount"] += 1
                if node["failcount"] >= HEALTH_CHECK_RETRY_COUNT:
                    log.warn("removing {}:{} from active list".format(node["host"], node["port"]))
                    fail_count += 1
            except:
                log.warn("Unknown exception caught")
            finally:
                if node["failcount"] >= HEALTH_CHECK_RETRY_COUNT:
                    log.warn("Forgetting about node {}:{} due to too many failures.".format(node['host'], node['port']))
                    node['host'] = None
                    node['id'] = None
                    if node['node_type'] == "dn":
                        log.warn("Removed a DN")
                        del app['nodes'][node_seq_num]
                        del node
                        # best to just break to avoid weird modified loop variable behavior
                        break
                    elif node['node_type'] == "sn":
                        log.warn("Removed a SN")
                        del app['nodes'][node_seq_num]
                        del node
                        # best to just break to avoid weird modified loop variable behavior
                        break
                    else:
                        log.warn(f"Lost a node that wasn't a dn or sn, no action taken")
                    #We've handled this particular loop's failed node
                    fail_count -= 1
                    # check to see if the cluster is in the process of scaling down and reached its target
                    log.debug(f"After node removal, checking to see if we can go ready fail_count is {fail_count}, app['cluster_state'] is {app['cluster_state']}, app['target_dn_count'] is {app['target_dn_count']}, getTargetNodeCount(app, 'dn') is {await getTargetNodeCount(app, 'dn')}, getActiveNodeCount(app, 'dn') is {getActiveNodeCount(app, 'dn')}, getTargetNodeCount(app, 'sn') is {await getTargetNodeCount(app, 'sn')}, app['target_sn_count'] is {app['target_sn_count']}, getActiveNodeCount(app, 'sn') is {getActiveNodeCount(app, 'sn')}")
                    if fail_count == 0 and app["cluster_state"] != "READY" and app['target_dn_count'] == getActiveNodeCount(app, "dn") and app['target_sn_count'] == getActiveNodeCount(app, "sn"):
                        log.info("All nodes healthy at new cluster size, changing cluster state to READY")
                        app["cluster_state"] = "READY"

        log.info("node health check fail_count: {}".format(fail_count))

        if fail_count > 0:
            if app["cluster_state"] == "READY":
                # go back to INITIALIZING state until another node is registered
                log.warn("Fail_count > 0, Setting cluster_state from READY to INITIALIZING")
                app["cluster_state"] = "INITIALIZING"
        elif fail_count == 0 and app["cluster_state"] != "READY" and app['target_dn_count'] == getActiveNodeCount(app, "dn") and app['target_sn_count'] == getActiveNodeCount(app, "sn"):
            log.info("All nodes healthy, changing cluster state to READY")
            app["cluster_state"] = "READY"
        #else: all is well



async def info(request):
    """HTTP Method to return node state to caller"""
    log.request(request)
    app = request.app
    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'
    answer = {}
    # copy relevant entries from state dictionary to response
    answer['id'] = request.app['id']
    answer['start_time'] = unixTimeToUTC(app['start_time'])
    answer['last_health_check'] = unixTimeToUTC(app['last_health_check'])
    answer['up_time'] = elapsedTime(app['start_time'])
    answer['cluster_state'] = app['cluster_state']
    answer['bucket_name'] = app['bucket_name']
    answer['target_sn_count'] = await getTargetNodeCount(app, "sn")
    answer['active_sn_count'] = getActiveNodeCount(app, "sn")
    answer['target_dn_count'] = await getTargetNodeCount(app, "dn")
    answer['active_dn_count'] = getActiveNodeCount(app, "dn")

    resp = json_response(answer)
    log.response(request, resp=resp)
    return resp

async def register(request):
    """ HTTP method for nodes to register with head node"""
    log.request(request)
    app = request.app
    text = await request.text()
    # body = await request.json()
    body = json.loads(text)
    log.info("body: {}".format(body))
    if 'id' not in body:
        msg = "Missing 'id'"
        log.response(request, code=400, message=msg)
        raise HTTPBadRequest(reason=msg)
    if 'ip' not in body:
        peername = request.transport.get_extra_info('peername')
        host, req_port = peername
        log.info("register host: {}, port: {}".format(host, req_port))
        if peername is None:
            raise HTTPBadRequest(reason="Can not determine caller IP")
    else:
        #Specify the ip is useful in docker / DCOS situations, where in certain situations a 
        #docker private network IP might be used
        host = body["ip"]
        log.info("explicit specification of host: {}".format(host))
    if 'port' not in body:
        msg = "missing key 'port'"
        log.response(request, code=400, message=msg)
        raise HTTPBadRequest(reason=msg)
    if 'node_type' not in body:
        raise HTTPBadRequest(reason="missing key 'node_type'")
    if body['node_type'] not in ('sn', 'dn'):
        msg="invalid node_type"
        log.response(request, code=400, message=msg)
        raise HTTPBadRequest(reason=msg)

    nodes = None
    ret_node = None

    node_ids = app['node_ids']
    if body['id'] in node_ids:
        # already registered?
        log.warn("Node {} is already registered!!!  Something may be wrong.".format(body['id']))
        ret_node = node_ids[body['id']]
    else:
        log.debug(f"Node {body['id']} is unknown, may be a new node coming online.")
        nodes = app['nodes']
        app['active_sn_count'] = getActiveNodeCount(app, "sn")
        app['active_dn_count'] = getActiveNodeCount(app, "dn")

        # If the cluster has any failed nodes, replace them.  Otherwise, see if the cluster is in the process of growing.
        if(body['node_type'] == "dn" or body['node_type'] == "sn"):
            replacedNode = False
            for node in nodes:
                if node['host'] is None and node['node_type'] == body['node_type']:
                    # found a free node
                    log.info("Found free node reference: {}".format(node))
                    node['host'] = host
                    node['port'] = body["port"]
                    node['id'] =   body["id"]
                    node['connected'] = unixTimeToUTC(int(time.time()))
                    node['failcount'] = 0
                    ret_node = node
                    node_ids[body["id"]] = ret_node
                    replacedNode = True
                    break
            if not replacedNode:
                node = {"node_number": len(nodes) - 1,
                    "node_type": body['node_type'],
                    "host": body['ip'],
                    "port": body['port'],
                    "id": body['id'],
                    "connected": unixTimeToUTC(int(time.time())),
                    "failcount": 0}
                log.debug(f"Added node node_type {node['node_type']} host {node['host']} port {node['port']} id {node['id']} connected {node['connected']} failcount {node['failcount']}")
                nodes.append(node)
                ret_node = node
                node_ids[body["id"]] = ret_node
        else:
            log.warn("Only sn or dn nodes may be replaced or added to a cluster")

    if ret_node is None:
        log.info("no free node to assign")

    inactive_node_count = getInactiveNodeCount(app)
    log.info("inactive_node_count: {}".format(inactive_node_count))
    if inactive_node_count == 0:
        # all the nodes have checked in
        log.info(f"setting cluster state to ready - was: {app['cluster_state']}")
        app['cluster_state'] = "READY"

    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'
    answer = {}
    if ret_node is not None:
        answer["node_number"] = ret_node["node_number"]
    else:
        # all nodes allocated, let caller know it's in the reserve pool
        answer["node_number"] = -1

    #answer["node_count"] = app["target_dn_count"]
    answer["node_count"] = await getTargetNodeCount(app, body['node_type'])

    resp = json_response(answer)
    log.response(request, resp=resp)
    return resp

async def nodestate(request):
    """HTTP method to return information about registed nodes"""
    log.request(request)
    node_type = request.match_info.get('nodetype', '*')
    node_number = '*'
    if node_type != '*':
        node_number = request.match_info.get('nodenumber', '*')

    log.info("nodestate/{}/{}".format(node_type, node_number))
    if node_type not in ("sn", "dn", "*"):
        msg="invalid node_type"
        log.response(request, code=400, message=msg)
        raise HTTPBadRequest(reason=msg)

    app = request.app
    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'

    if node_number == '*':
        nodes = []
        for node in app["nodes"]:
            if node["node_type"] == node_type or node_type == "*":
                nodes.append(node)
                log.debug(f"Added a node in nodestate method, up to {len(nodes)} nodes.")
        answer = {"nodes": nodes }
    else:
         answer = {}
         for node in app["nodes"]:
            if node["node_type"] == node_type and str(node["node_number"]) == node_number:
                answer = node
                break
    answer["cluster_state"] = app["cluster_state"]
    resp = json_response(answer)
    log.response(request, resp=resp)
    return resp


async def nodeinfo(request):
    """HTTP method to return node stats (cpu usage, request count, errors, etc.) about registed nodes"""
    log.request(request)
    node_stat_keys = NODE_STAT_KEYS
    stat_key = request.match_info.get('statkey', '*')
    if stat_key != '*':
        if stat_key not in node_stat_keys:
             raise HTTPBadRequest(reason="invalid key: {}".format(stat_key))
        node_stat_keys = (stat_key,)

    app = request.app
    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'

    app_node_stats = app["node_stats"]
    dn_count = await getTargetNodeCount(app, "dn")
    sn_count = await getTargetNodeCount(app, "sn")

    answer = {}
    # re-assemble the individual node stats to arrays indexed by node number
    for stat_key in node_stat_keys:
        log.info("stat_key: {}".format(stat_key))
        stats = {}
        for node in app["nodes"]:
            node_number = node["node_number"]
            node_type = node["node_type"]
            if node_type not in ("sn", "dn"):
                log.error("unexpected node_type: {}".format(node_type))
                continue
            node_id = node["id"]
            log.info("app_node_stats: {}".format(app_node_stats))
            if node_id not in app_node_stats:
                log.info("node_id: {} not found in node_stats".format(node_id))
                continue
            node_stats = app_node_stats[node_id]
            if stat_key not in node_stats:
                log.info("key: {} not found in node_stats for node_id: {}".format(stat_key, node_id))
                continue
            stats_field = node_stats[stat_key]
            for k in stats_field:
                if k not in stats:
                    stats[k] = {}
                    stats[k]["sn"] = [0,] * sn_count
                    stats[k]["dn"] = [0,] * dn_count
                stats[k][node_type][node_number] = stats_field[k]
        answer[stat_key] = stats

    resp = json_response(answer)
    log.response(request, resp=resp)
    return resp

async def getTargetNodeCount(app, node_type):
    count = None
    prev_count = None
    marathon = marathonClient.MarathonClient(app)
    if node_type == "dn":
        prev_count = app['target_dn_count'] 
        if "is_dcos" in app:
            app["target_dn_count"] = int(await marathon.getDNInstances())
        else:
            app["target_dn_count"] = app['target_dn_count']
        count = app['target_dn_count']
    elif node_type == "sn":
        prev_count = app['target_sn_count'] 
        if "is_dcos" in app:
            app["target_sn_count"] = int(await marathon.getSNInstances())
        else:
            app["target_sn_count"] = app['target_sn_count']
        count = app['target_sn_count']
    if prev_count != count:
        app["cluster_state"] = "INITIALIZING"

    return count

def getTargetNodeCountBlocking(app, node_type):
    count = None
    if node_type == "dn":
        count = app['target_dn_count']
    elif node_type == "sn":
        count = app['target_sn_count']
    return count

def getActiveNodeCount(app, node_type):
    count = 0
    for node in app['nodes']:
        if node["node_type"] == node_type and node["host"] is not None:
            count += 1
    return count

def getInactiveNodeCount(app):
    count = 0
    for node in app['nodes']:
        if node['host'] is None:
            count += 1
    return count


def init():
    """Intitialize application and return app object """
    app = Application()

    # set a bunch of global state
    app["id"] = createNodeId("head")
    app["cluster_state"] = "INITIALIZING"
    app["start_time"] = int(time.time())  # seconds after epoch
    app["target_sn_count"] = int(config.get("target_sn_count"))
    app["target_dn_count"] = int(config.get("target_dn_count"))
    log.info("target_sn_count: {}".format(app["target_sn_count"]))
    log.info("target_dn_count: {}".format(app["target_dn_count"]))

    bucket_name = config.get("bucket_name")
    if bucket_name:
        log.info("using bucket: {}".format(bucket_name))
        app["bucket_name"] = bucket_name
    else:
        log.info("No default bucket name is set")

    app["head_host"] = config.get("head_host")
    app["head_port"] = config.get("head_port")

    nodes = []
    for node_type in ("dn", "sn"):
        target_count = int(getTargetNodeCountBlocking(app, node_type))
        for i in range(target_count):
            node = {"node_number": i,
                "node_type": node_type,
                "host": None,
                "port": None,
                "id": None }
            nodes.append(node)
            log.warn(f"init added a node, up to {len(nodes)}")

    # check to see if we are running in a DCOS cluster
    is_dcos = os.environ.get('MARATHON_APP_ID')
    if is_dcos:
        log.warn("setting is_dcos to True")
        app["is_dcos"] = True
    else:
        log.warn("net setting is_dcos")

    app["nodes"] = nodes
    app["node_stats"] = {}  # stats retuned by node/info request.  Keyed by node id
    app["node_ids"] = {}  # dictionary to look up node by id
    app.router.add_get('/', info)
    app.router.add_get('/nodestate', nodestate)
    app.router.add_get('/nodestate/{nodetype}', nodestate)
    app.router.add_get('/nodestate/{nodetype}/{nodenumber}', nodestate)
    app.router.add_get('/nodeinfo', nodeinfo)
    app.router.add_get('/nodeinfo/{statkey}', nodeinfo)
    app.router.add_get('/info', info)
    app.router.add_post('/register', register)

    return app

#
# Main
#

if __name__ == '__main__':
    log.info("Head node initializing")
    loop = asyncio.get_event_loop()
    app = init()

    # create a client Session here so that all client requests
    #   will share the same connection pool
    max_tcp_connections = int(config.get("max_tcp_connections"))
    app["loop"] = loop
    app["last_health_check"] = 0

    asyncio.ensure_future(healthCheck(app), loop=loop)
    head_port = config.get("head_port")
    log.info("Starting service on port: {}".format(head_port))
    log.debug("debug test")
    run_app(app, port=int(head_port))
