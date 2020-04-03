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
import asyncio
import json
import time

from aiohttp.web import Application, StreamResponse, run_app, json_response
from aiohttp.web_exceptions import HTTPBadRequest

from asyncio import TimeoutError

from . import config
from .util.timeUtil import unixTimeToUTC, elapsedTime
from .util.httpUtil import http_get, getUrl
from .util.idUtil import  createNodeId
from . import hsds_logger as log

NODE_STAT_KEYS = ("cpu", "diskio", "memory", "log_stats", "disk", "netio",
    "req_count", "s3_stats", "azure_stats", "chunk_cache_stats")

async def healthCheck(app):
    """ Periodic method that pings each active node and verifies it is still healthy.
    If node doesn't respond, free up the node slot (the node can re-register if it comes back)'"""

    app["last_health_check"] = int(time.time())

    nodes = app["nodes"]

    while True:
        # sleep for a bit
        sleep_secs = config.get("head_sleep_time")
        await  asyncio.sleep(sleep_secs)

        now = int(time.time())
        log.info("health check {}, cluster_state: {}".format(unixTimeToUTC(now), app["cluster_state"]))

        fail_count = 0
        HEALTH_CHECK_RETRY_COUNT = 1 # times to try before calling a node dead
        for node in nodes:
            if node["host"] is None:
                fail_count += 1
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
            except TimeoutError as toe:
                log.warn("TimeoutError for req: {}: {}".format(url, str(toe)))
                # node has gone away?
                node["failcount"] += 1
                if node["failcount"] >= HEALTH_CHECK_RETRY_COUNT:
                    log.warn("node {}:{} not responding".format(node["host"], node["port"]))
                    fail_count += 1
            except Exception as e:
                log.warn("Exception for healthcheck: {}: {}".format(url, str(e)))
                # node has gone away?
                node["failcount"] += 1
                if node["failcount"] >= HEALTH_CHECK_RETRY_COUNT:
                    log.warn("removing {}:{} from active list".format(node["host"], node["port"]))
                    fail_count += 1

        log.info("node health check fail_count: {}".format(fail_count))
        if fail_count > 0:
            if app["cluster_state"] == "READY":
                # go back to INITIALIZING state until another node is registered
                log.warn("Fail_count > 0, Setting cluster_state from READY to INITIALIZING")
                app["cluster_state"] = "INITIALIZING"
        elif fail_count == 0 and app["cluster_state"] != "READY":
            log.info("All nodes healthy, changing cluster state to READY")
            app["cluster_state"] = "READY"



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
    answer['target_sn_count'] = getTargetNodeCount(app, "sn")
    answer['active_sn_count'] = getActiveNodeCount(app, "sn")
    answer['target_dn_count'] = getTargetNodeCount(app, "dn")
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

    peername = request.transport.get_extra_info('peername')
    if peername is None:
        raise HTTPBadRequest(reason="Can not determine caller IP")
    host, req_port = peername
    log.info("register host: {}, port: {}".format(host, req_port))

    nodes = None
    ret_node = None

    node_ids = app['node_ids']
    if body['id'] in node_ids:
        # already registered?
        ret_node = node_ids[body['id']]
    else:
        nodes = app['nodes']
        for node in nodes:
            if node['host'] is None and node['node_type'] == body['node_type']:
                # found a free node
                log.info("got free node: {}".format(node))
                node['host'] = host
                node['port'] = body["port"]
                node['id'] =   body["id"]
                node["connected"] = unixTimeToUTC(int(time.time()))
                node['failcount'] = 0
                ret_node = node
                node_ids[body["id"]] = ret_node
                break

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

    answer["node_count"] = app["target_dn_count"]

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
    dn_count = app['target_dn_count']
    sn_count = app['target_sn_count']

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

def getTargetNodeCount(app, node_type):
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
        target_count = getTargetNodeCount(app, node_type)
        for i in range(target_count):
            node = {"node_number": i,
                "node_type": node_type,
                "host": None,
                "port": None,
                "id": None }
            nodes.append(node)


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

def main():
    log.info("Head node initializing")
    loop = asyncio.get_event_loop()
    app = init()  

    # create a client Session here so that all client requests
    #   will share the same connection pool
    app["loop"] = loop

    asyncio.ensure_future(healthCheck(app), loop=loop)
    head_port = config.get("head_port")
    log.info("Starting service on port: {}".format(head_port))
    log.debug("debug test")
    run_app(app, port=int(head_port))


if __name__ == '__main__':
    main()
