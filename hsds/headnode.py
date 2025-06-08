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
import os
import time

from aiohttp.web import Application, StreamResponse, run_app, json_response
from aiohttp.web_exceptions import HTTPBadRequest, HTTPInternalServerError

from . import config
from .util.timeUtil import unixTimeToUTC, elapsedTime
from .util.nodeUtil import createNodeId
from . import hsds_logger as log
from .util import query_marathon as marathonClient

NODE_STAT_KEYS = (
    "cpu",
    "diskio",
    "memory",
    "log_stats",
    "disk",
    "netio",
    "req_count",
    "s3_stats",
    "azure_stats",
    "chunk_cache_stats",
)


class Node:
    def __init__(self, node_id=None, node_type=None, node_host=None, node_port=None):
        self._id = node_id
        self._type = node_type
        self._host = node_host
        self._port = node_port
        now = time.time()
        self._create_time = now
        self._last_poll = now
        self._stats = {}

    @property
    def id(self):
        return self._id

    @property
    def type(self):
        return self._type

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def stats(self):
        return self._stats

    @property
    def create_time(self):
        return self._create_time

    def set_stats(self, stats):
        self._stats = stats

    def get_info(self):
        info = {}
        info["id"] = self._id
        info["type"] = self._type
        info["host"] = self._host
        info["port"] = self._port
        return info

    def poll_update(self):
        now = time.time()
        self._last_poll = now

    def is_healthy(self):
        sleep_sec = int(config.get("node_sleep_time"))

        now = time.time()
        if now - self._last_poll < sleep_sec * 2:
            return True
        else:
            return False


async def isClusterReady(app):
    sn_count = 0
    dn_count = 0
    target_sn_count = await getTargetNodeCount(app, "sn")
    target_dn_count = await getTargetNodeCount(app, "dn")
    last_create_time = None
    nodes = app["nodes"]
    for node_id in nodes:
        node = nodes[node_id]
        if not node.is_healthy():
            log.debug(f"node {node.id} is unhealthy")
            continue
        if last_create_time is None or node.create_time > last_create_time:
            last_create_time = node.create_time
        if node.type == "sn":
            sn_count += 1
        else:
            dn_count += 1
    if sn_count == 0 or dn_count == 0:
        log.debug("no nodes, cluster not ready")
        return False
    if sn_count < target_sn_count or dn_count < target_dn_count:
        log.debug("not all nodes active, cluster not ready")
        return False

    log.debug("cluster is ready")
    return True


def removeNode(app, host=None, port=None):
    dead_node_ids = app["dead_node_ids"]
    nodes = app["nodes"]
    remove_id = None
    for node_id in nodes:
        node = nodes[node_id]
        if node.port == port and node.host == host:
            remove_id = node_id
            break  # only expecting one at most
    if remove_id:
        del nodes[remove_id]
        dead_node_ids.add(remove_id)


async def info(request):
    """HTTP Method to return node state to caller"""
    log.request(request)
    app = request.app
    resp = StreamResponse()
    resp.headers["Content-Type"] = "application/json"
    if await isClusterReady(app):
        cluster_state = "READY"
    else:
        cluster_state = "WAITING"
    answer = {}
    # copy relevant entries from state dictionary to response
    answer["id"] = request.app["id"]
    answer["start_time"] = unixTimeToUTC(app["start_time"])
    answer["last_health_check"] = unixTimeToUTC(app["last_health_check"])
    answer["up_time"] = elapsedTime(app["start_time"])
    answer["cluster_state"] = cluster_state
    answer["bucket_name"] = app["bucket_name"]
    answer["target_sn_count"] = await getTargetNodeCount(app, "sn")
    answer["active_sn_count"] = getActiveNodeCount(app, "sn")
    answer["target_dn_count"] = await getTargetNodeCount(app, "dn")
    answer["active_dn_count"] = getActiveNodeCount(app, "dn")

    resp = json_response(answer)
    log.response(request, resp=resp)
    return resp


async def register(request):
    """HTTP method for nodes to register with head node"""
    app = request.app
    if not request.has_body:
        msg = "register missing body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    body = await request.json()
    log.info(f"register request body: {body}")
    node_host = None
    node_port = None
    node_type = None
    node_id = None
    if "id" not in body:
        msg = "Missing 'id'"
        log.response(request, code=400, message=msg)
        raise HTTPBadRequest(reason=msg)
    node_id = body["id"]
    if "node_type" not in body:
        msg = "missing key 'node_type'"
        log.response(request, code=400, message=msg)
        raise HTTPBadRequest(reason=msg)
    node_type = body["node_type"]
    if node_type not in ("sn", "dn"):
        msg = f"invalid node_type: {node_type}"
        log.response(request, code=400, message=msg)
        raise HTTPBadRequest(reason=msg)
    if "port" not in body:
        msg = "missing key 'port'"
        log.response(request, code=400, message=msg)
        raise HTTPBadRequest(reason=msg)
    node_port = body["port"]

    if "ip" not in body:
        log.debug("register - get ip/port from request.transport")
        peername = request.transport.get_extra_info("peername")
        if peername is None:
            msg = "Can not determine caller IP"
            log.error(msg)
            raise HTTPBadRequest(reason=msg)
        if peername[0] is None or peername[0] in ("::1", "127.0.0.1"):
            node_host = "localhost"
        else:
            node_host = peername[0]
    else:
        # Specify the ip is useful in docker / DCOS situations, where in
        # certain situations a docker private network IP might be used
        node_host = body["ip"]

    log.info(f"register host: {node_host}, port: {node_port}")

    nodes = app["nodes"]
    dead_node_ids = app["dead_node_ids"]

    if node_id in nodes:
        # already registered?
        node = nodes[node_id]
        if node_type != node.type:
            msg = f"Unexpected node_type {node_type} (expected: {node.type}) "
            msg += f"for node_id: {node_id}"
            log.error(msg)
            raise HTTPBadRequest(reason=msg)
        if node_port != node.port:
            msg = f"Unexpected node_port {node_port} (expected: {node.port}) "
            msg += f"for node_id: {node_id}"
            log.error(msg)
            raise HTTPBadRequest(reason=msg)
        if node_host != node.host:
            msg = f"Unexpected node_host {node_host}(expected: {node.host}) "
            msg += f"for node_id: {node_id}"
            log.error(msg)
            raise HTTPBadRequest(reason=msg)
        node.poll_update()  # note that the node has checked in
    elif node_id in dead_node_ids:
        log.error(f"unexpected register request from node id: {node_id}")
        raise HTTPInternalServerError()
    else:
        log.info(f"Node {node_id} is unknown, new node coming online.")
        node = Node(
            node_id=node_id,
            node_type=node_type,
            node_host=node_host,
            node_port=node_port,
        )
        # delete any existing node with the same port
        removeNode(app, host=node_host, port=node_port)
        nodes[node_id] = node

    resp = StreamResponse()
    resp.headers["Content-Type"] = "application/json"
    answer = {}

    if await isClusterReady(app):
        answer["cluster_state"] = "READY"
    else:
        answer["cluster_state"] = "WAITING"
    sn_urls = []
    dn_urls = []
    sn_ids = []
    dn_ids = []
    for node_id in nodes:
        node = nodes[node_id]
        if not node.is_healthy():
            continue
        node_url = f"http://{node.host}:{node.port}"
        if node.type == "sn":
            sn_urls.append(node_url)
            sn_ids.append(node_id)
        else:
            dn_urls.append(node_url)
            dn_ids.append(node_id)

    # sort dn_urls so node number can be determined
    dn_id_map = {}
    for i in range(len(dn_urls)):
        dn_url = dn_urls[i]
        dn_id = dn_ids[i]
        dn_id_map[dn_url] = dn_id

    dn_urls.sort()
    dn_ids = []  # re-arrange to match url order
    for dn_url in dn_urls:
        dn_ids.append(dn_id_map[dn_url])

    answer["sn_urls"] = sn_urls
    answer["dn_urls"] = dn_urls
    answer["sn_ids"] = sn_ids
    answer["dn_ids"] = dn_ids
    answer["req_ip"] = node_host
    log.debug(f"register returning: {answer}")
    app["last_health_check"] = int(time.time())

    resp = json_response(answer)
    log.response(request, resp=resp)
    return resp


async def nodestate(request):
    """HTTP method to return information about registered nodes"""
    log.request(request)
    node_id = request.match_info.get("node_id", "*")
    node_type = request.match_info.get("node_type", "*")

    log.info(f"nodestate/{node_type}/{node_id}")

    app = request.app
    resp = StreamResponse()
    resp.headers["Content-Type"] = "application/json"
    nodes = app["nodes"]

    if node_id == "*":
        info_list = []
        for node_id in nodes:
            node = nodes[node_id]
            if node.type == node_type or node_type == "*":
                info_list.append(node.get_info())
        answer = {"nodes": info_list}
        log.debug(f"returning nodestate for {len(nodes)} nodes")
    elif node_id in nodes:
        node = nodes[node_id]
        answer = {}
        answer["node"] = node.get_info()
    if await isClusterReady(app):
        cluster_state = "READY"
    else:
        cluster_state = "WAITING"
    answer["cluster_state"] = cluster_state
    resp = json_response(answer)
    log.response(request, resp=resp)
    return resp


async def nodeinfo(request):
    """HTTP method to return node stats (cpu usage, request count, errors,
    etc.) about registed nodes
    """
    log.request(request)
    node_stat_keys = NODE_STAT_KEYS
    stat_key = request.match_info.get("statkey", "*")
    if stat_key != "*":
        if stat_key not in node_stat_keys:
            msg = f"nodeinfo - invalid key: {stat_key}"
            log.warn(msg)
            raise HTTPBadRequest(msg)
        node_stat_keys = (stat_key,)

    app = request.app
    resp = StreamResponse()
    resp.headers["Content-Type"] = "application/json"

    dn_count = 0
    sn_count = 0

    answer = {}
    nodes = app["nodes"]

    # re-assemble the individual node stats to arrays indexed by node number
    for stat_key in node_stat_keys:
        log.info(f"stat_key: {stat_key}")
        stats = {}
        for node_id in nodes:
            node = nodes[node_id]
            if not node.is_healthy:
                continue  # skip unhealthy node
            if node.type not in ("sn", "dn"):
                log.error(f"unexpected node_type: {node.type}")
                continue
            if stat_key not in node.stats:
                msg = f"key: {stat_key} not found in node_stats for "
                msg += f"node_id: {node_id}"
                log.info(msg)
                continue
            if node.type == "sn":
                node_number = sn_count
                sn_count += 1
            else:
                node_number = dn_count
                dn_count += 1
            stats_field = node.stats[stat_key]
            for k in stats_field:
                if k not in stats:
                    stats[k] = {}
                    stats[k]["sn"] = [
                        0,
                    ] * sn_count
                    stats[k]["dn"] = [
                        0,
                    ] * dn_count
                stats[k][node.type][node_number] = stats_field[k]
        answer[stat_key] = stats

    resp = json_response(answer)
    log.response(request, resp=resp)
    return resp


async def getTargetNodeCount(app, node_type):

    if node_type == "dn":
        key = "target_sn_count"
    elif node_type == "sn":
        key = "target_sn_count"
    else:
        raise KeyError()
    if "key" not in app:
        if "is_dcos" in app:
            marathon = marathonClient.MarathonClient(app)
            if node_type == "dn":
                app[key] = int(await marathon.getDNInstances())
            else:
                app[key] = int(await marathon.getDNInstances())
        else:
            app[key] = config.get(key)
    return app[key]


def getActiveNodeCount(app, node_type):
    count = 0
    nodes = app["nodes"]
    for node_id in nodes:
        node = nodes[node_id]
        if node.type != node_type:
            continue
        if node.is_healthy:
            count += 1
    return count


async def init():
    """Intitialize application and return app object"""

    # setup log config
    log_level = config.get("log_level")
    prefix = config.get("log_prefix")
    log_timestamps = config.get("log_timestamps", default=False)
    log.setLogConfig(log_level, prefix=prefix, timestamps=log_timestamps)

    app = Application()

    # set a bunch of global state
    app["id"] = createNodeId("head")

    bucket_name = config.get("bucket_name")
    if bucket_name:
        log.info(f"using bucket: {bucket_name}")
        app["bucket_name"] = bucket_name
    else:
        log.info("No default bucket name is set")

    app["head_port"] = config.get("head_port")

    nodes = {}

    # check to see if we are running in a DCOS cluster
    if "MARATHON_APP_ID" in os.environ:
        msg = "Found MARATHON_APP_ID environment variable, setting "
        msg += "is_dcos to True"
        log.info(msg)
        app["is_dcos"] = True
    else:
        log.info("not setting is_dcos")

    app["nodes"] = nodes
    app["dead_node_ids"] = set()
    app["start_time"] = int(time.time())  # seconds after epoch
    app["last_health_check"] = 0
    app["max_task_count"] = config.get("max_task_count")
    app.router.add_get("/", info)
    app.router.add_get("/nodestate", nodestate)
    app.router.add_get("/nodestate/{nodetype}", nodestate)
    app.router.add_get("/nodestate/{nodetype}/{nodenumber}", nodestate)
    app.router.add_get("/nodeinfo", nodeinfo)
    app.router.add_get("/nodeinfo/{statkey}", nodeinfo)
    app.router.add_get("/info", info)
    app.router.add_post("/register", register)

    return app


def create_app():
    """Create servicenode aiohttp application"""
    log.info("Head node initializing")
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init())
    return app


def main():
    """
    Main - entry point for headnode
    """
    app = create_app()

    # create a client Session here so that all client requests
    #   will share the same connection pool

    head_port = config.get("head_port")
    log.info(f"Starting service on port: {head_port}")
    run_app(app, port=int(head_port))


if __name__ == "__main__":
    main()
