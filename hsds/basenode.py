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

from aiohttp.web import Application
from aiohttp.web_exceptions import HTTPNotFound, HTTPGone
from aiohttp.web_exceptions import HTTPInternalServerError
from aiohttp.web_exceptions import HTTPServiceUnavailable



from . import config
from .util.httpUtil import http_get, http_post, jsonResponse
from .util.authUtil import getUserPasswordFromRequest, validateUserPassword
from .util.authUtil import isAdminUser
from .util.k8sClient import getDnLabelSelector, getPodIps
from .util.nodeUtil import createNodeId, getNodeNumber, getNodeCount

from . import hsds_logger as log

HSDS_VERSION = "1.0.0"


def getVersion():
    return HSDS_VERSION


def getHeadUrl(app):
    if "head_url" in app:
        head_url = app["head_url"]
    else:
        head_port = config.get("head_port")
        if head_port:
            if "KUBERNETES_SERVICE_HOST" in os.environ:
                dns_name = "127.0.0.1"
            else:
                dns_name = "head"
            head_url = f"http://{dns_name}:{head_port}"
        else:
            head_url = ""
        app["head_url"] = head_url
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
    except Exception as e:
        log.warn(f"uncaught exception in get_info: {e}")

    return rsp_json


async def k8s_get_dn_info(app, dn_urls=None):
    # call info on each dn container and return map of info json key'd by dn_url
    log.debug("k8s_get_dn_info")
    info_map = {}
    if dn_urls is None:
        dn_urls = app["dn_urls"]
    for dn_url in dn_urls:
        req = dn_url + "/info"
        log.debug(f"k8s_get_dn_urls - about to call: {req}")
        # TBD - running these requests in a batch would be a bit faster
        try:
            rsp_json = await http_get(app, req)
            if "node" not in rsp_json:
                log.error("k8s_get_dn_urls - Unexpected response from info (no node key)")
                continue
            node_json = rsp_json["node"]
            if "id" not in node_json:
                log.error("k8s_get_dn_urls - Unexpected response from info (no node/id key)")
                continue
        except HTTPServiceUnavailable:
            log.warn("k8s_get_dn_urls - 503 error from /info request")
            continue
        except Exception as e:
            log.error(f"k8s_get_dn_urls - Exception: {e} from /info request")
            continue
        info_map[dn_url] = node_json
        log.debug(f"adding {dn_url} to dn info map: {node_json}")
    log.debug(f"k8s_get_dn_info, returning {len(info_map)} items")
    return info_map


async def k8s_update_dn_info(app):
    """update dn urls by querying k8s api.
    Call each url to determine node_ids
    """
    log.info("k8s_update_dn_info")
    k8s_dn_label_selector = getDnLabelSelector(config)
    pod_ips = await getPodIps(k8s_dn_label_selector)
    if not pod_ips:
        log.error("Expected to find at least one hsds pod")
        return
    pod_ips.sort()  # for assigning node numbers
    log.debug(f"got pod_ips: {pod_ips}")

    dn_port = config.get("dn_port")
    dn_urls = []
    for pod_ip in pod_ips:
        dn_urls.append(f"http://{pod_ip}:{dn_port}")

    old_count = len(app["dn_urls"])
    new_count = len(dn_urls)

    if old_count != new_count:
        log.info(f"pod count changed from {old_count} to {new_count}, fetch dn_ids")
        scale_update = True
    elif app["dn_urls"] != dn_urls:
        log.info(f"pod ips have changed: {dn_urls}, fetch dn_ids")
        scale_update = True
    else:
        scale_update = False

    log.debug(f"scale_update: {scale_update}")
    if scale_update:
        # save to global
        app["dn_urls"] = dn_urls
        log.info(f"k8s_update_dn_info - dn_urls: {dn_urls}")
        log.debug("scale_update is True, calling k8s_get_dn_info")
        info_map = await k8s_get_dn_info(app, dn_urls=dn_urls)
        dn_ids = []
        dn_node_numbers = []
        min_node_count = len(dn_urls)
        max_node_count = len(dn_urls)
        for dn_url in dn_urls:
            if dn_url in info_map:
                item = info_map[dn_url]
                if "id" in item:
                    dn_ids.append(item["id"])
                if "node_number" in item:
                    dn_node_numbers.append(item["node_number"])
                if "node_count" in item:
                    node_count = item["node_count"]
                else:
                    node_count = 0
                if node_count < min_node_count:
                    min_node_count = node_count
                elif node_count > max_node_count:
                    max_node_count = node_count

        log.debug(f"scale update - min_node_count: {min_node_count}")
        log.debug(f"scale update - max_node_couunt: {max_node_count}")
        log.debug(f"scale_update - dn_node_numbers: {dn_node_numbers}")
        log.debug(f"scale update - dn_ids: {dn_ids}")
        # signal ready state by setting app["dn_ids"] only if:
        #  1) node_count == len(dn_urls) for all dn's
        #  2) dn_ids set for all nodes
        #  3) dn node number are consecutive

        consecutive = True
        dn_node_numbers.sort()
        for i in range(len(dn_node_numbers)):
            if dn_node_numbers[i] != i:
                consecutive = False
                break

        # save ids
        log.info(f"scaling - updating dn_ids to: {dn_ids}")
        app["dn_ids"] = dn_ids

        if len(dn_ids) != new_count:
            log.warn(f"scaling - got {len(dn_ids)} dn_ids expected {new_count}")
        elif len(dn_node_numbers) != len(dn_urls):
            log.warn(f"scaling - got {len(dn_node_numbers)} node numbers, expected {new_count}")
        elif not consecutive:
            log.warn(f"scaling - node_numbers not consecutive - got: {dn_node_numbers}")
        else:
            log.info("scaling - node numbers complete")


async def docker_update_dn_info(app):
    """update list of dn_urls by making request to head node"""
    head_url = getHeadUrl(app)
    if not head_url:
        log.warn("head_url is not set, can not register yet")
        return
    req_reg = head_url + "/register"
    log.debug(f"register: {req_reg}")

    body = {"id": app["id"], "port": app["node_port"], "node_type": app["node_type"]}
    rsp_json = None

    try:
        log.debug(f"register req: {req_reg} body: {body}")
        rsp_json = await http_post(app, req_reg, data=body)
    except HTTPInternalServerError:
        log.error("HEAD node seems to be down.")
        app["dn_urls"] = []
        app["dn_ids"] = []
    except OSError:
        log.error("failed to register")
        app["dn_urls"] = []
        app["dn_ids"] = []

    if rsp_json is not None:
        log.debug(f"register response: {rsp_json}")
        app["dn_urls"] = rsp_json["dn_urls"]
        app["dn_ids"] = rsp_json["dn_ids"]


def get_dn_id_set(app):
    id_set = set()
    dn_ids = app["dn_ids"]
    for dn_id in dn_ids:
        id_set.add(dn_id)
    return id_set


async def update_dn_info(app):
    """update http urls and ids for each dn node"""

    if "is_standalone" in app:
        # nothing to do in standalone mode
        return

    id_set_pre = get_dn_id_set(app)

    if "is_k8s" in app and not getHeadUrl(app):
        await k8s_update_dn_info(app)
    else:
        # docker or kubernetes running with head container
        await docker_update_dn_info(app)

    # do a log if there has been a change in the dn nodes
    id_set_post = get_dn_id_set(app)
    log.debug(f"update_dn_info - id_set_post: {id_set_post}")
    if id_set_pre != id_set_post:
        gone_ids = id_set_pre.difference(id_set_post)
        if gone_ids:
            msg = f"update_dn_info - dn_nodes: {gone_ids} "
            msg += "are no longer active"
            log.info(msg)
        new_ids = id_set_post.difference(id_set_pre)
        if new_ids:
            log.info(f"update_dn_info - dn_nodes: {new_ids} are now active")


def updateReadyState(app, old_dn_urls=None):
    """update node state (and node_number and node_count) based on number
    of dn_urls available
    """
    if "is_standalone" in app:
        # dn_urls don't change in standalone mode, so just return
        log.debug("skip updateReadyState for standalone app")
        return
    dn_urls = app["dn_urls"]
    dn_ids = app["dn_ids"]
    log.debug(f"updateReadyState - for old_dn_urls: {old_dn_urls}")
    log.debug(f"updateReadyState - for new dn_urls: {dn_urls}")
    log.debug(f"updateReadyState - dn_ids: {dn_ids}")

    is_ready = True
    if len(dn_urls) == 0 or len(dn_urls) != len(dn_ids):
        if len(dn_urls) > 0:
            log.warning(f"not all dn_ids found, got: {dn_ids}")
        is_ready = False

    if app["node_type"] == "dn":
        # dn node
        old_number = app["node_number"]
        node_number = getNodeNumber(app)
        if old_number != node_number:
            msg = f"node_number was {old_number}, setting to: {node_number}"
            log.info(msg)
            meta_cache = app["meta_cache"]
            chunk_cache = app["chunk_cache"]
            dirty_cache_count = meta_cache.dirtyCount + chunk_cache.dirtyCount
            if dirty_cache_count > 0:
                # set the node state to waiting till the chunk cache have
                # been flushed
                msg = f"updateReadyState - waiting on {dirty_cache_count} "
                msg += "cache items to be flushed"
                log.info(msg)
                is_ready = False
            else:
                # flush remaining items from cache
                meta_cache.clearCache()
                chunk_cache.clearCache()
                msg = f"scaling - setting node_number to: {node_number} (old value: {old_number}"
                log.info(msg)
                app["node_number"] = node_number
    else:
        # sn node
        if old_dn_urls:
            old_count = len(old_dn_urls)
        else:
            old_count = 0
        new_count = len(dn_urls)
        if old_count != new_count:
            msg = f"scaling - number of dn nodes has changed from {old_count} "
            msg += f"to {new_count}"
            log.info(msg)

    # finally, change state if indicated
    node_state = app["node_state"]
    if node_state == "READY":
        if not is_ready:
            log.info("setting node_state from READY to WAITING")
            app["node_state"] = "WAITING"
    elif node_state == "WAITING":
        if is_ready:
            log.info("setting node_state from WAITING to READY")
            app["node_state"] = "READY"
    elif node_state == "INITIALIZING":
        if is_ready:
            log.info("setting node_state from INITIALIZING to READY")
    elif node_state == "TERMINATING":
        if is_ready:
            log.warn("got is_ready for node in TERMINATING state")
    else:
        log.error(f"unexpected node_state: {node_state}")


def _activeTaskCount():
    count = 0
    for task in asyncio.all_tasks():
        if not task.done():
            count += 1
    return count


async def doHealthCheck(app, chaos_die=0):
    node_state = app["node_state"]
    if node_state == "READY" and chaos_die > 0 and app["node_type"] == "dn":
        if random.randint(0, chaos_die) == 0:
            log.error("chaos die - suicide!")
            sys.exit(1)
        else:
            log.info("chaos die - still alive")
    log.info(f"healthCheck - node_state: {node_state}")
    if node_state != "TERMINATING":
        old_dn_urls = app["dn_urls"]
        await update_dn_info(app)  # may update app["dn_urls"]
        updateReadyState(app, old_dn_urls=old_dn_urls)

    svmem = psutil.virtual_memory()
    num_tasks = len(asyncio.all_tasks())
    msg = f"health check vm: {svmem.percent} num tasks: {num_tasks} "
    msg += f"active tasks: {_activeTaskCount()}"
    log.debug(msg)


async def healthCheck(app):
    """Periodic method that either registers with headnode (if state in
    INITIALIZING) or calls headnode to verify vitals about this node
    (otherwise)
    """

    # let the server event loop startup before starting the health check
    await asyncio.sleep(1)
    log.info("health check start")
    sleep_secs = config.get("node_sleep_time")
    chaos_die = config.get("chaos_die")
    if chaos_die > 0:
        log.debug(f"chaos_die number: {chaos_die}")

    while True:
        try:
            await doHealthCheck(app, chaos_die=chaos_die)
        except Exception as e:
            msg = f"Unexpected {e.__class__.__name__} exception in "
            msg += f"doHealthCheck: {e}"
            log.error(msg)
        await asyncio.sleep(sleep_secs)


async def about(request):
    """HTTP Method to return general info about the service"""
    log.request(request)

    app = request.app
    (username, pswd) = getUserPasswordFromRequest(request)
    if username:
        await validateUserPassword(app, username, pswd)
    answer = {}
    answer["start_time"] = app["start_time"]
    answer["state"] = app["node_state"]
    answer["hsds_version"] = getVersion()
    answer["name"] = config.get("server_name")
    answer["greeting"] = config.get("greeting")
    answer["about"] = config.get("about")
    answer["node_count"] = getNodeCount(app)
    answer["dn_urls"] = app["dn_urls"]
    answer["dn_ids"] = app["dn_ids"]
    if username:
        answer["username"] = username
    else:
        answer["username"] = "anonymous"
    if username and isAdminUser(app, username):
        answer["isadmin"] = True
    else:
        answer["isadmin"] = False

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
    node["id"] = app["id"]
    node["type"] = app["node_type"]
    node["start_time"] = app["start_time"]
    node["state"] = app["node_state"]
    if app["node_type"] == "dn":
        node["node_number"] = app["node_number"]
    node["node_count"] = getNodeCount(app)

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
    sdiskusage = psutil.disk_usage("/")
    disk_stats["total"] = sdiskusage.total
    disk_stats["used"] = sdiskusage.used
    disk_stats["free"] = sdiskusage.free
    disk_stats["percent"] = sdiskusage.percent
    answer["disk"] = disk_stats
    answer["log_stats"] = log.log_count
    answer["req_count"] = log.req_count
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

    # setup log config
    log_level = config.get("log_level")
    prefix = config.get("log_prefix")
    log_timestamps = config.get("log_timestamps", default=False)

    # Make stdout/stderr encoding consistent across all operating systems
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

    log.setLogConfig(log_level, prefix=prefix, timestamps=log_timestamps)

    # create the app object
    log.info("Application baseInit")
    app = Application()

    app["node_state"] = "INITIALIZING"
    app["node_number"] = -1
    app["node_type"] = node_type
    app["start_time"] = int(time.time())  # seconds after epoch
    app["start_time_relative"] = time.perf_counter()  # high precision time
    app["register_time"] = 0
    app["max_task_count"] = config.get("max_task_count")
    app["storage_clients"] = {}  # storage client drivers

    is_standalone = config.getCmdLineArg("standalone")

    if is_standalone:
        log.info("running in standalone mode")
        app["is_standalone"] = True

        # set node_number and node_id
        # for standalone, node_number will be passed on command line
        # create node_id based on the node_number
        node_number = config.getCmdLineArg("node_number")
        if node_number is None:
            log.info("No node_number argument")
            node_number = 0
        else:
            node_number = int(node_number)
        app["node_number"] = node_number
        node_id = createNodeId(node_type, node_number=node_number)
    else:
        # create node id based on uuid
        node_id = createNodeId(node_type)
        node_port = config.get(node_type + "_port")
        log.info(f"using node port: {node_port}")
        app["node_port"] = node_port

    is_readonly = config.getCmdLineArg("readonly")
    if is_readonly:
        log.info("running in readonly mode")
        app["is_readonly"] = True

    log.info(f"setting node_id to: {node_id}")
    app["id"] = node_id

    bucket_name = config.get("bucket_name")
    if bucket_name:
        log.info(f"using bucket: {bucket_name}")
    else:
        log.info("no default bucket defined")
    app["bucket_name"] = bucket_name
    app["allow_any_bucket_read"] = config.get("allow_any_bucket_read", default=True)
    app["allow_any_bucket_write"] = config.get("allow_any_bucket_write", default=True)
    app["dn_urls"] = []
    app["dn_ids"] = []  # node ids for each dn_url

    if is_standalone:
        dn_urls_arg = config.getCmdLineArg("dn_urls")
        if dn_urls_arg:
            dn_urls = dn_urls_arg.split(",")
            dn_urls.sort()
            dn_ids = []
            for i in range(len(dn_urls)):
                dn_url = dn_urls[i]
                if not dn_url.startswith("http"):
                    log.warn(f"Unexpected dn_url value: {dn_url}, type: {type(dn_url)}")
                dn_id = createNodeId("dn", node_number=i)
                dn_ids.append(dn_id)
        else:
            if node_type == "sn":
                msg = "Expected dn_urls option for standalone mode"
                log.error(msg)
                raise ValueError(msg)
            dn_urls = []
            dn_ids = []

        app["dn_urls"] = dn_urls
        app["dn_ids"] = dn_ids

        # check to see if we are running in a DCOS cluster
    elif "IS_DOCKER" in os.environ:
        log.info("running in docker")
        app["is_docker"] = True
    elif "MARATHON_APP_ID" in os.environ:
        msg = "Found MARATHON_APP_ID environment variable, "
        msg += "setting is_dcos to True"
        log.info(msg)
        app["is_dcos"] = True
    elif "KUBERNETES_SERVICE_HOST" in os.environ:
        # indicates we are running in a k8s cluster
        log.info("running in kubernetes")
        app["is_k8s"] = True
    else:
        # check the root inode - high values indicate
        # we are running in a container
        if os.path.isdir("/"):
            stat = os.stat("/")
            if stat and stat.st_ino > 10:
                log.info("running in docker based on inode number")
                app["is_docker"] = True

    if "is_dcos" in app:
        if "PORT0" not in os.environ:
            msg = "Expected PORT0 environment variable for DCOS"
            log.error(msg)
            node_port = config.get(node_type + "_port")
        else:
            node_port = os.environ["PORT0"]
    else:
        node_port = config.get(node_type + "_port")
        log.info(f"using node port: {node_port}")
        app["node_port"] = node_port

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

    app.router.add_get("/info", info)
    app.router.add_get("/about", about)

    if is_standalone:
        # can go straight to ready state
        msg = "setting node_state to inital state of READY for standalone"
        log.info(msg)
        app["node_state"] = "READY"
    else:
        app["node_state"] = "WAITING"

    return app
