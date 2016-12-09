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
import sys
import signal
import random
import time
import json
import asyncio
from aiohttp import ClientSession, TCPConnector, HttpProcessingError 
import config
import hsds_logger as log
from helper import getRequestHeaders, getTestDomainName, setupDomain

globals = {}

def checkDockerLink():
    # scan through /etc/hosts and see if any hsds links have been defined
    linkfound = False
    with open('/etc/hosts') as f:
        lines = f.readlines()
    for line in lines:
        fields = line.split('\t')
        if len(fields) < 2:
            continue
        host_name = fields[1]
        if host_name.startswith("hsds_sn"):
            linkfound = True
            break
    return linkfound
    

async def getEndpoints():
    docker_machine_ip = config.get("docker_machine_ip")
    req = "{}/nodestate/sn".format(config.get("head_endpoint")) 
    client = globals["client"]
    async with client.get(req) as rsp:
        if rsp.status == 200:
            rsp_json = await rsp.json()
    nodes = rsp_json["nodes"]
    sn_endpoints = []
    docker_links = checkDockerLink()
    for node in nodes:
        if not node["host"]:
            continue
        if docker_links:
            # when running in docker, use the machine address as host
            host = "hsds_sn_{}".format(node["node_number"])
        elif docker_machine_ip:
            host = docker_machine_ip
        else:
            host = node["host"]
        url = "http://{}:{}".format(host, node["port"])
        sn_endpoints.append(url)
    log.info("{} endpoints".format(len(sn_endpoints)))
    globals["sn_endpoints"] = sn_endpoints

def getEndpoint():
    """ choose random endpoint from our list
    """
    end_point = random.choice(globals["sn_endpoints"])
    return end_point

async def createGroup():
    """ create a new group and link it to the parent group with 
    link name of group name
    """
    client = globals["client"]
    domain = globals["domain"]
    params = {"host": domain}
    base_req = getEndpoint()
    headers = getRequestHeaders()
  
    # create a new group
    req = base_req + "/groups"
    log.info("POST:" + req)
    globals["grp_request_count"] += 1
    group_name = globals["grp_request_count"]
    timeout = config.get("timeout")
    async with client.post(req, headers=headers, timeout=timeout, params=params) as rsp:
        if rsp.status != 201:
            log.error("POST {} failed with status: {}, rsp: {}".format(req, rsp.status, str(rsp)))
            globals["grp_failed_posts"] += 1
            raise HttpProcessingError(code=rsp.status, message="Unexpected error")
        else:
            globals["group_count"] += 1
            log.info("group_count: {}".format(globals["group_count"]))
        group_json = await rsp.json()
        group_id = group_json["id"]

    # link group to parent
    root_id = globals["root"] 
    group_name = "group_{}".format(group_name)  
    req = base_req + "/groups/" + root_id + "/links/" + group_name
    data = {"id": group_id }
    log.info("PUT " + req)
    globals["lnk_request_count"] += 1
    async with client.put(req, data=json.dumps(data), headers=headers, timeout=timeout, params=params) as rsp:
        if rsp.status == 409:
            # another task has created this link already
            log.warn("got 409 in request: " + req)
        elif rsp.status != 201:
            globals["lnk_failed_posts"] += 1
            log.error("got http error: {} for request: {}, rsp: {}".format(rsp.status, req, rsp))
            raise HttpProcessingError(code=rsp.status, message="Unexpected error")
        else:
            link_created = True
    
    return group_id                


async def verifyDomain(domain):
    """ create domain if it doesn't already exist
    """
    params = {"host": domain}
    headers = getRequestHeaders()
    client = globals["client"]
    req = getEndpoint() + '/'
    root_id = None
    log.info("GET " + req)
    timeout = config.get("timeout")
    async with client.get(req, headers=headers, timeout=timeout, params=params) as rsp:
        if rsp.status == 200:
            domain_json = await rsp.json()
        else:
            log.info("got status: {}".format(rsp.status))
    if rsp.status == 200:
        root_id = domain_json["root"]
    elif rsp.status == 404:
        # create the domain
        setupDomain(domain)
        async with client.get(req, headers=headers, timeout=timeout, params=params) as rsp:
            if rsp.status == 200:
                domain_json = await rsp.json()
                root_id = domain_json["root"]
            else:
                log.error("got status: {} for GET req: {}".format(rsp.status, req))
                raise HttpProcessingError(code=rsp.status, message="Service error")
    globals["root"] = root_id

 
def print_results():
    print("grp_request_count: {}".format(globals["grp_request_count"]))
    print("grp_failed_posts: {}".format(globals["grp_failed_posts"]))
    print("lnk_request_count: {}".format(globals["lnk_request_count"]))
    print("lnk_failed_posts: {}".format(globals["lnk_failed_posts"]))
    print("group_target: {}".format(globals["group_target"]))
    print("group_count: {}".format(globals["group_count"]))
    elapsed = globals["stop_time"] - globals["start_time"]
    print("elapsed time: {}".format(elapsed))


def sig_handler(sig, frame):
    log.warn("Caught signal: {}".format(str(sig)))
    print_results()
    sys.exit()


def main(): 
    domain = getTestDomainName("mkgroups_perf")
    print("domain: {}".format(domain) )
       
    log.info("initializing")
    signal.signal(signal.SIGTERM, sig_handler)  # add handlers for early exit
    signal.signal(signal.SIGINT, sig_handler)

    loop = asyncio.get_event_loop()
    globals["loop"] = loop
    #domain = helper.getTestDomainName()
    
    # create a client Session here so that all client requests 
    #   will share the same connection pool
    max_tcp_connections = int(config.get("max_tcp_connections"))
    client = ClientSession(loop=loop, connector=TCPConnector(limit=max_tcp_connections))
    globals["client"] = client
    globals["group_count"] = 0
    globals["grp_request_count"] = 0
    globals["lnk_request_count"] = 0
    globals["grp_failed_posts"] = 0
    globals["lnk_failed_posts"] = 0
    globals["group_target"] = config.get("group_target")
    max_concurrent_tasks = config.get("max_concurrent_tasks")

    loop.run_until_complete(getEndpoints())

    if len(globals["sn_endpoints"]) == 0:
        log.error("no SN endpoints found!")
        loop.close()
        client.close()
        sys.exit()
    for endpoint in globals["sn_endpoints"]:
        log.info("got endpoint: {}".format(endpoint))

    loop.run_until_complete(verifyDomain(domain))
    globals["domain"] = domain # save the domain 
    globals["start_time"] = time.time()

    # start making groups!
    while globals["grp_request_count"] < globals["group_target"]:
        tasks = []
        count = max_concurrent_tasks
        if globals["group_target"] - globals["grp_request_count"] < count:
            count = globals["group_target"] - globals["grp_request_count"]
        log.info("adding {} tasks".format(count))
        for i in range(count):
            tasks.append(asyncio.ensure_future(createGroup()))   
        # got a batch, move them out!
        loop.run_until_complete(asyncio.gather(*tasks))
        tasks = []
    
    loop.close()
    client.close()
    globals["stop_time"] = time.time()

    print_results()     

main()