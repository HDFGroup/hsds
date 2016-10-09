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
import random
import base64
import json
import asyncio
from aiohttp import ClientSession, TCPConnector, HttpProcessingError 
import config
import hsds_logger as log

globals = {}

"""
get default request headers for domain
"""
def getRequestHeaders(domain=None, username=None, password=None):
    if username is None:
        username = config.get("user_name")
    if password is None:
        password = config.get("user_password")
    headers = { }
    if domain is not None:
        headers['host'] = domain
    if username and password:
        auth_string = username + ':' + password
        auth_string = auth_string.encode('utf-8')
        auth_string = base64.b64encode(auth_string)
        auth_string = auth_string.decode('utf-8')
        auth_string = "Basic " + auth_string
        headers['Authorization'] = auth_string
    return headers

async def getEndpoints():
    docker_machine_ip = config.get("docker_machine_ip")
    req = "http://{}:{}/nodestate/sn".format(config.get("head_host"), config.get("head_port")) 
    client = globals["client"]
    globals["request_count"] += 1
    async with client.get(req) as rsp:
        if rsp.status == 200:
            rsp_json = await rsp.json()
    nodes = rsp_json["nodes"]
    sn_endpoints = []
    for node in nodes:
        if not node["host"]:
            continue
        host = node["host"]
        if docker_machine_ip:
            # when running in docker, use the machine addres as host
            host = docker_machine_ip
        url = "http://{}:{}".format(host, node["port"])
        sn_endpoints.append(url)
    log.info("{} endpoints".format(len(sn_endpoints)))
    globals["sn_endpoints"] = sn_endpoints

def getEndpoint():
    """ choose random endpoint from our list
    """
    end_point = random.choice(globals["sn_endpoints"])
    return end_point

def getFileList():
    """ read text file that gives list of inputfiles and
    add filenames to input_files global
    """
    file_list = "filelist.txt"
     
    if len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help"):
        print("usage: import_ghcn_files [filelist.txt]")
        sys.exit()
    if len(sys.argv) > 1:
        file_list = sys.argv[1]

    with open(file_list) as f:
        content = f.readlines()
    input_files = []
    for line in content:
        line = line.rstrip()
        if line and not line.startswith('#'):
            input_files.append(line)
    globals["input_files"] = input_files

async def createGroup(parent_group, group_name):
    """ create a new group and link it to the parent group with 
    link name of group name
    """
    client = globals["client"]
    domain = globals["domain"]
    params = {"host": domain}
    base_req = getEndpoint()
    headers = getRequestHeaders()

    # TBD - replace with atomic create & link operation?
    
    # create a new group
    req = base_req + "/groups"
    log.info("POST:" + req)
    globals["request_count"] += 1
    async with client.post(req, headers=headers, params=params) as rsp:
        if rsp.status != 201:
            log.error("Group creation failed: {}, rsp: {}".format(rsp.status, str(rsp)))
            raise HttpProcessingError(code=rsp.status, message="Unexpected error")
        group_json = await rsp.json()
        group_id = group_json["id"]

    # link group to parent
    req = base_req + "/groups/" + parent_group + "/links/" + group_name
    data = {"id": group_id }
    link_created = False
    log.info("PUT " + req)
    globals["request_count"] += 1
    async with client.put(req, data=json.dumps(data), headers=headers, params=params) as rsp:
        if rsp.status == 409:
            # another task has created this link already
            log.warn("got 409 in request: " + req)
        elif rsp.status != 201:
            log.error("got http error: {} for request: {}, rsp: {}".format(rsp.status, req, rsp))
            raise HttpProcessingError(code=rsp.status, message="Unexpected error")
        else:
            link_created = True

    if not link_created:
        # fetch the existing link and return the group 
        log.info("GET " + req)
        globals["request_count"] += 1
        async with client.get(req, headers=headers, params=params) as rsp:
            if rsp.status != 200:
                log.warn("unexpected error (expected to find link) {} for request: {}".format(rsp.status, req))
                raise HttpProcessingError(code=rsp.status, message="Unexpected error")
            else:
                rsp_json = await rsp.json()
                link_json = rsp_json["link"]
                if link_json["class"] != "H5L_TYPE_HARD":
                    raise ValueError("Unexpected Link type: {}".format(link_json))
                group_id = link_json["id"]
    
    return group_id                



async def verifyGroupPath(h5path):
    """ create any groups along the path that doesn't exist
    """
    #print("current task: ", asyncio.Task.current_task())
    client = globals["client"]
    domain = globals["domain"]
    h5path_cache = globals["h5path_cache"]
    params = {"host": domain}
    parent_group = h5path_cache['/']  # start with root
    group_names = h5path.split('/')
    
    headers = getRequestHeaders()
    
    base_req = getEndpoint() + '/groups/'
    next_path = '/'

    for group_name in group_names:
        if not group_name:
            continue  # skip empty names
        next_path += group_name
        if not next_path.endswith('/'):
            next_path += '/'  # prep for next roundtrips
        if next_path in h5path_cache:
            # we already have the group id
            parent_group = h5path_cache[next_path]    
            continue
        
        req = base_req + parent_group + "/links/" + group_name
        log.info("GET " + req)
        globals["request_count"] += 1
        async with client.get(req, headers=headers, params=params) as rsp:
            if rsp.status == 404:
                parent_group = await createGroup(parent_group, group_name)
            elif rsp.status != 200:
                raise HttpProcessingError(code=rsp.status, message="Unexpected error")
            else:
                rsp_json = await rsp.json()
                link_json = rsp_json["link"]
                if link_json["class"] != "H5L_TYPE_HARD":
                    raise ValueError("Unexpected Link type: {}".format(link_json))
                parent_group = link_json["id"]
                h5path_cache[next_path] = parent_group
    
    return parent_group
  

async def verifyDomain(domain):
    """ create domain if it doesn't already exist
    """
    params = {"host": domain}
    headers = getRequestHeaders()
    client = globals["client"]
    req = getEndpoint() + '/'
    root_id = None
    log.info("GET " + req)
    globals["request_count"] += 1
    async with client.get(req, headers=headers, params=params) as rsp:
        if rsp.status == 200:
            domain_json = await rsp.json()
        else:
            log.info("got status: {}".format(rsp.status))
    if rsp.status == 200:
        root_id = domain_json["root"]
    elif rsp.status == 404:
        # create the domain
        log.info("PUT " + req)
        globals["request_count"] += 1
        async with client.put(req, headers=headers, params=params) as rsp:
            if rsp.status != 201:
                raise HttpProcessingError(code=rsp.status, message="Unexpected error")
        log.info("GET " + req)
        globals["request_count"] += 1
        async with client.get(req, headers=headers, params=params) as rsp:
            if rsp.status == 200:
                domain_json = await rsp.json()
                root_id = domain_json["root"]
            else:
                raise HttpProcessingError(code=rsp.status, message="Service error")
    globals["root"] = root_id

async def import_line_task(line):
    try:
        await import_line(line)
    except HttpProcessingError as hpe:
        log.error("failed to write line: {}".format(line))
        globals["failed_line_updates"] += 1

async def import_line(line):
    domain = globals["domain"]
    params = {"host": domain}
    headers = getRequestHeaders()
    client = globals["client"]
    globals["lines_read"] += 1
    
    fields = line.split(',')
    if len(fields) != 8:
        log.warn("unexpected number of fields in line: [()]".foramt(line))
        return
    station = fields[0]
    if len(station) != 11:
        log.warn("unexpected station length line: [()]".foramt(line))
        return
    date = fields[1]
    if len(date) != 8:
        log.warn("unexpected station length line: [()]".foramt(line))
        return
    obstype = fields[2]
    if len(obstype) != 4:
        log.warn("unexpected obstype length line: [()]".foramt(line))
        return
    value = 0
    try:
        value = int(fields[3])
    except ValueError:
        log.warn("unexpected value in line: [()]".foramt(line))
        return
    # TBD - do something with other fields
    log.info("data: {} {} {} {}".format(station, obstype, date, value))
    h5path = "/data/" + station + "/" + obstype
    grp_id = await verifyGroupPath(h5path)

    # create the attribute
    data = {'type': 'H5T_STD_I32LE', 'value': value}
    req = getEndpoint() + "/groups/" + grp_id + "/attributes/" + date
    log.info("PUT " + req)
    globals["request_count"] += 1
    async with client.put(req, headers=headers, data=json.dumps(data), params=params) as rsp:
        if rsp.status == 409:
            log.warn("409 for req: " + req)
        elif rsp.status != 201:
            raise HttpProcessingError(code=rsp.status, message="Unexpected error")
        else:
            globals["attribute_count"] += 1

    
def import_file(filename):
    log.info("import_file: {}".format(filename))
    loop = globals["loop"]
    max_concurrent_tasks = config.get("max_concurrent_tasks")
    tasks = []
    with open(filename, 'r') as fh:
        for line in fh:
            line = line.rstrip()
            #loop.run_until_complete(import_line(line))
            tasks.append(asyncio.ensure_future(import_line_task(line)))
            if len(tasks) < max_concurrent_tasks:
                continue  # get next line
            # got a batch, move them out!
            loop.run_until_complete(asyncio.gather(*tasks))
            tasks = []
    # finish any remaining tasks
    loop.run_until_complete(asyncio.gather(*tasks))
    globals["files_read"] += 1



def main():    
    domain = config.get("domain_name") + '.' + config.get("user_name") + ".home"
    print("domain: {}".format(domain) )
    
    getFileList() # populates file_list global
     
    log.info("initializing")
    loop = asyncio.get_event_loop()
    globals["loop"] = loop
    #domain = helper.getTestDomainName()
    
    # create a client Session here so that all client requests 
    #   will share the same connection pool
    max_tcp_connections = int(config.get("max_tcp_connections"))
    client = ClientSession(loop=loop, connector=TCPConnector(limit=max_tcp_connections))
    globals["client"] = client
    globals["files_read"] = 0
    globals["lines_read"] = 0
    globals["attribute_count"] = 0
    globals["request_count"] = 0
    globals["failed_line_updates"] = 0

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

    # keep a lookup table of h5paths to obj ids to reduce server roundtrips
    h5path_cache = {'/': globals["root"]}
    globals["h5path_cache"] = h5path_cache
    log.info("domain root: {}".format(globals["root"]))

    loop.run_until_complete(verifyGroupPath("/data"))

    input_files = globals["input_files"]
    for filename in input_files:
        import_file(filename)

    log.info("h5path_cache...")
    keys = list(h5path_cache.keys())
    keys.sort()
    for key in keys:
        log.info("{} -> {}".format(key, h5path_cache[key]))

    print("files read: {}".format(globals["files_read"]))
    print("lines read: {}".format(globals["lines_read"]))
    print("lines unable to process: {}".format(globals["failed_line_updates"]))
    print("num groups: {}".format(len(keys)))
    print("attr created: {}".format(globals["attribute_count"]))
    print("requests made: {}".format(globals["request_count"]))
    

    loop.close()
    client.close()
    
     

main()