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
import time
import json

from aiohttp.web import StreamResponse, run_app

from aiohttp.errors import HttpProcessingError

import config
from basenode import baseInit, healthCheck
from util.timeUtil import unixTimeToUTC
from util.httpUtil import  jsonResponse
from util.s3Util import  getS3Keys, getS3JSONObj, getS3Bytes, putS3Bytes, isS3Obj, getS3ObjStats
from util.idUtil import getCollectionForId, getS3Key
from util.chunkUtil import getDatasetId #, getChunkIndex
import hsds_logger as log

async def listKeys(app):
    """ Get all s3 keys in the bucket and create list of objkeys and domain keys """
    log.info("listKeys start")
    s3keys = await getS3Keys(app)
    log.info("got: {} keys".format(len(s3keys)))
    domains = {}
    groups = {}
    datasets = {}
    datatypes = {}
    top_level_domains = {}
    group_cnt = 0
    dset_cnt = 0
    datatype_cnt = 0
    chunk_cnt = 0
    domain_cnt = 0
    other_cnt = 0
    # 24693-g-ccd7e104-f86c-11e6-8f7b-0242ac110009
    for s3key in s3keys:
        if len(s3key) >= 44 and s3key[0:5].isalnum() and s3key[5] == '-' and s3key[6] in ('g', 'd', 'c', 't'):
            objid = s3key[6:]
            if objid[0] == 'g':
                groups[objid] = {}
                group_cnt += 1
            elif objid[0] == 'd':
                datasets[objid] = { "chunks": {} }
                dset_cnt += 1
            elif objid[0] == 't':
                datatypes[objid] = {}
                datatype_cnt += 1
            elif objid[0] == 'c':
                chunk_cnt += 1
        elif s3key == "headnode":
            pass
        elif s3key.endswith(".txt"):
            # ignore collection files
            pass
        elif s3key.endswith("/.domain.json"):
            n = s3key.index('/')
            if n == 0:
                log.warn("unexpected domain name (leading slash): {}".format(s3key))
            elif n == -1:
                log.warn("unexpected domain name (no slash): {}".format(s3key))
            else:
                tld = s3key[:n]
                if tld not in top_level_domains:
                    top_level_domains[tld] = {}
                domain_cnt += 1
                # TBD - add a domainUtil func for this
                domain = '/' + s3key[:-(len("/.domain.json"))]
                domains[domain] = {"groups": {}, "datasets": {}, "datatypes": {}}
            
        else:
            log.warn("unknown object: {}".format(s3key))
    log.info("domain_cnt: {}".format(domain_cnt))
    log.info("group_cnt: {}".format(group_cnt))
    log.info("dset_cnt: {}".format(dset_cnt))
    log.info("datatype_cnt: {}".format(datatype_cnt))
    log.info("chunk_cnt: {}".format(chunk_cnt))
    log.info("other_cnt: {}".format(other_cnt))
    log.info("top_level_domains:")
    for tld in top_level_domains:
        log.info(tld)    
    
    app["domains"] = domains
    app["groups"] = groups
    app["datasets"] = datasets
    app["datatypes"] = datatypes

    chunk_del = []  # list of chunk ids that no longer have a dataset

    # iterate through s3keys again and add any chunks to the corresponding dataset
    for s3key in s3keys:
        if len(s3key) >= 44 and s3key[0:5].isalnum() and s3key[5] == '-' and s3key[6] == 'c':
            chunk_id = s3key[6:]
            dset_id = getDatasetId(chunk_id)
            if dset_id not in datasets:
                chunk_del.append(chunk_id)
            else:
                dset = datasets[dset_id]
                dset_chunks = dset["chunks"]
                dset_chunks[chunk_id] = {}

    log.info("chunk delete list ({} items):".format(len(chunk_del)))
    for chunk_id in chunk_del:
        #log.info(chunk_id)
        pass

    log.info("listKeys done")

async def markObj(app, obj_id):
    """ Mark obj as in-use and for group objs, recursively call for hardlink objects 
    """
    domains = app["domains"]
    collection = getCollectionForId(obj_id)
    obj_ids = app[collection]
    if obj_id not in obj_ids:
        log.warn("markObj: key {} not found s3_key: {}".format(obj_id, getS3Key(obj_id)))
        return
    obj = obj_ids[obj_id]
    obj["u"] = True  # in use
    #if collection == "groups":
    # add the objid to our domain list by collection type
    s3key = getS3Key(obj_id)
    try:
        data = await getS3Bytes(app, s3key)
    except HttpProcessingError as hpe:
        log.error("Error {} reading S3 key: {} ".format(hpe.code, s3key))
        return
    num_bytes = len(data)
    obj_json = json.loads(data.decode('utf8'))
    if "domain" not in obj_json:
        log.warn("Expected to find domain key for obj: {}".format(obj_id))
        return
    domain = obj_json["domain"]
    if domain in domains:
        domain_obj = domains[domain] 
        domain_col = domain_obj[collection]
        domain_col[obj_id] = { "size": num_bytes }
        log.info("added {} to domain collection: {}".format(obj_id, collection))
    else:
        log.warn("domain {} for group: {} not found".format(domain, obj_id))
    if collection == "groups":
        # For group objects, iteratore through all the hard lines and mark those objects
        links = obj_json["links"]
        for link_name in links:
            link_json = links[link_name]
            if link_json["class"] == "H5L_TYPE_HARD":
                link_id = link_json["id"]
                await markObj(app, link_id)


async def markAndSweep(app):
    """ Implement classic mark and sweep algorithm. """
    groups = app["groups"]
    datasets = app["datasets"]
    datatypes = app["datatypes"]
    domains = app["domains"]
    log.info("markAndSweep start")
    # mark objects as not inuse
    for objid in groups:
        obj = groups[objid]
        obj["u"] = False
    for objid in datasets:
        obj = datasets[objid]
        obj["u"] = False
    for objid in datatypes:
        obj = datatypes[objid]
        obj["u"] = False
    
    # now iterate through domains
    log.info("mark domain objects start")
    for domain_key in domains:
        s3key = getS3Key(domain_key)
        try:
            obj_json = await getS3JSONObj(app, s3key)
        except HttpProcessingError:
            log.warn("domain object: {} not found".format(s3key))
            continue
        if "root" not in obj_json:
            log.info("no root for {}".format(domain_key))
            continue
        root_id = obj_json["root"]
        log.info("{} root: {}".format(domain_key, root_id))
        await markObj(app, root_id) 
    log.info("mark domain objects done")

    # delete any objects that are not in use
    log.info("delete unmarked objects start")
    delete_count = 0
    for objid in groups:
        obj = groups[objid]
        if obj["u"] is False:
            delete_count += 1
            log.info("delete {}".format(objid))
    for objid in datatypes:
        obj = datatypes[objid]
        if obj["u"] is False:
            delete_count += 1
            log.info("delete {}".format(objid))
    for objid in datasets:
        obj = datasets[objid]
        if obj["u"] is False:
            chunks = obj["chunks"]
            for chunkid in chunks:
                log.info("delete {}".format(chunkid))
    log.info("delete unmarked objects done")

async def updateDatasetContents(app, domain, dsetid):
    """ Create a object listing all the chunks for given dataset
    """
    log.info("updateDatasetContents: {}".format(dsetid))
    datasets = app["datasets"]
    if dsetid not in datasets:
        log.error("expected to find dsetid")
        return
    dset_obj = datasets[dsetid]
    chunks = dset_obj["chunks"]
    if len(chunks) == 0:
        log.info("no chunks for dataset")
        return
    col_s3key = domain[1:] + "/." + dsetid + ".chunks.txt"  
    if await isS3Obj(app, col_s3key):
        # contents already exist, return
        # TBD: Add an option to force re-creation of index?
        return
         
    # collect s3 stats for all the chunk objects
    for chunk_id in chunks:
        try:
            # TBD - do this in batches for efficiency
            chunk_stats = await getS3ObjStats(app, getS3Key(chunk_id))
        except HttpProcessingError as hpe:
            log.error("error getting chunk stats for chunk: {}".format(chunk_id))
            continue
        chunks[chunk_id] = chunk_stats 
    chunk_ids = list(chunks.keys())
    chunk_ids.sort()
    text_data = b""
    for chunk_id in chunk_ids:
        log.info("getting chunk_obj for {}".format(chunk_id))
        chunk_obj = chunks[chunk_id]
        # chunk_obj should have keys: ETag, Size, and LastModified 
        if "ETag" not in chunk_obj:
            log.warn("chunk_obj for {} not initialized".format(chunk_id))
            continue
        line = "{} {} {} {}\n".format(chunk_id[39:], chunk_obj["ETag"], chunk_obj["LastModified"], chunk_obj["Size"])
        log.info("chunk contents: {}".format(line))
        line = line.encode('utf8')
        text_data += line
    log.info("write chunk collection key: {}, count: {}".format(col_s3key, len(chunk_ids)))
    try:
        await putS3Bytes(app, col_s3key, text_data)
    except HttpProcessingError:
        log.error("S3 Error writing chunk collection key: {}".format(col_s3key))
    
    

async def updateDomainContents(app):
    """ For each domain, create context files listing objids and size for objects in the domain.
    """
    log.info("updateDomainContents start")
     
    domains = app["domains"]
    log.info("{} domains".format(len(domains)))
    for domain in domains:
        log.info("domain: {}".format(domain))
        domain_obj = domains[domain]
        for collection in ("groups", "datatypes", "datasets"):
            domain_col = domain_obj[collection]
            log.info("domain_{} count: {}".format(collection, len(domain_col)))
            col_s3key = domain[1:] + "/." + collection + ".txt"  
            if await isS3Obj(app, col_s3key):
                # Domain collection already exist
                # TBD: add option to force re-creation?
                #continue
                return
            if len(domain_col) > 0:
                col_ids = list(domain_col.keys())
                col_ids.sort()
                text_data = b""
                for obj_id in col_ids:
                    col_obj = domain_col[obj_id]
                    line = "{} {}\n".format(obj_id, col_obj["size"])
                    line = line.encode('utf8')
                    text_data += line
                    if getCollectionForId(obj_id) == "datasets":
                        # create chunk listing
                        await updateDatasetContents(app, domain, obj_id)
                log.info("write collection key: {}, count: {}".format(col_s3key, len(col_ids)))
                try:
                    await putS3Bytes(app, col_s3key, text_data)
                except HttpProcessingError:
                    log.error("S3 Error writing {}.json key: {}".format(collection, col_s3key))
            


async def bucketCheck(app):
    """ Periodic method that iterates through all keys in the bucket  
    """

    #initialize these objecs here rather than in main to avoid "ouside of coroutine" errors

    app["last_bucket_check"] = int(time.time())

    # update/initialize root object before starting node updates
 
    while True:  
        if app["node_state"] != "READY":
            log.info("bucketCheck waiting for Node state to be READY")
            await  asyncio.sleep(1)
            continue

        now = int(time.time())
        log.info("bucket check {}".format(unixTimeToUTC(now)))
        if "domains" not in app:
            # haven't run listKeys yet - create a dict of all keys in the bucket
            await listKeys(app)
        await markAndSweep(app)
        await updateDomainContents(app)

        # sleep for a bit
        sleep_secs = config.get("async_sleep_time")
        await  asyncio.sleep(sleep_secs)
        

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
     
    resp = await jsonResponse(request, answer)
    log.response(request, resp=resp)
    return resp
 

async def init(loop):
    """Intitialize application and return app object"""
    
    app = baseInit(loop, 'an')

    app.router.add_get('/', info)
    app.router.add_get('/info', info)
    
    
    return app

#
# Main
#

if __name__ == '__main__':
    log.info("AsyncNode initializing")
    
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init(loop))   
    # run background tasks
    asyncio.ensure_future(bucketCheck(app), loop=loop)
    asyncio.ensure_future(healthCheck(app), loop=loop)
    async_port = config.get("an_port")
    log.info("Starting service on port: {}".format(async_port))
    run_app(app, port=int(async_port))
