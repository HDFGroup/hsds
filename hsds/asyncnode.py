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
import sys
import time

from aiohttp.web import Application, StreamResponse, run_app
#from aiohttp.errors import HttpBadRequest, HttpProcessingError
import aiobotocore

import config
from util.timeUtil import unixTimeToUTC
from util.httpUtil import  jsonResponse, getUrl
from util.s3Util import  isS3Obj, getS3Client, getS3Keys, getS3JSONObj
from util.idUtil import  createNodeId, getHeadNodeS3Key, getCollectionForId, getS3Key
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
        elif s3key.endswith(".domain.json"):
            domain_cnt += 1
            domains[s3key] = {}
            n = s3key.index('/')
            if n == 0:
                log.warn("unexpected domain name (leading slash): {}".format(s3key))
            elif n == -1:
                log.warn("unexpected domain name (no slash): {}".format(s3key))
            else:
                tld = s3key[:n]
                if tld not in top_level_domains:
                    top_level_domains[tld] = {}
            
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
    """ Mark obj as in-use and for group objs, recursively call for hardline objects 
    """
    collection = getCollectionForId(obj_id)
    obj_ids = app[collection]
    if obj_id not in obj_ids:
        log.warn("markObj: key {} not found s3_key: {}".format(obj_id, getS3Key(obj_id)))
        return
    obj = obj_ids[obj_id]
    obj["u"] = True  # in use
    if collection == "groups":
        s3Key = getS3Key(obj_id)
        grp_json = await getS3JSONObj(app, s3Key)
        links = grp_json["links"]
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
        domain = domains[domain_key] 
        obj_json = await getS3JSONObj(app, domain_key)
        if "root" not in obj_json:
            log.info("no root for {}".format(domain))
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



async def bucketCheck(app):
    """ Periodic method that iterates through all keys in the bucket  
    If node doesn't respond, free up the node slot (the node can re-register if it comes back)'"""
    #initialize these objecs here rather than in main to avoid "ouside of coroutine" errors
    
    if 's3' not in app:
        log.info("creating S3 client")
        session = app["session"]
        app['s3'] = getS3Client(session)

    app["last_bucket_check"] = int(time.time())

    # update/initialize root object before starting node updates
    headnode_key = getHeadNodeS3Key()
    log.info("headnode S3 key: {}".format(headnode_key))
    headnode_obj_found = await isS3Obj(app, headnode_key)

    head_url = getUrl(app["head_host"], app["head_port"])  
    log.info("hear_url: {}".format(head_url))
    
    if not headnode_obj_found:
        # first time hsds has run with this bucket name?
        log.warn("headnode not found")
    else:
        log.info("headnode found")


    while True:
        # sleep for a bit
        sleep_secs = config.get("head_sleep_time")
        await  asyncio.sleep(sleep_secs)

        now = int(time.time())
        log.info("bucket check {}".format(unixTimeToUTC(now)))
        await listKeys(app)
        await markAndSweep(app)
        

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
    app = Application(loop=loop)

    # set a bunch of global state 
    app["id"] = createNodeId("async")
    app["start_time"] = int(time.time())  # seconds after epoch 
    bucket_name = config.get("bucket_name")
    if not bucket_name:
        log.error("BUCKET_NAME environment variable not set")
        sys.exit()
    log.info("using bucket: {}".format(bucket_name))
    app["bucket_name"] = bucket_name     
    app["head_host"] = config.get("head_host")
    app["head_port"] = config.get("head_port")
    app.router.add_get('/', info)
    app.router.add_get('/info', info)
    
    
    return app

#
# Main
#

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init(loop))   

    # create a client Session here so that all client requests 
    #   will share the same connection pool
    max_tcp_connections = int(config.get("max_tcp_connections")) 
    app["session"] = aiobotocore.get_session(loop=loop)   
    asyncio.ensure_future(bucketCheck(app), loop=loop)
    async_port = config.get("async_port")
    log.info("Starting service on port: {}".format(async_port))
    run_app(app, port=int(async_port))
