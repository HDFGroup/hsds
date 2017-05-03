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

from aiohttp.web import run_app
from aiohttp.errors import HttpProcessingError

import config
from basenode import baseInit, healthCheck
from util.timeUtil import unixTimeToUTC
from util.s3Util import putS3Bytes, isS3Obj, deleteS3Obj
from util.idUtil import getCollectionForId, getS3Key
from asyncnode_lib import listKeys, markObj
import hsds_logger as log
 
FORCE_CONTENT_LIST_CREATION = True
 
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
    # TBD: Replace with domainUtil func
    col_s3key = domain[1:] + "/." + dsetid + ".chunks.txt"  
    if await isS3Obj(app, col_s3key):
        # contents already exist, return
        # TBD: Add an option to force re-creation of index?
        if not FORCE_CONTENT_LIST_CREATION:
            return
         
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
  

async def updateDomainContent(app, domain):
    """ Create/update context files listing objids and size for objects in the domain.
    """
    log.info("updateDomainContent: {}".format(domain))
     
    domains = app["domains"]
    log.info("{} domains".format(len(domains)))
     
    domain_obj = domains[domain]
    # for folder objects, the domain_obj won't have a groups key
    if "groups" not in domain_obj or len(domain_obj["groups"]) == 0:
        log.info("Folder domain skipping: {}".format(domain))
        return  # just a folder domain
    for collection in ("groups", "datatypes", "datasets"):
        domain_col = domain_obj[collection]
        log.info("domain_{} count: {}".format(collection, len(domain_col)))
        col_s3key = domain[1:] + "/." + collection + ".txt"  
        if await isS3Obj(app, col_s3key):
            # Domain collection already exist
            # TBD: add option to force re-creation?
            if not FORCE_CONTENT_LIST_CREATION:
                continue
        if len(domain_col) > 0:
            col_ids = list(domain_col.keys())
            col_ids.sort()
            text_data = b""
            for obj_id in col_ids:
                col_obj = domain_col[obj_id]
                line = "{} {} {} {}\n".format(obj_id, col_obj["ETag"], col_obj["LastModified"], col_obj["Size"])
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

    log.info("updateDomainContent: {} Done".format(domain))

async def deleteObj(app, objid):
    """ Delete the given object if it has been created more the x seconds ago.
    Log S3 errors, but don't raise an exception. """
    s3keys = app["s3keys"]
    log.info("deleteObj {}".format(objid))
    s3key = getS3Key(objid)
    if s3key not in s3keys:
        log.error("deleteObj, key: {} not found".format(s3key))
        return False
    obj = s3keys[s3key]
    if "LastModified" not in obj:
        log.error("Expected LastModified in s3key dict")
        return False
    lastModified = obj["LastModified"]
    now = time.time()
    if now - lastModified < app["anonymous_ttl"]:
        log.info("obj: {} isn't old enough to delete yet".format(objid))
        return False

    if "Size" not in obj:
        log.error("Expected Size is s3key dict")
        return False
    num_bytes = obj["Size"]
    log.info("Remove s3key: {}, [{} bytes]".format(s3key, num_bytes))
    del s3keys[s3key]  # remove from global key list
    try:
        await deleteS3Obj(app, s3key)
        bucket_stats = app["bucket_stats"]
        if "deleted_count" not in bucket_stats:
            bucket_stats["deleted_count"] = 0
        bucket_stats["deleted_count"] = bucket_stats["deleted_count"] + 1
    except HttpProcessingError as hpe:
        log.warn("Error deleting s3 obj: {}".format(hpe.code))
        # TBD: add back to s3keys?
    await  asyncio.sleep(0)
    return True

async def sweepObjs(app):
    """ Iterate through the object tree and delete any unlinked objects """
    #s3keys = app["s3keys"]
    groups = app["groups"]
    datasets = app["datasets"]
    datatypes = app["datatypes"]
    chunks = app["chunks"]
    bucket_stats = app["bucket_stats"]
    if "deleted_count" not in bucket_stats:
        bucket_stats["deleted_count"] = 0
    log.info("sweepObjs")
    deleted_count = bucket_stats["deleted_count"]

    deleted_ids = []
    for dsetid in datasets:
        dset_obj = datasets[dsetid]
        if dset_obj["used"]:
            continue  # in use
        if await deleteObj(app, dsetid):
            deleted_ids.append(dsetid)

        # delete any chunks
        if "chunks" in dset_obj:
            deleted_chunk_ids = []
            dset_chunks = dset_obj["chunks"]
            for chunkid in dset_chunks:
                if await deleteObj(app, chunkid):
                    deleted_chunk_ids.append(chunkid)
            for chunkid in deleted_chunk_ids:
                del dset_chunks[chunkid]
                del chunks[chunkid]
    # nor delete the dataset ids            
    for objid in deleted_ids:
        del datasets[objid]

    deleted_ids = []
    for datatypeid in datatypes:
        datatype_obj = datatypes[datatypeid]
        if datatype_obj["used"]:
            continue  # in use
        if await deleteObj(app, datatypeid):
            deleted_ids.append(datatypeid)
    for objid in deleted_ids:
        del datatypes[objid]
    deleted_ids = []
    for groupid in groups:
        group_obj = groups[groupid]
        if group_obj["used"]:
            continue  # in use
        if await deleteObj(app, groupid):
            deleted_ids.append(groupid)
    for objid in deleted_ids:
        del groups[objid]
    # iteratate through 
    log.info("SweepObj done")



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
        else:
            break

    now = int(time.time())
    log.info("bucket check {}".format(unixTimeToUTC(now)))
    # do initial listKeys
    await listKeys(app)
     
    log.info("Mark domain objects")
    domains = app["domains"]
    # check each domain
    for domain in domains:
        await markObj(app, domain)
    # remove any unlinked objects
    await sweepObjs(app)
    updateBucketStats(app)

    # do GC for all domains at startup
    for domain in domains:
        log.info("domain: {}".format(domain))

    for domain in domains:
        log.info("domain: {}".format(domain))
        # organize collections of groups/datasets/and datatypes for each domain

        await updateDomainContent(app, domain)
        """ 
        try:
            await updateDomainContent(app, domain)
        except Exception  as e:
            log.warn("got exception in updateDomainContent for domain: {}: {}".format(domain, e))
            continue 
        """

    while True:
        # sleep for a bit
        sleep_secs = config.get("async_sleep_time")
        await  asyncio.sleep(sleep_secs)

def updateBucketStats(app):  
    """ Collect some high level stats for use by the info request """
    bucket_stats = app["bucket_stats"]
    if "s3keys" in app:
        s3keys = app["s3keys"]
        bucket_stats["object_count"] = len(s3keys)
    if "domains" in app:
        domains = app["domains"]
        bucket_stats["domain_count"] = len(domains)
    if "groups" in app:
        groups = app["groups"]
        bucket_stats["group_count"] = len(groups)
    if "datasets" in app:
        datasets = app["datasets"]
        bucket_stats["dataset_count"] = len(datasets)
    if "datatypes" in app:
        datatypes = app["datatypes"]
        bucket_stats["datatype_count"] = len(datatypes)
    if "chunks" in app:
        chunks = app["chunks"]
        bucket_stats["chunk_count"] = len(chunks)
    if "bytes_in_bucket" in app:
        bucket_stats["storage_size"] = app["bytes_in_bucket"] 

async def init(loop):
    """Intitialize application and return app object"""
    
    app = baseInit(loop, 'an')
    app["bucket_stats"] = {}
     
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
    app["anonymous_ttl"] = config.get("anonymous_ttl")
    log.info("Starting service on port: {}".format(async_port))
    run_app(app, port=int(async_port))
