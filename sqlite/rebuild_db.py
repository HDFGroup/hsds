import asyncio
import sys
import sqlite3
from aiobotocore import get_session
from util.s3Util import getS3Keys, getS3JSONObj, releaseClient
from util.idUtil import isS3ObjKey, isValidUuid, isValidChunkId, getS3Key, getObjId
from util.domainUtil import isValidDomain
from dbutil import dbInitTable, insertDomainTable, batchInsertChunkTable, insertObjectTable, insertTLDTable
import hsds_logger as log
import config

async def getRootProperty(app, objid):
    """ Get the root property if not already set """
    log.debug("getRootProperty {}".format(objid))
    
    if isValidDomain(objid):
        log.debug("got domain id: {}".format(objid))
    else:
        if not isValidUuid(objid) or isValidChunkId(objid):
            raise ValueError("unexpected key for root property: {}".format(objid))
    s3key = getS3Key(objid)
    obj_json = await getS3JSONObj(app, s3key)
    rootid = None
    if "root" not in obj_json:
        if isValidDomain(objid):
            log.info("No root for folder domain: {}".format(objid))
        else:
            log.error("no root for {}".format(objid))
    else:
        rootid = obj_json["root"]
        log.debug("got rootid {} for obj: {}".format(rootid, objid))
    return rootid

async def gets3keys_callback(app, s3keys):
    #
    # this is called for each page of results by getS3Keys 
    #
    
    log.debug("getS3Keys count: {} keys".format(len(s3keys)))   
    chunk_items = [] # batch up chunk inserts for faster processing 

    for s3key in s3keys:
        log.debug("got s3key: {}".format(s3key))
        if not isS3ObjKey(s3key):
            log.debug("ignoring: {}".format(s3key))
            continue

        item = s3keys[s3key]   
        id = getObjId(s3key)
        log.debug("object id: {}".format(id))
        
        etag = ''
        if "ETag" in item:
            etag = item["ETag"]
            if len(etag) > 2 and etag[0] == '"' and etag[-1] == '"':
                # for some reason the etag is quoated
                etag = etag[1:-1]
            
        lastModified = ''
        if "LastModified" in item:
            lastModified = item["LastModified"]
             
        objSize = 0
        if "Size" in item:
            objSize = item["Size"]

        app["objects_in_bucket"]  += 1
        app["bytes_in_bucket"] += objSize

        # save to a table based on the type of object this is
        if isValidDomain(id):
            rootid = await getRootProperty(app, id)  # get Root property from S3
            try:
                insertDomainTable(conn, id, etag=etag, lastModified=lastModified, objSize=objSize, rootid=rootid)
            except KeyError:
                log.warn("got KeyError inserting domain: {}".format(id))
                continue
            # add to Top-Level-Domain table if this is not a subdomain
            index = id[1:-1].find('/')  # look for interior slash - isValidDomain implies len > 2
            if index == -1:
                try:
                    insertTLDTable(conn, id)
                except KeyError:
                    log.warn("got KeyError inserting TLD: {}".format(id))
                    continue
        elif isValidChunkId(id):
            log.debug("Got chunk: {}".format(id))
            chunk_items.append((id, etag, objSize, lastModified))
        else:
            rootid = await getRootProperty(app, id)  # get Root property from S3
            try:
                insertObjectTable(conn, id, etag=etag, lastModified=lastModified, objSize=objSize, rootid=rootid)
            except KeyError:
                log.warn("got KeyError inserting object: {}".format(id))
                continue
         
    # insert any remaining chunks
    if len(chunk_items) > 0:
        log.info("batchInsertChunkTable = {} items".format(len(chunk_items)))
        try:
            batchInsertChunkTable(conn, chunk_items)
        except KeyError:
            log.warn("got KeyError inserting chunk: {}".format(id))
            sys.exit(1)
    log.debug("gets3keys_callback done")

#
# List objects in the s3 bucket
#

async def listKeys(app):
    """ Get all s3 keys in the bucket and create list of objkeys and domain keys """
    
    log.info("listKeys start") 
    # Get all the keys for the bucket
    # request include_stats, so that for each key we get the ETag, LastModified, and Size values.
    await getS3Keys(app, include_stats=True, callback=gets3keys_callback)

#
# Main
#

if __name__ == '__main__':
    log.info("rebuild_db initializing")
    
    # we need to setup a asyncio loop to query s3
    loop = asyncio.get_event_loop()
    
    app = {}
    app["bucket_name"] = config.get("bucket_name")
    db_file = config.get("db_file")
    conn = sqlite3.connect(db_file)
    app["conn"] = conn
    app["bytes_in_bucket"] = 0
    app["objects_in_bucket"] = 0

    try:
        dbInitTable(conn)
    except sqlite3.OperationalError:
        print("Error creating db tables. Remove db file: {} and try again".format(db_file))
        sys.exit(-1)
    
    app["loop"] = loop

    session = get_session(loop=loop)
    app["session"] = session
    loop.run_until_complete(listKeys(app))
    releaseClient(app)
    loop.close()
    conn.close()

    print("done!")
    print("objects in bucket: {}".format(app["objects_in_bucket"]))
    print("bytes in bucket: {}".format(app["bytes_in_bucket"]))