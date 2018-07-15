import asyncio
from os.path import isfile, exists, join
import sys
import sqlite3
from aiobotocore import get_session
from util.s3Util import getS3JSONObj, releaseClient, getS3Keys
from util.idUtil import isS3ObjKey, isValidUuid, isValidChunkId, getS3Key, getObjId
from util.domainUtil import isValidDomain
from util.dbutil import  insertRow, batchInsertChunkTable, getRootObjects, getDatasetChunks, updateRowColumn, dbInitTable, getRow, listObjects
import hsds_logger as log
import config

async def getObjProperties(app, objid):
    """ Get the root property if not already set """
    log.debug("getObjProperties {}".format(objid))
    
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

    rsp = {"root": rootid}

    if "owner" in obj_json:
        rsp["owner"] = obj_json["owner"]
     
    return rsp

async def gets3keys_callback(app, s3keys):
    #
    # this is called for each page of results by getS3Keys 
    #
    
    log.debug("gets3keys_callback count: {} keys".format(len(s3keys)))   
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
            
        lastModified = 0
        if "LastModified" in item:
            lastModified = int(item["LastModified"])
             
        objSize = 0
        if "Size" in item:
            objSize = item["Size"]

        app["objects_in_bucket"]  += 1
        app["bytes_in_bucket"] += objSize

        # save to a table based on the type of object this is
        if isValidDomain(id):
            props = await getObjProperties(app, id)  # get Root property from S3
            try:
                log.debug("insert domain: {}  root: {} owner: {}".format(id, props["root"], props["owner"]))
                insertRow(conn, id, etag=etag, lastModified=lastModified, objSize=objSize, rootid=props["root"], owner=props["owner"])
            except KeyError:
                log.warn("got KeyError inserting domain: {}".format(id))
                continue

        elif isValidChunkId(id):
            log.debug("Got chunk: {}".format(id))
            chunk_items.append((id, etag, objSize, lastModified))
        else:
            props = await getObjProperties(app, id)  # get Root property from S3
             
            try:
                rootid = props["root"]
                if not rootid:
                    log.error("Expected to find root property in object: {}".format(id))
                else:
                    log.debug("insert row id: {} root: {}".format(id, rootid))
                    insertRow(conn, id, etag=etag, lastModified=lastModified, objSize=objSize, rootid=rootid)
                    if id == rootid:
                        log.info("adding {} to rootTable".format(rootid))
                        insertRow(conn, rootid, etag=etag, lastModified=lastModified, objSize=objSize, table="RootTable") 
            except KeyError:
                log.error("got KeyError inserting object: {}".format(id))
                   
         
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

    await asyncio.sleep(0)
    conn = app["conn"]
    
    roots = getRootObjects(conn)
    log.info("getRootObjects got {} roots".format(len(roots)))
    for root_obj in roots:
        rootid = root_obj["id"]
        log.debug("list objects for root: {}".format(rootid))
        list_map = listObjects(conn, rootid=rootid)
        if rootid not in list_map:
            log.error("expected to find {} in listObjectMap".format(rootid))
            continue
        root_object = list_map[rootid]
        totalSize = 0
        totalChunks = 0
        lastModified = 0
        for collection in ("groups", "datasets", "datatypes"):
            log.debug("{}:".format(collection))
            collection_objs = root_object[collection]
            for id in collection_objs:
                log.debug("   {}".format(id))
                obj = collection_objs[id]
                if obj["size"] > 0:
                    totalSize += obj["size"]
                if obj["lastModified"] > lastModified:
                    lastModified = obj["lastModified"]

        # update group count
        if len(root_object["groups"]) == 0:
            log.error("expected to find at least one group obj for root: {}".format(rootid))
            continue
        updateRowColumn(conn, rootid, "groupCount", len(root_object["groups"]), table="RootTable")

        # update datatype count
        if len(root_object["datatypes"]) > 0:
            updateRowColumn(conn, rootid, "typeCount", len(root_object["datatypes"]), table="RootTable")

        # update dataset count    
        if len(root_object["datasets"]) > 0:
            updateRowColumn(conn, rootid, "datasetCount", len(root_object["datasets"]), table="RootTable")
        dataset_objs = root_object["datasets"]

        # gather info about chunks for each dataset
        for datasetid in dataset_objs:
            dset_obj = dataset_objs[datasetid]
            allocated_size = 0
            dset_lastModified = dset_obj["lastModified"]
            # TODO - get chunks in batches
            chunks = getDatasetChunks(conn, datasetid)
            for chunkid in chunks:
                chunk = chunks[chunkid]
                if chunk["size"] > 0:
                    allocated_size += chunk["size"]
                    totalSize += chunk["size"]
                if chunk["lastModified"] > dset_lastModified:
                    dset_lastModified = chunk["lastModified"]
                    if chunk["lastModified"] > lastModified:
                        lastModified = chunk["lastModified"]
            updateRowColumn(conn, datasetid, "totalSize", allocated_size, rootid=rootid)
            if dset_lastModified > dset_obj["lastModified"]:
                updateRowColumn(conn, datasetid, "lastModified", dset_lastModified, rootid=rootid)
            updateRowColumn(conn, datasetid, "chunkCount",  len(chunks), rootid=rootid)
            totalChunks += len(chunks)
         
        # update any root properities that have changed
        if totalChunks > 0:
            updateRowColumn(conn, rootid, "chunkCount", totalChunks, table="RootTable")
        if totalSize > 0:
            updateRowColumn(conn, rootid, "totalSize", totalSize, table="RootTable")
        if lastModified > 0:
            updateRowColumn(conn, rootid, "lastModified", lastModified, table="RootTable")
        row = getRow(conn, rootid, table="RootTable")
        log.info("Updated row: {}".format(row))

    

#
# Main
#

if __name__ == '__main__':
    log.info("rebuild_db initializing")
    
    # we need to setup a asyncio loop to query s3
    loop = asyncio.get_event_loop()
    
    app = {}
    app["bucket_name"] = config.get("bucket_name")
    db_dir = config.get("db_dir")
    if not db_dir:
        log.error("No database directory defined")
        sys.exit(-1)
    if not exists(db_dir):
        log.error("Database directory: {} does not exist".format(db_dir))
        sys.exit(-1)
    
    db_file = config.get("db_file")
    if not db_file:
        log.error("db_file not defined")
        sys.exit(-1)
    db_path = join(db_dir, db_file)
    log.info("db_path: {}".format(db_path))

    if isfile(db_path):
        log.error("Database already exists, delete first")
        sys.exit(-1)
    
    conn = sqlite3.connect(db_path)
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