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
# s3Util:
# S3-related functions
# 
import json
from botocore.exceptions import ClientError
from aiohttp.errors import HttpProcessingError 

import hsds_logger as log
import config

def getS3Client(app):
    """ Return s3client handle
    """
    if "s3" in app:
        return app["s3"]
    # first time setup of s3 client
    if "session" not in app:
        raise KeyError("Session not initialized")
    session = app["session"]
    aws_region = config.get("aws_region")
    aws_secret_access_key = config.get("aws_secret_access_key")
    if not aws_secret_access_key or aws_secret_access_key == 'xxx':
        msg="Invalid aws secret access key, using None"
        log.info(msg)
    aws_access_key_id = config.get("aws_access_key_id")
    if not aws_access_key_id or aws_access_key_id == 'xxx':
        msg="Invalid aws access key, using None"
        log.info(msg)

    s3_gateway = config.get('aws_s3_gateway')
    if not s3_gateway:
        msg="Invalid aws s3 gateway"
        log.error(msg)
        raise ValueError(msg)
    log.info("s3_gateway: {}".format(s3_gateway))

    use_ssl = False
    if s3_gateway.startswith("https"):
        use_ssl = True
    s3 = session.create_client('s3', region_name=aws_region,
                                   aws_secret_access_key=aws_secret_access_key,
                                   aws_access_key_id=aws_access_key_id,
                                   endpoint_url=s3_gateway,
                                   use_ssl=use_ssl)

    app['s3'] = s3  # save so same client can be returned in subsiquent calls

    return s3

def releaseClient(app):
    """ release the client collection to s3
     (Used for cleanup on application exit)
    """
    if 's3' in app:
        client = app['s3']
        client.close()
        del app['s3']

def s3_stats_increment(app, counter, inc=1):
    """ Incremenet the indicated connter
    """
    if "s3_stats" not in app:
        return # app hasn't set up s3stats
    s3_stats = app['s3_stats']
    if counter not in s3_stats:
        log.error("unexpected counter for s3_stats: {}".format(counter))
        return
    if inc < 1:
        log.error("unexpected inc for s3_stats: {}".format(inc))
        return
        
    s3_stats[counter] += inc
 
async def getS3JSONObj(app, key):
    """ Get S3 object identified by key and read as JSON
    """
    
    client = getS3Client(app)
    bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info("getS3JSONObj({})".format(key))
    s3_stats_increment(app, "get_count")
    try:
        resp = await client.get_object(Bucket=bucket, Key=key)
        data = await resp['Body'].read()
        resp['Body'].close()
    except ClientError as ce:
        # key does not exist?
        # check for not found status
        # Note: Error.Code should always exist - cf https://github.com/boto/botocore/issues/885
        is_404 = False
        if "ResponseMetadata" in ce.response:
            metadata = ce.response["ResponseMetadata"]
            if "HTTPStatusCode" in metadata:
                if metadata["HTTPStatusCode"] == 404:
                    is_404 = True
        if is_404:
            msg = "s3_key: {} not found ".format(key,)
            log.info(msg)
            raise HttpProcessingError(code=404, message=msg)
        else:
            s3_stats_increment(app, "error_count")
            log.warn("got ClientError on s3 get: {}".format(str(ce)))
            msg = "Error getting s3 obj: " + str(ce)
            log.error(msg)
            raise HttpProcessingError(code=500, message=msg)

    s3_stats_increment(app, "bytes_in", inc=len(data)) 
    json_dict = json.loads(data.decode('utf8'))
    log.info("s3 returned: {}".format(json_dict))
    return json_dict

async def getS3Bytes(app, key):
    """ Get S3 object identified by key and read as bytes
    """
    
    client = getS3Client(app)
    bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info("getS3Bytes({})".format(key))
    s3_stats_increment(app, "get_count")
    try:
        resp = await client.get_object(Bucket=bucket, Key=key)
        data = await resp['Body'].read()
        resp['Body'].close()
    except ClientError as ce:
        # key does not exist?
        # check for not found status
        is_404 = False
        if "ResponseMetadata" in ce.response:
            metadata = ce.response["ResponseMetadata"]
            if "HTTPStatusCode" in metadata:
                if metadata["HTTPStatusCode"] == 404:
                    is_404 = True
        if is_404:
            msg = "s3_key: {} not found ".format(key,)
            log.warn(msg)
            raise HttpProcessingError(code=404, message=msg)
        else:
            s3_stats_increment(app, "error_count")
            log.warn("got ClientError on s3 get: {}".format(str(ce)))
            msg = "Error getting s3 obj: " + str(ce)
            log.error(msg)
            raise HttpProcessingError(code=500, message=msg)

    if data and len(data) > 0:
        s3_stats_increment(app, "bytes_in", inc=len(data))
    return data

async def putS3JSONObj(app, key, json_obj):
    """ Store JSON data as S3 object with given key
    """
   
    client = getS3Client(app)
    bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info("putS3JSONObj({})".format(key))
    s3_stats_increment(app, "put_count")
    data = json.dumps(json_obj)
    data = data.encode('utf8')
    try:
        await client.put_object(Bucket=bucket, Key=key, Body=data)
    except ClientError as ce:
        s3_stats_increment(app, "error_count")
        msg = "Error putting s3 obj: " + str(ce)
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg)
    if data and len(data) > 0:
        s3_stats_increment(app, "bytes_out", inc=len(data))
    log.info("putS3JSONObj complete")

async def putS3Bytes(app, key, data):
    """ Store byte string as S3 object with given key
    """
    
    client = getS3Client(app)
    bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info("putS3Bytes({})".format(key))
    s3_stats_increment(app, "put_count")
    try:
        await client.put_object(Bucket=bucket, Key=key, Body=data)
    except ClientError as ce:
        s3_stats_increment(app, "error_count")
        msg = "Error putting s3 obj: " + str(ce)
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg)
    if data and len(data) > 0:
        s3_stats_increment(app, "bytes_in", inc=len(data))
    log.info("putS3Bytes complete")

async def deleteS3Obj(app, key):
    """ Delete S3 object identfied by given key
    """
    
    client = getS3Client(app)
    bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info("deleteS3Obj({})".format(key))
    s3_stats_increment(app, "delete_count")
    try:
        await client.delete_object(Bucket=bucket, Key=key)
    except ClientError as ce:
        # key does not exist? 
        s3_stats_increment(app, "error_count")
        msg = "Error deleting s3 obj: " + str(ce)
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg)
    log.info("deleteS3Obj complete")

async def getS3ObjStats(app, key):
    """ Return etag, size, and last modified time for given object
    """
    
    client = getS3Client(app)
    bucket = app['bucket_name']
    stats = {}
    
    if key[0] == '/':
        #key = key[1:]  # no leading slash
        msg = "key with leading slash: {}".format(key)
        log.error(msg)
        raise KeyError(msg)

    log.info("getS3ObjStats({})".format(key))
    
    s3_stats_increment(app, "list_count")
    try:
        resp = await client.list_objects(Bucket=bucket, MaxKeys=1, Prefix=key)
    except ClientError as ce:
        # key does not exist? 
        s3_stats_increment(app, "error_count")
        msg = "Error listing s3 obj: " + str(ce)
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg)
    if 'Contents' not in resp:
        msg = "key: {} not found".format(key)
        log.info(msg)
        raise HttpProcessingError(code=404, message=msg)
    contents = resp['Contents']
    log.info("s3_contents: {}".format(contents))
    
    found = False
    if len(contents) > 0:
        item = contents[0]
        if item["Key"] == key:
            # if the key is a S3 folder, the key will be the first object in the folder,
            # not the requested object
            found = True
            if item["ETag"]:
                stats["ETag"] = item["ETag"]
            else:
                if "Owner" in item and "ID" in item["Owner"] and item["Owner"]["ID"] == "minio":
                    pass # minio is not creating ETags...
                else:
                    log.warn("No ETag for key: {}".format(key))
                # If no ETAG put in a fake one
                stats["ETag"] = "9999"
            stats["Size"] = item["Size"]
            stats["LastModified"] = int(item["LastModified"].timestamp())
    if not found:
        msg = "key: {} not found".format(key)
        log.info(msg)
        raise HttpProcessingError(code=404, message=msg)

    return stats
    
async def isS3Obj(app, key):
    """ Test if the given key maps to S3 object
    """
    found = False
    client = getS3Client(app)
    bucket = app['bucket_name']
    log.info("isS3Obj {}".format(key))
    s3_stats_increment(app, "list_count")
    try:
        resp = await client.list_objects(Bucket=bucket, MaxKeys=1, Prefix=key)
    except ClientError as ce:
        # key does not exist? 
        # TBD - does this ever get triggered when the key is present?
        log.warn("isS3Obj {} client error: {}".format(key, str(ce)))
        s3_stats_increment(app, "error_count")
        return False
    if 'Contents' not in resp:
        log.info("isS3Obj {} not found (no Contents)".format(key))
        return False
    contents = resp['Contents']
    if len(contents) > 0:
        item = contents[0]
        if item["Key"] == key:
            # if the key is a S3 folder, the key will be the first object in the folder,
            # not the requested object
            found = True
    log.info("isS3Obj {} returning {}".format(key, found))
    return found

        
     
"""
Helper function for getKeys
"""
async def _fetch_all(pages):
    responses = []
    while True:
        n = await pages.next_page()
        if n is None:
            break
        responses.append(n)
    return responses
    

async def getS3Keys(app, prefix='', deliminator='', suffix='', include_stats=False):
    # return keys matching the arguments
    s3_client = getS3Client(app)
    bucket_name = app['bucket_name']
    log.info("getS3Keys('{}','{}','{}')".format(prefix, deliminator, suffix))
    paginator = s3_client.get_paginator('list_objects')
     
    # TBD - how to paginate when more than 1000 keys are present
    # TBD - for some reason passing in non-null deliminator doesn't work
    pages = paginator.paginate(MaxKeys=1000, Bucket=bucket_name, Prefix=prefix, Delimiter=deliminator)
    responses = await _fetch_all(pages)
    log.info("getS3Keys, got {} responses".format(len(responses)))
    if include_stats:
        # use a dictionary to hold return values
        key_names = {}
    else:
        # just use a list
        key_names = []
    last_key = None
    for response in responses:
        if 'CommonPrefixes' in response:
            log.info("got CommonPrefixes in s3 response")
            common = response["CommonPrefixes"]
            for item in common:
                if 'Prefix' in item:
                    log.info("got s3 prefix: {}".format(item['Prefix']))
                    if include_stats:
                        # TBD: not sure what makes sense to include for stats here
                        key_names[item['Prefix']] = {}
                    else:
                        key_names.append(item['Prefix'])

        elif 'Contents' in response:
            log.info("got Contents in s3 response")
            contents = response['Contents']
            for item in contents:
                key_name = item['Key']
                log.info("got s3key: {}".format(key_name))
                if suffix and not key_name.endswith(suffix):
                    log.info("got s3key without suffix")
                    continue
                
                if prefix:
                    key_name = key_name[len(prefix):]  # just include after prefix
                    print("adjusted key_name:", key_name)
                if suffix:
                    n = len(suffix)
                    key_name = key_name[:-n]
                if deliminator:
                    if key_name.endswith(deliminator):
                        key_name = key_name[:-1]  # dont show ending deliminator
                    if key_name.startswith(deliminator):
                        key_name = key_name[1:]
                    index = key_name.find(deliminator)
                    if index > 0:
                        # include just to the deliminator
                        key_name = key_name[:index]
                    if not key_name:
                        continue  # trimed away to non-existence
                    if last_key and key_name == last_key:
                        continue  # not unique
                    last_key = key_name
                if include_stats:
                    stats = {}
                    if item["ETag"]:
                        stats["Etag"] = item["Etag"]
                    else:
                        if "Owner" in item and "ID" in item["Owner"] and item["Owner"]["ID"] == "minio":
                            pass # minio is not creating ETags...
                        else:
                            log.warn("No ETag for key: {}".format(key_name))
                        # If no ETAG put in a fake one
                        stats["ETag"] = "9999"
                    if "Size" in item:
                        stats["Size"] = item["Size"]
                    else:
                        log.warn("No Size for key: {}".format(key_name))
                    if "LastModified" in item:
                        stats["LastModified"] = int(item["LastModified"].timestamp())
                    else:
                        log.warn("No LastModified for key: {}".format(key_name))
                    key_names[key_name] = stats
                else:
                    key_names.append(key_name)
                
    return key_names



"""
Initialize the s3 stat collection dict
"""
def getInitialS3Stats():
    s3_stats = {}
    s3_stats["get_count"] = 0
    s3_stats["put_count"] = 0
    s3_stats["delete_count"] = 0
    s3_stats["list_count"] = 0
    s3_stats["error_count"] = 0
    s3_stats["bytes_in"] = 0
    s3_stats["bytes_out"] = 0
    return s3_stats