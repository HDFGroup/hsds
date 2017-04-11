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
    
    client = app['s3']
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
        s3_stats_increment(app, "error_count")
        log.warn("clientError exception: {}".format(str(ce)))
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

    s3_stats_increment(app, "bytes_in", inc=len(data)) 
    json_dict = json.loads(data.decode('utf8'))
    log.info("s3 returned: {}".format(json_dict))
    return json_dict

async def getS3Bytes(app, key):
    """ Get S3 object identified by key and read as bytes
    """
    
    client = app['s3']
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

    s3_stats_increment(app, "bytes_in", inc=len(data))
    return data

async def putS3JSONObj(app, key, json_obj):
    """ Store JSON data as S3 object with given key
    """
   
    client = app['s3']
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
    s3_stats_increment(app, "bytes_out", inc=len(data))
    log.info("putS3JSONObj complete")

async def putS3Bytes(app, key, data):
    """ Store byte string as S3 object with given key
    """
    
    client = app['s3']
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
    s3_stats_increment(app, "bytes_in", inc=len(data))
    log.info("putS3Bytes complete")

async def deleteS3Obj(app, key):
    """ Delete S3 object identfied by given key
    """
    
    client = app['s3']
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


    
async def isS3Obj(app, key):
    """ Test if the given key maps to S3 object
    """
    
    client = app['s3']
    bucket = app['bucket_name']

    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info("isS3Obj({})".format(key))
    
    s3_stats_increment(app, "list_count")
    try:
        resp = await client.list_objects(Bucket=bucket, MaxKeys=1, Prefix=key)
    except ClientError as ce:
        # key does not exist? 
        s3_stats_increment(app, "error_count")
        msg = "Error listing s3 obj: " + str(ce)
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg)
    #print("list_object resp: {}".format(resp))
    if 'Contents' not in resp:
        return False
    contents = resp['Contents']
    
    found = False
    if len(contents) > 0:
        #print("list_objects contents:", contents)
        item = contents[0]
        if item["Key"] == key:
            # if the key is a S3 folder, the key will be the first object in the folder,
            # not the requested object
            found = True
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
    

async def getS3Keys(app, prefix='', deliminator='', suffix=''):
    # return keys matching the arguments
    s3_client = app['s3']
    bucket_name = app['bucket_name']
    log.info("getS3Keys({},{},{})".format(prefix, deliminator, suffix))
    paginator = s3_client.get_paginator('list_objects')
     
    # TBD - how to paginate when more than 1000 keys are present
    # TBD - for some reason passing in non-null deliminator doesn't work
    pages = paginator.paginate(MaxKeys=1000, Bucket=bucket_name, Prefix=prefix, Delimiter='')
    responses = await _fetch_all(pages)
    log.info("getS3Keys, got {} responses".format(len(responses)))
    key_names = []
    last_key = None
    for response in responses:
        if 'CommonPrefixes' in response:
            log.info("got common prefixes in s3 response")
            common = response["CommonPrefixes"]
            for item in common:
                if 'Prefix' in item:
                    log.info("got s3 prefix: {}".format(item['Prefix']))
                    key_names.append(item['Prefix'])

        elif 'Contents' in response:
            log.info("got Contents in s3 response")
            contents = response['Contents']
            for item in contents:
                key_name = item['Key']
                log.info("got s3key: {}".format(key_name))
                if suffix and not key_name.endswith(suffix):
                    continue
                
                if prefix:
                    key_name = key_name[len(prefix):]  # just include after prefix
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
                key_names.append(key_name)
                
    return key_names

def getS3Client(session):
    """ Return s3client handle
    """

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
    aws_client = session.create_client('s3', region_name=aws_region,
                                   aws_secret_access_key=aws_secret_access_key,
                                   aws_access_key_id=aws_access_key_id,
                                   endpoint_url=s3_gateway,
                                   use_ssl=use_ssl)
    return aws_client

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