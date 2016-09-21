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
from aiohttp import HttpProcessingError 

import hsds_logger as log
 
async def getS3JSONObj(app, key):
    """ Get S3 object identified by key and read as JSON
    """
    log.info("getS3JSONObj({})".format(key))
    client = app['s3']
    bucket = app['bucket_name']
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
            log.warn("got ClientError on s3 get: {}".format(str(ce)))
            msg = "Error getting s3 obj: " + str(ce)
            log.error(msg)
            raise HttpProcessingError(code=500, message=msg)
       
    json_dict = json.loads(data.decode('utf8'))
    log.info("s3 returned: {}".format(json_dict))
    return json_dict

async def putS3JSONObj(app, key, json_obj):
    """ Store JSON data as S3 object with given key
    """
    log.info("putS3JSONObj({})".format(key))
    client = app['s3']
    bucket = app['bucket_name']
    data = json.dumps(json_obj)
    data = data.encode('utf8')
    try:
        await client.put_object(Bucket=bucket, Key=key, Body=data)
    except ClientError as ce:
        msg = "Error putting s3 obj: " + str(ce)
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg)
    log.info("putS3JSONObj complete")

async def deleteS3Obj(app, key):
    """ Delete S3 object identfied by given key
    """
    log.info("deleteS3Obj({})".format(key))
    client = app['s3']
    bucket = app['bucket_name']
    try:
        await client.delete_object(Bucket=bucket, Key=key)
    except ClientError as ce:
        # key does not exist? 
        msg = "Error deleting s3 obj: " + str(ce)
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg)
    log.info("deleteS3Obj complete")
    
async def isS3Obj(app, key):
    """ Test if the given key maps to S3 object
    """
    log.info("isS3Obj({})".format(key))
    client = app['s3']
    bucket = app['bucket_name']
    try:
        resp = await client.list_objects(Bucket=bucket, MaxKeys=1, Prefix=key)
    except ClientError as ce:
        # key does not exist? 
        msg = "Error listing s3 obj: " + str(ce)
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg)
    if 'Contents' not in resp:
        return False
    contents = resp['Contents']
    
    if len(contents) > 0:
        return True
    else:
        return False


  


 
