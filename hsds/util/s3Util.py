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
import hsds_logger as log
 
async def getS3JSONObj(app, key):
    log.info("getS3JSONObj({})".format(key))
    client = app['s3']
    bucket = app['bucket_name']
    resp = await client.get_object(Bucket=bucket, Key=key)
    data = await resp['Body'].read()
    resp['Body'].close()
    json_dict = json.loads(data.decode('utf8'))
    log.info("s3 returned: {}".format(json_dict))
    return json_dict

async def putS3JSONObj(app, key, json_obj):
    log.info("putS3JSONObj({})".format(key))
    client = app['s3']
    bucket = app['bucket_name']
    data = json.dumps(json_obj)
    data = data.encode('utf8')
    await client.put_object(Bucket=bucket, Key=key, Body=data)
    log.info("putS3JSONObj complete")

async def deleteS3Obj(app, key):
    log.info("deleteS3Obj({})".format(key))
    client = app['s3']
    bucket = app['bucket_name']
    await client.delete_object(Bucket=bucket, Key=key)
    log.info("deleteS3Obj complete")
    
async def isS3Obj(app, key):
    log.info("isS3Obj({})".format(key))
    client = app['s3']
    bucket = app['bucket_name']
    resp = await client.list_objects(Bucket=bucket, MaxKeys=1, Prefix=key)
    if 'Contents' not in resp:
        return False
    contents = resp['Contents']
    
    if len(contents) > 0:
        return True
    else:
        return False


  


 
