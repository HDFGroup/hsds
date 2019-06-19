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
import asyncio
from  inspect import iscoroutinefunction
import json
import time
import zlib
import subprocess
import datetime
from botocore.exceptions import ClientError
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError
from asyncio import CancelledError
from aiobotocore.config import AioConfig
 

import hsds_logger as log
import config

def getS3Client(app):
    """ Return s3client handle
    """

    if "session" not in app:
        # app startup should have set this
        raise KeyError("Session not initialized")
    session = app["session"]

    if "s3" in app:
        if "token_expiration" in app:
            # check that our token is not about to expire
            expiration = app["token_expiration"]
            now = datetime.datetime.now()
            delta = expiration - now
            if delta.total_seconds() > 10:
                return app["s3"]
            # otherwise, fall through and get a new token
            log.info("S3 access token has expired - renewing")
        else:
            return app["s3"]
    
    # first time setup of s3 client or limited time token has expired
    aws_region = config.get("aws_region")
    log.info(f"aws_region {aws_region}")
    aws_secret_access_key = None
    aws_access_key_id = None 
    aws_session_token = None
    aws_iam_role = config.get("aws_iam_role")
    aws_secret_access_key = config.get("aws_secret_access_key")
    aws_access_key_id = config.get("aws_access_key_id")
    if not aws_secret_access_key or aws_secret_access_key == 'xxx':
        log.info("aws secret access key not set")
        aws_secret_access_key = None
    if not aws_access_key_id or aws_access_key_id == 'xxx':
        log.info("aws access key id not set")
        aws_access_key_id = None
  
    if aws_iam_role and not aws_secret_access_key:
        log.info("using iam role: {}".format(aws_iam_role))
        log.info("getting EC2 IAM role credentials")
        # Use EC2 IAM role to get credentials
        # See: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html?icmpid=docs_ec2_console
        curl_cmd = ["curl", "http://169.254.169.254/latest/meta-data/iam/security-credentials/{}".format(aws_iam_role)]
        p = subprocess.run(curl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if p.returncode != 0:
            msg = "Error getting IAM role credentials: {}".format(p.stderr)
            log.error(msg)
        else:
            stdout = p.stdout.decode("utf-8")
            try:
                cred = json.loads(stdout)
                aws_secret_access_key = cred["SecretAccessKey"]
                aws_access_key_id = cred["AccessKeyId"]
                log.info("Got ACCESS_KEY_ID: {} from EC2 metadata".format(aws_access_key_id))     
                aws_session_token = cred["Token"]
                log.info("Got Expiration of: {}".format(cred["Expiration"]))
                expiration_str = cred["Expiration"][:-1] + "UTC" # trim off 'Z' and add 'UTC'
                # save the expiration
                app["token_expiration"] = datetime.datetime.strptime(expiration_str, "%Y-%m-%dT%H:%M:%S%Z")
            except json.JSONDecodeError:
                msg = "Unexpected error decoding EC2 meta-data response"
                log.error(msg)
            except KeyError:
                msg = "Missing expected key from EC2 meta-data response"
                log.error(msg)
       
    s3_gateway = config.get('aws_s3_gateway')
    if not s3_gateway:
        msg="Invalid aws s3 gateway"
        log.error(msg)
        raise ValueError(msg)
    if s3_gateway[0] == '[' and s3_gateway[-1] == ']':
        # convert string to a comma separated list
        items = s3_gateway[1:-1].split(',')
        s3_gateway = []
        for item in items:
            s3_gateway.append(item.strip())
    if isinstance(s3_gateway, list):
        # use the node number to select an item from the list
        node_number = 0
        if "node_number" in app:
            node_number = app["node_number"]
        item = s3_gateway[node_number % len(s3_gateway)]
        log.debug(f"selecting: {item} from s3_gateway list: {s3_gateway}")
        s3_gateway = item
    log.info(f"Using S3Gateway: {s3_gateway}")
    use_ssl = False
    if s3_gateway.startswith("https"):
        use_ssl = True
    max_pool_connections = config.get('aio_max_pool_connections')
    aio_config = AioConfig(max_pool_connections=max_pool_connections)
    s3 = session.create_client('s3', region_name=aws_region,
                                   aws_secret_access_key=aws_secret_access_key,
                                   aws_access_key_id=aws_access_key_id,
                                   aws_session_token=aws_session_token,
                                   endpoint_url=s3_gateway,
                                   use_ssl=use_ssl,
                                   config=aio_config)

    app['s3'] = s3  # save so same client can be returned in subsequent calls

    return s3

async def releaseClient(app):
    """ release the client collection to s3
     (Used for cleanup on application exit)
    """
    if 's3' in app:
        client = app['s3']
        await client.close()
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
 
async def getS3JSONObj(app, key, bucket=None):
    """ Get S3 object identified by key and read as JSON
    """
    
    client = getS3Client(app)
    if not bucket:
        bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info(f"getS3JSONObj(s3://{bucket})/{key}")
    s3_stats_increment(app, "get_count")
    try:
        resp = await client.get_object(Bucket=bucket, Key=key)
        data = await resp['Body'].read()
        resp['Body'].close()
    except ClientError as ce:
        # key does not exist?
        # check for not found status
        # Note: Error.Code should always exist - cf https://github.com/boto/botocore/issues/885
        response_code = ce.response['Error']['Code']
        log.info(f"ClientError on getS3JSONObj key: {key} bucket: {bucket}: {response_code}")

        # remove key from pending map if present
        if "pending_s3_read" in app:  
            pending_s3_read = app["pending_s3_read"]
            if key in pending_s3_read:
                log.debug(f"remove {key} from pending_s3_read")
                del pending_s3_read[key]
         
        if response_code == "NoSuchKey":
            msg = f"s3_key: {key} not found "
            log.info(msg)
            raise HTTPNotFound()
        elif response_code == "NoSuchBucket":
            msg = f"s3_bucket: {bucket} not fiound"
            log.info(msg)
            raise HTTPNotFound()
        else:
            s3_stats_increment(app, "error_count")
            log.warn("got ClientError on s3 get: {}".format(str(ce)))
            msg = "Error getting s3 obj: " + str(ce)
            log.error(msg)
            raise HTTPInternalServerError()

    s3_stats_increment(app, "bytes_in", inc=len(data)) 
    try:
        json_dict = json.loads(data.decode('utf8'))
    except UnicodeDecodeError:
        s3_stats_increment(app, "error_count")
        log.error("Error loading JSON at key: {}".format(key))
        msg = "Unexpected i/o error"
        raise HTTPInternalServerError()

    log.debug("s3 returned: {}".format(json_dict))
    return json_dict

async def getS3Bytes(app, key, deflate_level=None, s3offset=0, s3size=None, bucket=None):
    """ Get S3 object identified by key and read as bytes
    """
    
    client = getS3Client(app)
    if not bucket:
        bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info(f"getS3Bytes(s3://{bucket}/{key})")
    s3_stats_increment(app, "get_count")
    range=""
    if s3size:
        range = f"bytes={s3offset}-{s3offset+s3size-1}"
        log.info(f"s3 range request: {range}")

    try:

        resp = await client.get_object(Bucket=bucket, Key=key, Range=range)
        data = await resp['Body'].read()
        resp['Body'].close()
    except ClientError as ce:
        # key does not exist?
        # check for not found status
        response_code = ce.response["Error"]["Code"]
        if response_code == "NoSuchKey":
            msg = f"s3_key: {key} not found "
            log.warn(msg)
            raise HTTPInternalServerError()
        elif response_code == "NoSuchBucket":
            msg = f"s3_bucket: {bucket} not fiound"
            log.info(msg)
            raise HTTPNotFound()
        else:
            s3_stats_increment(app, "error_count")
            log.error(f"got unexpected ClientError on s3 get {key}: {ce}")
            raise HTTPInternalServerError()

    if data and len(data) > 0:
        s3_stats_increment(app, "bytes_in", inc=len(data))
        log.info(f"read: {len(data)} bytes for S3 key: {key}")
        if deflate_level is not None:
            try:
                unzip_data = zlib.decompress(data)
                log.info(f"uncompressed to {len(unzip_data)} bytes")
                data = unzip_data
            except zlib.error as zlib_error:
                log.info(f"zlib_err: {zlib_error}")
                log.warn(f"unable to uncompress s3 obj: {key}")
        
    return data

async def putS3JSONObj(app, key, json_obj, bucket=None):
    """ Store JSON data as S3 object with given key
    """
   
    client = getS3Client(app)
    if not bucket:
        bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info(f"putS3JSONObj(s3://{bucket}/{key})")
    s3_stats_increment(app, "put_count")
    data = json.dumps(json_obj)
    data = data.encode('utf8')
    try:
        rsp = await client.put_object(Bucket=bucket, Key=key, Body=data)
        now = int(time.time())
        s3_rsp = {"etag": rsp["ETag"], "size": len(data), "lastModified": now}
    except ClientError as ce:
        s3_stats_increment(app, "error_count")
        msg = f"Error putting s3 obj {key}: {ce}"
        log.error(msg)
        raise HTTPInternalServerError()
    if data and len(data) > 0:
        s3_stats_increment(app, "bytes_out", inc=len(data))
    log.debug(f"putS3JSONObj {key} complete, s3_rsp: {s3_rsp}")
    return s3_rsp

async def putS3Bytes(app, key, data, deflate_level=None, bucket=None):
    """ Store byte string as S3 object with given key
    """
    
    client = getS3Client(app)
    if not bucket:
        bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info(f"putS3Bytes(s3://{bucket}/{key}), {len(data)} bytes")
    s3_stats_increment(app, "put_count")
    if deflate_level is not None:
        try:
            # the keyword parameter is enabled with py3.6
            # zip_data = zlib.compress(data, level=deflate_level)
            zip_data = zlib.compress(data, deflate_level)
            log.info(f"compressed from {len(data)} bytes to {len(zip_data)} bytes with level: {deflate_level}")
            data = zip_data
        except zlib.error as zlib_error:
            log.info(f"zlib_err: {zlib_error}")
            log.warn(f"unable to compress s3 obj: {key}, using raw bytes")
    
    try:
        rsp = await client.put_object(Bucket=bucket, Key=key, Body=data)
        now = int(time.time())
        s3_rsp = {"etag": rsp["ETag"], "size": len(data), "lastModified": now}
    except ClientError as ce:
        s3_stats_increment(app, "error_count")
        msg = f"ClientError putting s3 obj {key}: {ce}"
        log.error(msg)
        raise HTTPInternalServerError()
    except CancelledError as cle:
        s3_stats_increment(app, "error_count")
        msg = f"CancelledError putting s3 obj {key}: {cle}"
        log.error(msg)
        raise HTTPInternalServerError()
    except Exception as e:
        s3_stats_increment(app, "error_count")
        msg = f"Unexpected Exception {type(e)} putting s3 obj {key}: {e}"
        log.error(msg)
        raise HTTPInternalServerError()
    if data and len(data) > 0:
        s3_stats_increment(app, "bytes_in", inc=len(data))
    log.debug(f"putS3Bytes complete for s3 obj {key}, s3_rsp: {s3_rsp}")
    # s3 rsp format:
    # {'ETag': '"1b95a7bf5fab6f5c0620b8e3b30a53b9"', 'ResponseMetadata': 
    #     {'HostId': '', 'HTTPHeaders': {'X-Amz-Request-Id': '1529F570A809AD26', 'Server': 'Minio/RELEASE.2017-08-05T00-00-53Z (linux; amd64)', 'Vary': 'Origin', 'Date': 'Sun, 29 Apr 2018 16:36:53 GMT', 'Content-Length': '0', 'Content-Type': 'text/plain; charset=utf-8', 'Etag': '"1b95a7bf5fab6f5c0620b8e3b30a53b9"', 'X-Amz-Bucket-Region': 'us-east-1', 'Accept-Ranges': 'bytes'}, 
    #       'HTTPStatusCode': 200, 'RequestId': '1529F570A809AD26'}}
    return s3_rsp

async def deleteS3Obj(app, key, bucket=None):
    """ Delete S3 object identfied by given key
    """
    
    client = getS3Client(app)
    if not bucket:
        bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info("deleteS3Obj({})".format(key))
    s3_stats_increment(app, "delete_count")
    try:
        await client.delete_object(Bucket=bucket, Key=key)
    except ClientError as ce:
        # key does not exist? 
        key_found = await isS3Obj(app, key)
        if not key_found:
            log.warn(f"delete on s3key {key} but not found")
            raise HTTPNotFound()
        # else some other error
        s3_stats_increment(app, "error_count")
        msg = "Error deleting s3 obj: " + str(ce)
        log.error(msg)

        raise HTTPInternalServerError()
    log.debug("deleteS3Obj complete")

async def getS3ObjStats(app, key, bucket=None):
    """ Return etag, size, and last modified time for given object
    """
    
    client = getS3Client(app)
    if not bucket:
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
        raise HTTPInternalServerError()
    if 'Contents' not in resp:
        msg = "key: {} not found".format(key)
        log.info(msg)
        raise HTTPInternalServerError()
    contents = resp['Contents']
    log.debug("s3_contents: {}".format(contents))
    
    found = False
    if len(contents) > 0:
        item = contents[0]
        if item["Key"] == key:
            # if the key is a S3 folder, the key will be the first object in the folder,
            # not the requested object
            found = True
            if item["ETag"]:
                etag = item["ETag"]
                if len(etag) > 2 and etag[0] == '"' and etag[-1] == '"':
                    # S3 returning extra quotes around etag?
                    etag = etag[1:-1]
                    stats["ETag"] = etag
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
        raise HTTPNotFound()

    return stats
    
async def isS3Obj(app, key, bucket=None):
    """ Test if the given key maps to S3 object
    """
    found = False
    client = getS3Client(app)
    if not bucket:
        bucket = app['bucket_name']
    else:
        log.debug(f"using bucket: [{bucket}]")
    log.debug(f"isS3Obj s3://{bucket}/{key}") 
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
        log.debug("isS3Obj {} not found (no Contents)".format(key))
        return False
    contents = resp['Contents']
    if len(contents) > 0:
        item = contents[0]
        if item["Key"] == key:
            # if the key is a S3 folder, the key will be the first object in the folder,
            # not the requested object
            found = True
    log.debug("isS3Obj {} returning {}".format(key, found))
    return found

 
def getPageItems(response, items, include_stats=False):  
     
    log.info("getPageItems")
    
    if 'CommonPrefixes' in response:
        log.debug("got CommonPrefixes in s3 response")
        common = response["CommonPrefixes"]
        for item in common:
            if 'Prefix' in item:
                log.debug("got s3 prefix: {}".format(item['Prefix']))
                items.append(item["Prefix"])
                 
    elif 'Contents' in response:
        log.debug("got Contents in s3 response")
        contents = response['Contents']
        for item in contents:
            key_name = item['Key']
            if include_stats:
                stats = {}
                if item["ETag"]:
                    stats["ETag"] = item["ETag"]
                else:
                    log.warn("No ETag for key: {}".format(key_name))
                if "Size" in item:
                    stats["Size"] = item["Size"]
                else:
                    log.warn("No Size for key: {}".format(key_name))
                if "LastModified" in item:
                    stats["LastModified"] = int(item["LastModified"].timestamp())
                else:
                    log.warn("No LastModified for key: {}".format(key_name))
                log.debug("key: {} stats: {}".format(key_name, stats))
                items[key_name] = stats
            else:
                items.append(key_name)
               
# end getPageItems
    

async def getS3Keys(app, prefix='', deliminator='', suffix='', include_stats=False, callback=None, bucket=None, limit=None):
    # return keys matching the arguments
    s3_client = getS3Client(app)
    if not bucket:
        bucket = app['bucket_name']
    log.info(f"getS3Keys('{prefix}','{deliminator}','{suffix}', include_stats={include_stats}")
    paginator = s3_client.get_paginator('list_objects')
    if include_stats:
        # use a dictionary to hold return values
        key_names = {}
    else:
        # just use a list
        key_names = []
    count = 0

    try:
        async for page in paginator.paginate(
            PaginationConfig={'PageSize': 1000}, Bucket=bucket,  Prefix=prefix, Delimiter=deliminator):
            assert not asyncio.iscoroutine(page)
            #log.info(f"got page: {page}")
            getPageItems(page, key_names, include_stats=include_stats)
            count += len(key_names)
            if callback:
                if iscoroutinefunction(callback):
                    await callback(app, key_names)
                else:
                    callback(app, key_names)
                if include_stats:
                    key_names = {}
                else:
                    key_names = []
            if limit and count >= limit:
                log.info(f"getS3Keys - reached limit {limit}")
                break
    except ClientError as ce:
        log.warn(f"bucket: {bucket} does not exist, exception: {ce}")
        raise HTTPNotFound()
    except Exception as e:
        log.error(f"s3 paginate got exception {type(e)}: {e}")
        raise HTTPInternalServerError()

 
    log.info(f"getS3Keys done, got {len(key_names)} keys")
               
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