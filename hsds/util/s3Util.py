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
import json
import zlib
import subprocess
import datetime
from botocore.exceptions import ClientError
from aiohttp.errors import HttpProcessingError 

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
    aws_secret_access_key = None
    aws_access_key_id = None 
    aws_session_token = None
    aws_iam_role = config.get("aws_iam_role")
    log.info("using iam role: {}".format(aws_iam_role))
    aws_secret_access_key = config.get("aws_secret_access_key")
    aws_access_key_id = config.get("aws_access_key_id")
    if not aws_secret_access_key or aws_secret_access_key == 'xxx':
        log.info("aws secret access key not set")
        aws_secret_access_key = None
    if not aws_access_key_id or aws_access_key_id == 'xxx':
        log.info("aws access key id not set")
        aws_access_key_id = None
  
    if aws_iam_role and not aws_secret_access_key:
        log.info("getted EC2 IAM role credentials")
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
    use_ssl = False
    if s3_gateway.startswith("https"):
        use_ssl = True
    s3 = session.create_client('s3', region_name=aws_region,
                                   aws_secret_access_key=aws_secret_access_key,
                                   aws_access_key_id=aws_access_key_id,
                                   aws_session_token=aws_session_token,
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
    try:
        json_dict = json.loads(data.decode('utf8'))
    except UnicodeDecodeError:
        s3_stats_increment(app, "error_count")
        log.error("Error loading JSON at key: {}".format(key))
        msg = "Unexpected i/o error"
        raise HttpProcessingError(code=500, message=msg)

    log.debug("s3 returned: {}".format(json_dict))
    return json_dict

async def getS3Bytes(app, key, deflate_level=None):
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
        log.info("read: {} bytes for S3 key: {}".format(len(data), key))
        if deflate_level is not None:
            try:
                unzip_data = zlib.decompress(data)
                log.info("uncompressed to {} bytes".format(len(unzip_data)))
                data = unzip_data
            except zlib.error as zlib_error:
                log.info("zlib_err: {}".format(zlib_error))
                log.warn("unable to uncompress s3 obj: {}, returning raw bytes".format(key))
        
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
    log.debug("putS3JSONObj complete")

async def putS3Bytes(app, key, data, deflate_level=None):
    """ Store byte string as S3 object with given key
    """
    
    client = getS3Client(app)
    bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info("putS3Bytes({}), {} bytes".format(key, len(data)))
    s3_stats_increment(app, "put_count")
    if deflate_level is not None:
        try:
            # the keyword parameter is enabled with py3.6
            # zip_data = zlib.compress(data, level=deflate_level)
            zip_data = zlib.compress(data, deflate_level)
            log.info("compressed from {} bytes to {} bytes with level: {}".format(len(data), len(zip_data), deflate_level))
            data = zip_data
        except zlib.error as zlib_error:
            log.info("zlib_err: {}".format(zlib_error))
            log.warn("unable to compress s3 obj: {}, using raw bytes".format(key))
    
    try:
        await client.put_object(Bucket=bucket, Key=key, Body=data)
    except ClientError as ce:
        s3_stats_increment(app, "error_count")
        msg = "Error putting s3 obj: " + str(ce)
        log.error(msg)
        raise HttpProcessingError(code=500, message=msg)
    if data and len(data) > 0:
        s3_stats_increment(app, "bytes_in", inc=len(data))
    log.debug("putS3Bytes complete")

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
    log.debug("deleteS3Obj complete")

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
    log.debug("isS3Obj {} returning {}".format(key, found))
    return found

        
     
"""
Helper function for getS3Keys
"""
async def _fetch_all(app, pages, key_names, prefix='', deliminator='', suffix='', include_stats=False, callback=None):
    count = 0
    retry_limit = 3
    while True:
        for retry_number in range(retry_limit):
            try: 
                response = await pages.next_page()
                break  # success
            except AttributeError as ae:
                # aiohttp if throwing an attribute error when there  is a problem
                # with the S3 connection
                # back off and try again
                if retry_number == retry_limit - 1:
                    log.error("Error retreiving s3 keys")
                    raise HttpProcessingError(code=500, message="Unexpected Error retreiving S3 keys")
                log.warn("Error retrieving S3 keys, retrying")
                sleep_seconds = (retry_number+1)**2  # sleep, 1,4,9, etc. seconds
                await asyncio.sleep(sleep_seconds)
        if response is None:
            break
        last_key = None
        if 'CommonPrefixes' in response:
            log.debug("got CommonPrefixes in s3 response")
            common = response["CommonPrefixes"]
            for item in common:
                if 'Prefix' in item:
                    log.debug("got s3 prefix: {}".format(item['Prefix']))
                    if include_stats:
                        # TBD: not sure what makes sense to include for stats here
                        key_names[item['Prefix']] = {}
                    else:
                        key_names.append(item['Prefix'])
                    count += 1

        elif 'Contents' in response:
            log.debug("got Contents in s3 response")
            contents = response['Contents']
            for item in contents:
                key_name = item['Key']
                log.debug("got s3key: {}".format(key_name))
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
                        key_name = key_name[:-1]  # don't show ending deliminator
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
                    #
                    # key + stats format:
                    #
                    # key   38 + chunk_index                       | chunk|etag - 32 hex -> 16 bytes               | lm - 4 bytes  | size - 4 bytes
                    # c-287d0b70-29e0-11e7-ab6f-0242ac110007_17_2_4 860e189d78013404a3940900b956892c1493142971 1233936
                    # 
                    # ~ 146 bytes in Python
                    # ~ 45 bytes in C
                    
                    stats = {}
                    if item["ETag"]:
                        stats["ETag"] = item["ETag"]
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
                    log.debug("key: {} stats: {}".format(key_name, stats))
                    key_names[key_name] = stats
                    count += 1
                else:
                    key_names.append(key_name)
                    count += 1
        # done with all items in this response
        if callback is not None and len(key_names) > 0:
            callback(app, key_names)
            # reset key_names
            log.debug("reset key_names")
            key_names.clear()
             
             
    # end while True
    return count
# end _fetch_all
        
    

async def getS3Keys(app, prefix='', deliminator='', suffix='', include_stats=False, callback=None):
    # return keys matching the arguments
    s3_client = getS3Client(app)
    bucket_name = app['bucket_name']
    log.info("getS3Keys('{}','{}','{}')".format(prefix, deliminator, suffix))
    paginator = s3_client.get_paginator('list_objects')
    if include_stats:
        # use a dictionary to hold return values
        key_names = {}
    else:
        # just use a list
        key_names = []
     
    # TBD - for some reason passing in non-null deliminator doesn't work
    pages = paginator.paginate(MaxKeys=1000, Bucket=bucket_name, Prefix=prefix, Delimiter=deliminator)
    # fetch all will fill in key_names unless callback is provided
    count = await _fetch_all(app, pages, key_names, prefix=prefix, deliminator=deliminator, suffix=suffix, include_stats=include_stats, callback=callback)

    log.info("getS3Keys done, got {} keys".format(count))
               
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