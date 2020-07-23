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
# storUtil:
# storage access functions.  Abstracts S3 API vs Azure storage access
#
import json
import zlib
import numpy as np
from numba import jit
from aiohttp.web_exceptions import HTTPInternalServerError


from .. import hsds_logger as log
from .s3Client import S3Client
try:
    from .azureBlobClient import AzureBlobClient
except ImportError:
    def AzureBlobClient(app):
        return None
try:
    from .fileClient import FileClient
except ImportError:
    def FileClient(app):
        log.error("ImportError for FileClient")
        return None
from .. import config

@jit(nopython=True)
def _doShuffle(src, des, element_size):
    count = len(src) // element_size
    for i in range(count):
        offset = i*element_size
        e = src[offset:(offset+element_size)]
        for byte_index in range(element_size):
            j = byte_index*count + i
            des[j] = e[byte_index]
    return des

@jit(nopython=True)
def _doUnshuffle(src, des, element_size):
    count = len(src) // element_size
    for i in range(element_size):
        offset = i*count
        e = src[offset:(offset+count)]
        for byte_index in range(count):
            j = byte_index*element_size + i
            des[j] = e[byte_index]
    return des

def _shuffle(element_size, chunk):
    if element_size <= 1:
        return  None # no shuffling needed
    chunk_size = len(chunk)
    if chunk_size % element_size != 0:
        raise ValueError("unexpected chunk size")

    arr = np.zeros((chunk_size,), dtype='u1')
    _doShuffle(chunk, arr, element_size)

    return arr.tobytes()

def _unshuffle(element_size, chunk):
    if element_size <= 1:
        return  None # no shuffling needed
    chunk_size = len(chunk)
    if chunk_size % element_size != 0:
        raise ValueError("unexpected chunk size")
    arr = np.zeros((chunk_size,), dtype='u1')
    _doUnshuffle(chunk, arr, element_size)

    return arr.tobytes()

def _getStorageClient(app):
    """ get storage client s3 or azure blob
    """

    if config.get("aws_s3_gateway"):
        log.debug("_getStorageClient getting S3Client")
        client = S3Client(app)
    elif config.get("azure_connection_string"):
        log.debug("_getStorageClient getting AzureBlobClient")
        client = AzureBlobClient(app)
    else:
        log.debug("_getStorageClient getting FileClient")
        client = FileClient(app)
    return client

def getStorageDriverName(app):
    """ Return name of storage driver that is being used
    """
    if config.get("aws_s3_gateway"):
        driver = "S3Client"
    elif config.get("azure_connection_string"):
        driver = "AzureBlobClient"
    else:
        driver = "FileClient"
    return driver

async def releaseStorageClient(app):
    """ release the client storage connection
     (Used for cleanup on application exit)
    """
    client = _getStorageClient(app)
    await client.releaseClient()

async def getStorJSONObj(app, key, bucket=None):
    """ Get object identified by key and read as JSON
    """

    client = _getStorageClient(app)
    if not bucket:
        bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info(f"getStorJSONObj({bucket})/{key}")

    data = await client.get_object(key, bucket=bucket)

    try:
        json_dict = json.loads(data.decode('utf8'))
    except UnicodeDecodeError:
        log.error(f"Error loading JSON at key: {key}")
        raise HTTPInternalServerError()

    log.debug(f"storage key {key} returned: {json_dict}")
    return json_dict

async def getStorBytes(app, key, shuffle=0, deflate_level=None, offset=0, length=None, bucket=None):
    """ Get object identified by key and read as bytes
    """

    client = _getStorageClient(app)
    if not bucket:
        bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info(f"getStorBytes({bucket}/{key})")

    data = await client.get_object(bucket=bucket, key=key, offset=offset, length=length)

    if data and len(data) > 0:
        log.info(f"read: {len(data)} bytes for key: {key}")
        if deflate_level is not None:
            try:
                unzip_data = zlib.decompress(data)
                log.info(f"uncompressed to {len(unzip_data)} bytes")
                data = unzip_data
            except zlib.error as zlib_error:
                log.info(f"zlib_err: {zlib_error}")
                log.warn(f"unable to uncompress obj: {key}")
        if shuffle > 0:
            log.debug(f"shuffle is {shuffle}")
            unshuffled = _unshuffle(shuffle, data)
            if unshuffled is not None:
                log.debug(f"unshuffled to {len(unshuffled)} bytes")
                data = unshuffled

    return data

async def putStorJSONObj(app, key, json_obj, bucket=None):
    """ Store JSON data as storage object with given key
    """

    client = _getStorageClient(app)
    if not bucket:
        bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info(f"putS3JSONObj({bucket}/{key})")
    data = json.dumps(json_obj)
    data = data.encode('utf8')

    rsp = await client.put_object(key, data, bucket=bucket)

    return rsp

async def putStorBytes(app, key, data, shuffle=0, deflate_level=None, bucket=None):
    """ Store byte string as S3 object with given key
    """

    client = _getStorageClient(app)
    if not bucket:
        bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info(f"putStorBytes({bucket}/{key}), {len(data)} bytes shuffle: {shuffle} deflate: {deflate_level}")
    if shuffle > 0:
        shuffled_data = _shuffle(shuffle, data)
        if shuffled_data is not None:
            log.info(f"shuffled data to {len(shuffled_data)}")
            data = shuffled_data

    if deflate_level is not None:
        try:
            # the keyword parameter is enabled with py3.6
            # zip_data = zlib.compress(data, level=deflate_level)
            zip_data = zlib.compress(data, deflate_level)
            log.info(f"compressed from {len(data)} bytes to {len(zip_data)} bytes with level: {deflate_level}")
            data = zip_data
        except zlib.error as zlib_error:
            log.info(f"zlib_err: {zlib_error}")
            log.warn(f"unable to compress obj: {key}, using raw bytes")

    rsp = await client.put_object(key, data, bucket=bucket)

    return rsp

async def deleteStorObj(app, key, bucket=None):
    """ Delete storage object identfied by given key
    """

    client = _getStorageClient(app)
    if not bucket:
        bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    log.info(f"deleteStorObj({key})")

    await client.delete_object(key, bucket=bucket)

    log.debug("deleteStorObj complete")

async def getStorObjStats(app, key, bucket=None):
    """ Return etag, size, and last modified time for given object
    """
    # TBD - will need to be refactored to handle azure responses

    client = _getStorageClient(app)
    if not bucket:
        bucket = app['bucket_name']
    stats = {}

    if key[0] == '/':
        #key = key[1:]  # no leading slash
        msg = f"key with leading slash: {key}"
        log.error(msg)
        raise KeyError(msg)

    log.info(f"getStorObjStats({key}, bucket={bucket})")

    stats = await client.get_key_stats(key, bucket=bucket)

    return stats

async def isStorObj(app, key, bucket=None):
    """ Test if the given key maps to S3 object
    """
    found = False
    client = _getStorageClient(app)
    if not bucket:
        bucket = app['bucket_name']
    else:
        log.debug(f"using bucket: [{bucket}]")
    log.debug(f"isStorObj {bucket}/{key}")

    found = await client.is_object(bucket=bucket, key=key)
    
    log.debug(f"isStorObj {key} returning {found}")
    return found

async def getStorKeys(app, prefix='', deliminator='', suffix='', include_stats=False, callback=None, bucket=None, limit=None):
    # return keys matching the arguments
    client = _getStorageClient(app)
    if not bucket:
        bucket = app['bucket_name']
    log.info(f"getStorKeys('{prefix}','{deliminator}','{suffix}', include_stats={include_stats}")

    key_names = await client.list_keys(prefix=prefix, deliminator=deliminator, suffix=suffix,
        include_stats=include_stats, callback=callback, bucket=bucket, limit=limit)

    log.info(f"getStorKeys done, got {len(key_names)} keys")

    return key_names
