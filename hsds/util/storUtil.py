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
import time
import json
import zlib
import numcodecs as codecs
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError
from aiohttp.client_exceptions import ClientError
from asyncio import CancelledError


from .. import hsds_logger as log
from .httpUtil import get_http_client
from .s3Client import S3Client
try:
    from .azureBlobClient import AzureBlobClient
except ImportError:
    def AzureBlobClient(app):
        log.error("Unable to import AzureBlobClient")
        return None
try:
    from .fileClient import FileClient
except ImportError:
    def FileClient(app):
        log.error("ImportError for FileClient")
        return None
from .. import config


def _shuffle(element_size, chunk):
    shuffler = codecs.Shuffle(element_size)
    arr = shuffler.encode(chunk)
    return arr.tobytes()


def _unshuffle(element_size, chunk):
    shuffler = codecs.Shuffle(element_size)
    arr = shuffler.decode(chunk)
    return arr.tobytes()


def _getStorageClient(app):
    """ get storage client s3 or azure blob
    """

    if "storage_client" in app:
        return app["storage_client"]

    if config.get("aws_s3_gateway"):
        log.debug("_getStorageClient getting S3Client")
        client = S3Client(app)
    elif config.get("azure_connection_string"):
        log.debug("_getStorageClient getting AzureBlobClient")
        client = AzureBlobClient(app)
    else:
        log.debug("_getStorageClient getting FileClient")
        client = FileClient(app)
    app["storage_client"] = client  # save so we don't neeed to recreate each time
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

    if "storage_client" in app:
        del app["storage_client"]


async def rangegetProxy(app, bucket=None, key=None, offset=0, length=0):
    """ fetch bytes from rangeget proxy
    """
    rangeget_port = config.get("rangeget_port")
    if "is_docker" in app:
        rangeget_host = "rangeget"
    else:
        rangeget_host = "127.0.0.1"
    req = f"http://{rangeget_host}:{rangeget_port}/"
    client = get_http_client(app)
    log.debug(f"rangeGetProxy: {req}")
    params = {}
    params["bucket"] = bucket
    params["key"] = key
    params["offset"] = offset
    params["length"] = length
    try:
        rsp = await client.get(req, params=params)
    except ClientError as ce:
        log.error(f"Error for http_get({req}): {ce} ")
        raise HTTPInternalServerError()
    except CancelledError as cle:
        log.warn(f"CancelledError for http_get({req}): {cle}")
        return None

    log.debug(f"http_get {req} status: <{rsp.status}>")
    if rsp.status == 200:
        data = await rsp.read()  # read response as bytes
        if not data:
            log.warn(f"rangeget for: {bucket}{key} no data returned")
            raise HTTPNotFound()
        if len(data) != length:
            log.warn(
                f"expected {length} bytes for rangeget {bucket}{key}, but got: {len(data)}")
        return data

    elif rsp.status == 404:
        log.warn(f"rangeget for: {bucket}{key} not found")
        raise HTTPNotFound()
    else:
        msg = f"request to {req} failed with code: {rsp.status}"
        log.error(msg)
        raise HTTPInternalServerError()


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

    log.debug(f"storage key {key} returned json object with {len(json_dict)} keys")
    return json_dict


async def getStorBytes(app, key, filter_ops=None, offset=0, length=-1, bucket=None, use_proxy=False):
    """ Get object identified by key and read as bytes
    """

    client = _getStorageClient(app)
    if not bucket:
        bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    if offset is None:
        offset = 0
    if length is None:
        length = 0
    log.info(f"getStorBytes({bucket}/{key}, offset={offset}, length: {length})")

    data_cache_page_size = int(config.get("data_cache_page_size"))

    shuffle = 0
    compressor = None
    if filter_ops:
        log.debug(f"getStorBytes for {key} with filter_ops: {filter_ops}")
        if "use_shuffle" in filter_ops and filter_ops['use_shuffle']:
            shuffle = filter_ops['item_size']
            log.debug("using shuffle filter")
        if "compressor" in filter_ops:
            compressor = filter_ops["compressor"]
            log.debug(f"using compressor: {compressor}")

    if offset > 0 and use_proxy and length < data_cache_page_size:
        # use rangeget proxy
        data = await rangegetProxy(app, bucket=bucket, key=key, offset=offset, length=length)
    else:
        data = await client.get_object(bucket=bucket, key=key, offset=offset, length=length)
    if data is None or len(data) == 0:
        log.info(f"no data found for {key}")
        return data

    log.info(f"read: {len(data)} bytes for key: {key}")
    if length > 0 and len(data) != length:
        log.warn(f"requested {length} bytes but got {len(data)} bytes")
    if compressor:

        # compressed chunk data...

        # first check if this was compressed with blosc
        blosc_metainfo = codecs.blosc.cbuffer_metainfo(data)  # returns typesize, isshuffle, and memcopied
        if blosc_metainfo[0] > 0:
            log.info(f"blosc compressed data for {key}")
            try:
                blosc = codecs.Blosc()
                udata = blosc.decode(data)
                log.info(f"uncompressed to {len(udata)} bytes")
                data = udata
                shuffle = 0  # blosc will unshuffle the bytes for us
            except Exception as e:
                log.error(f"got exception: {e} using blosc decompression for {key}")
                raise HTTPInternalServerError()
        elif compressor == "zlib":
            # data may have been compressed without blosc, try using zlib directly
            log.info(f"using zlib to decompress {key}")
            try:
                udata = zlib.decompress(data)
                log.info(f"uncompressed to {len(udata)} bytes")
                data = udata
            except zlib.error as zlib_error:
                log.info(f"zlib_err: {zlib_error}")
                log.error(f"unable to uncompress obj: {key}")
                raise HTTPInternalServerError()
        else:
            log.error(f"don't know how to decompress data in {compressor} format for {key}")
            raise HTTPInternalServerError()

    if shuffle > 0:
        log.debug(f"shuffle is {shuffle}")
        start_time = time.time()
        unshuffled = _unshuffle(shuffle, data)
        if unshuffled is not None:
            log.debug(f"unshuffled to {len(unshuffled)} bytes")
            data = unshuffled
        finish_time = time.time()
        log.debug(f"unshuffled {len(data)} bytes, {(finish_time - start_time):.2f} elapsed")

    return data


async def putStorBytes(app, key, data, filter_ops=None, bucket=None):
    """ Store byte string as S3 object with given key
    """

    client = _getStorageClient(app)
    if not bucket:
        bucket = app['bucket_name']
    if key[0] == '/':
        key = key[1:]  # no leading slash
    shuffle = -1  # auto-shuffle
    clevel = 5
    cname = None  # compressor name
    if filter_ops:
        if "compressor" in filter_ops:
            cname = filter_ops["compressor"]
        if "use_shuffle" in filter_ops and not filter_ops['use_shuffle']:
            shuffle = 0  # client indicates to turn off shuffling
        if "level" in filter_ops:
            clevel = filter_ops["level"]
    log.info(f"putStorBytes({bucket}/{key}), {len(data)} bytes shuffle: {shuffle} compressor: {cname} level: {clevel}")

    if cname:
        try:
            blosc = codecs.Blosc(cname=cname, clevel=clevel, shuffle=shuffle)
            cdata = blosc.encode(data)
            # TBD: add cname in blosc constructor
            log.info(f"compressed from {len(data)} bytes to {len(cdata)} bytes using filter: {blosc.cname} with level: {blosc.clevel}")
            data = cdata
        except Exception as e:
            log.error(f"got exception using blosc encoding: {e}")
            raise HTTPInternalServerError()

    rsp = await client.put_object(key, data, bucket=bucket)

    return rsp


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
        # key = key[1:]  # no leading slash
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
                                       include_stats=include_stats, callback=callback,
                                       bucket=bucket, limit=limit)

    log.info(f"getStorKeys done, got {len(key_names)} keys")

    return key_names
