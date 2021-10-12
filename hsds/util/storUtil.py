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
# storage access functions.
# Abstracts S3 API vs Azure vs Posix storage access
#
import time
import json
import zlib
import numcodecs as codecs
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError
from aiohttp.client_exceptions import ClientError
from asyncio import CancelledError


from .. import hsds_logger as log
from .httpUtil import http_get
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


def getCompressors():
    """ return available compressors """
    compressors = codecs.blosc.list_compressors()
    # replace zlib with the equivalent gzip since that is the h5py name
    if "gzip" not in compressors and "zlib" in compressors:
        for i in range(len(compressors)):
            if compressors[i] == "zlib":
                compressors[i] = "gzip"
                break

    return compressors


def setBloscThreads(nthreads):
    """ Set the number of threads blosc will use for compression """
    codecs.blosc.set_nthreads(nthreads)


def getBloscThreads():
    """ Get the number of blosc threads to be used for compression """
    nthreads = codecs.blosc_get_nthreads()

    return nthreads


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
    # save client so we don't neeed to recreate each time
    app["storage_client"] = client
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

def _getURIParts(uri):
    """ return tuple of (bucket, path) for given URI """
    if uri.startswith("s3://"):
        uri = uri[5:]
    if uri.startswith('/'):
        raise ValueError("invalid uri")
    n = uri.find('/')
    if n < 0:
        raise ValueError("invalid uri")
    fields = (uri[:n], uri[n:])
    return fields

def getBucketFromStorURI(uri):
    """ Return a bucket name given a storage URI 
        Examples:
          s3://mybucket/folder/object.json  -> mybucket
          mybucket/folder/object.json  -> mybucket
          mybucket -> ValueError  # no slash
          /mybucket/folder/object.json -> ValueError # not expecting abs path
    """
    fields = _getURIParts(uri)
    bucket = fields[0]
    if not bucket:
        raise ValueError("invalid uri")
    return bucket

def getKeyFromStorURI(uri):
    """ Return a key (path within a bucket) given a storage URI 
        Examples:
          s3://mybucket/folder/object.json  -> mybucket
          mybucket/folder/object.json  -> mybucket
          mybucket -> ValueError  # no slash
          /mybucket/folder/object.json -> ValueError # not expecting abs path
    """
    fields = _getURIParts(uri)
    path = fields[1]
    if not path:
        raise ValueError("invalid uri")
    return path


async def rangegetProxy(app, bucket=None, key=None, offset=0, length=0):
    """ fetch bytes from rangeget proxy
    """
    if "rangeget_url" in app:
        req = app["rangeget_url"] + '/'
    else:
        rangeget_port = config.get("rangeget_port")
        if "is_docker" in app:
            rangeget_host = "rangeget"
        else:
            rangeget_host = "127.0.0.1"
        req = f"http://{rangeget_host}:{rangeget_port}/"
    log.debug(f"rangeGetProxy: {req}")
    params = {}
    params["bucket"] = bucket
    params["key"] = key
    params["offset"] = offset
    params["length"] = length

    try:
        data = await http_get(app, req, params=params)
        log.debug(f"rangeget request: {req}, read {len(data)} bytes")
    except HTTPNotFound:
        # external HDF5 file, should exist
        log.warn(f"range get request not found for params: {params}")
        raise
    except ClientError as ce:
        log.error(f"Error for rangeget({req}): {ce} ")
        raise HTTPInternalServerError()
    except CancelledError as cle:
        log.warn(f"CancelledError for rangeget request({req}): {cle}")
        return None

    if len(data) != length:
        msg = f"expected {length} bytes for rangeget {bucket}{key}, "
        msg += f"but got: {len(data)}"
        log.warn(msg)
    return data
    


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

    msg = f"storage key {key} returned json object "
    msg += f"with {len(json_dict)} keys"
    log.debug(msg)
    return json_dict


async def getStorBytes(app, key, filter_ops=None, offset=0,
                       length=-1, bucket=None, use_proxy=False):
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
    msg = f"getStorBytes({bucket}/{key}, offset={offset}, length: {length})"
    log.info(msg)

    default_req_size = 128 * 1024 * 1024  # 128KB
    data_cache_max_req_size = int(config.get("data_cache_max_req_size", default=default_req_size))

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

    kwargs = {"bucket": bucket, "key": key,
              "offset": offset, "length": length}
    if offset > 0 and use_proxy and length < data_cache_max_req_size:
        # use rangeget proxy
        data = await rangegetProxy(app, **kwargs)
    else:
        data = await client.get_object(**kwargs)
    if data is None or len(data) == 0:
        log.info(f"no data found for {key}")
        return data

    log.info(f"read: {len(data)} bytes for key: {key}")
    if length > 0 and len(data) != length:
        log.warn(f"requested {length} bytes but got {len(data)} bytes")
    if compressor:

        # compressed chunk data...

        # first check if this was compressed with blosc
        # returns typesize, isshuffle, and memcopied
        blosc_metainfo = codecs.blosc.cbuffer_metainfo(data)
        if blosc_metainfo[0] > 0:
            log.info(f"blosc compressed data for {key}")
            try:
                blosc = codecs.Blosc()
                udata = blosc.decode(data)
                log.info(f"uncompressed to {len(udata)} bytes")
                data = udata
                shuffle = 0  # blosc will unshuffle the bytes for us
            except Exception as e:
                msg = f"got exception: {e} using blosc decompression for {key}"
                log.error(msg)
                raise HTTPInternalServerError()
        elif compressor == "zlib":
            # data may have been compressed without blosc,
            # try using zlib directly
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
            msg = f"don't know how to decompress data in {compressor} "
            msg += f"format for {key}"
            log.error(msg)
            raise HTTPInternalServerError()

    if shuffle > 0:
        log.debug(f"shuffle is {shuffle}")
        start_time = time.time()
        unshuffled = _unshuffle(shuffle, data)
        if unshuffled is not None:
            log.debug(f"unshuffled to {len(unshuffled)} bytes")
            data = unshuffled
        finish_time = time.time()
        elapsed = finish_time - start_time
        msg = f"unshuffled {len(data)} bytes, {(elapsed):.2f} elapsed"
        log.debug(msg)

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
    msg = f"putStorBytes({bucket}/{key}), {len(data)} bytes shuffle: {shuffle}"
    msg += f" compressor: {cname} level: {clevel}"
    log.info(msg)

    if cname:
        try:
            blosc = codecs.Blosc(cname=cname, clevel=clevel, shuffle=shuffle)
            cdata = blosc.encode(data)
            # TBD: add cname in blosc constructor
            msg = f"compressed from {len(data)} bytes to {len(cdata)} bytes "
            msg += f"using filter: {blosc.cname} with level: {blosc.clevel}"
            log.info(msg)
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


async def getStorKeys(app, prefix='', deliminator='', suffix='',
                      include_stats=False, callback=None, bucket=None,
                      limit=None):
    # return keys matching the arguments
    client = _getStorageClient(app)
    if not bucket:
        bucket = app['bucket_name']
    msg = f"getStorKeys('{prefix}','{deliminator}','{suffix}', "
    msg += f"include_stats={include_stats}"
    log.info(msg)
    kwargs = {}
    kwargs["prefix"] = prefix
    kwargs["deliminator"] = deliminator
    kwargs["suffix"] = suffix
    kwargs["include_stats"] = include_stats
    kwargs["callback"] = callback
    kwargs["bucket"] = bucket
    kwargs["limit"] = limit

    key_names = await client.list_keys(**kwargs)

    log.info(f"getStorKeys done, got {len(key_names)} keys")

    return key_names
