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
import json
import time
import zlib
import numpy as np
import numcodecs as codecs
import bitshuffle
from aiohttp.web_exceptions import HTTPInternalServerError

from .. import hsds_logger as log
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

BYTE_SHUFFLE = 1
BIT_SHUFFLE = 2


def getCompressors():
    """return available compressors"""
    compressors = codecs.blosc.list_compressors()
    # replace zlib with the equivalent gzip since that is the h5py name
    if "gzip" not in compressors and "zlib" in compressors:
        for i in range(len(compressors)):
            if compressors[i] == "zlib":
                compressors[i] = "gzip"
                break

    # add deflate as a synonym for gzip
    if "gzip" in compressors:
        compressors.append("deflate")

    return compressors


def getSupportedFilters(include_compressors=True):
    """return list of other supported filters"""
    filters = [
        "bitshuffle",
        "shuffle",
        "fletcher32",
        "nbit",        # No-op
        "scaleoffset"  # No-op
    ]
    if include_compressors:
        filters.extend(getCompressors())
    return filters


def setBloscThreads(nthreads):
    """Set the number of threads blosc will use for compression"""
    codecs.blosc.set_nthreads(nthreads)


def getBloscThreads():
    """Get the number of blosc threads to be used for compression"""
    nthreads = codecs.blosc.get_nthreads()
    return nthreads


def _shuffle(codec, data, chunk_shape=None, dtype=None):
    item_size = dtype.itemsize
    chunk_size = int(np.prod(chunk_shape)) * item_size
    block_size = None
    if codec == 1:
        # byte shuffle, use numcodecs Shuffle
        shuffler = codecs.Shuffle(item_size)
        arr = shuffler.encode(data)
        return arr.tobytes()
    elif codec == 2:
        # bit shuffle, use bitshuffle package
        # bitshufle is expecting numpy array
        # todo - enable block size to be set as part of the filter options
        block_size = config.get("bit_shuffle_default_blocksize", default=2048)

        data = np.frombuffer(data, dtype=dtype)
        data = data.reshape(chunk_shape)
        log.debug(f"bitshuffle.compress_lz4 - chunk_size: {chunk_size} block_size: {block_size}")
        arr = bitshuffle.compress_lz4(data, block_size)

    else:
        log.error(f"Unexpected codec: {codec} for _shuffle")
        raise ValueError()

    arr_bytes = arr.tobytes()
    if block_size:
        # prepend a 12 byte header with:
        #   uint64 value of chunk_size
        #   uint32 value of block_size

        # unfortunate we need to do a data copy here
        # don't see a way to preappend to the bytes we
        # get from numpy
        buffer = bytearray(len(arr_bytes) + 12)
        buffer[0:8] = int(chunk_size).to_bytes(8, "big")
        buffer[8:12] = int(block_size * item_size).to_bytes(4, "big")
        buffer[12:] = arr_bytes
        arr_bytes = bytes(buffer)

    return arr_bytes


def _unshuffle(codec, data, dtype=None, chunk_shape=None):
    item_size = dtype.itemsize
    chunk_size = int(np.prod(chunk_shape)) * item_size

    if codec == 1:
        # byte shuffle, use numcodecs Shuffle
        shuffler = codecs.Shuffle(item_size)
        arr = shuffler.decode(data)
    elif codec == 2:
        # bit shuffle, use bitshuffle
        # bitshufle is expecting numpy array
        data = np.frombuffer(data, dtype=np.dtype("uint8"))
        if len(data) < 12:
            # there should be at least 12 bytes for the header
            msg = f"got {len(data)} bytes for bitshuffle, "
            msg += f"expected {12 + len(chunk_size)} bytes"
            raise HTTPInternalServerError()

        # use lz4 uncompress with bitshuffle
        total_nbytes = int.from_bytes(data[:8], "big")
        block_nbytes = int.from_bytes(data[8:12], "big")
        if total_nbytes != chunk_size:
            msg = f"header reports total_bytes to be {total_nbytes} bytes,"
            msg += f"expected {chunk_size} bytes"
            log.error(msg)
            raise HTTPInternalServerError()

        # header has block size, so use that
        block_size = block_nbytes // dtype.itemsize
        msg = f"got bitshuffle header - total_nbytes: {total_nbytes}, "
        msg += f"block_nbytes: {block_nbytes}, block_size: {block_size}"
        log.debug(msg)
        data = data[12:]

        try:
            arr = bitshuffle.decompress_lz4(data, chunk_shape, dtype, block_size)
        except Exception as e:
            log.error(f"except using bitshuffle.decompress_lz4: {e}")
            raise HTTPInternalServerError()

    return arr.tobytes()


def _uncompress(data, compressor=None, shuffle=0, level=None, dtype=None, chunk_shape=None):
    """ Uncompress the provided data using compessor and/or shuffle """
    msg = f"_uncompress(compressor={compressor}, shuffle={shuffle})"
    if level is not None:
        msg += f", level: {level}"
    log.debug(msg)
    start_time = time.time()
    if compressor:
        if compressor in ("gzip", "deflate"):
            # blosc referes to this as zlib
            compressor = "zlib"
        # first check if this was compressed with blosc
        # returns typesize, isshuffle, and memcopied
        blosc_metainfo = codecs.blosc.cbuffer_metainfo(data)
        if blosc_metainfo[0] > 0:
            log.info(f"blosc compressed data for {len(data)} bytes")
            try:
                blosc = codecs.Blosc()
                udata = blosc.decode(data)
                log.info(f"blosc uncompressed to {len(udata)} bytes")
                data = udata
                if shuffle == BYTE_SHUFFLE:
                    shuffle = 0  # blosc will unshuffle the bytes for us
            except Exception as e:
                msg = f"got exception: {e} using blosc decompression"
                log.error(msg)
                raise HTTPInternalServerError()
        elif compressor == "zlib":
            # data may have been compressed without blosc,
            # try using zlib directly
            log.info(f"using zlib to decompress {len(data)} bytes")
            try:
                udata = zlib.decompress(data)
                log.info(f"uncompressed to {len(udata)} bytes")
                data = udata
            except zlib.error as zlib_error:
                log.info(f"zlib_err: {zlib_error}")
                log.error("unable to uncompress data with zlib")
                raise HTTPInternalServerError()
        else:
            msg = f"don't know how to decompress data in {compressor} "
            log.error(msg)
            raise HTTPInternalServerError()
    if shuffle:
        data = _unshuffle(shuffle, data, dtype=dtype, chunk_shape=chunk_shape)
    finish_time = time.time()
    elapsed = finish_time - start_time
    msg = f"uncompressed {len(data)} bytes, {(elapsed):.3f}s elapsed"
    log.debug(msg)

    return data


def _compress(data, compressor=None, level=5, shuffle=0, dtype=None, chunk_shape=None):
    if not compressor and shuffle != 2:
        # nothing to do
        return data
    log.debug(f"_compress(compressor={compressor}, shuffle={shuffle})")
    start_time = time.time()
    data_size = len(data)
    if shuffle == 2:
        # bit shuffle the data before applying the compressor
        log.debug("bitshuffling data")
        try:
            data = _shuffle(shuffle, data, dtype=dtype, chunk_shape=chunk_shape)
        except Exception as e:
            log.error(f"got exception using bitshuffle: {e}")
        shuffle = 0  # don't do any blosc shuffling

    if compressor:
        if compressor in ("gzip", "deflate"):
            # blosc referes to this as zlib
            compressor = "zlib"
        # try with blosc compressor
        try:
            blosc = codecs.Blosc(cname=compressor, clevel=level, shuffle=shuffle)
            cdata = blosc.encode(data)
            msg = f"compressed from {len(data)} bytes to {len(cdata)} bytes "
            msg += f"using filter: {blosc.cname} with level: {blosc.clevel}"
            log.info(msg)
        except Exception as e:
            log.error(f"got exception using blosc encoding: {e}")
    else:
        # no compressor, just pass back the shuffled data
        cdata = data

    if cdata is not None:
        finish_time = time.time()
        elapsed = finish_time - start_time
        ratio = data_size * 100.0 / len(cdata)
        msg = f"compressed {data_size} bytes to {len(cdata)} bytes, "
        msg += f"ratio: {ratio:.2f}%, {(elapsed):.3f}s elapsed"
        log.debug(msg)
        data = cdata  # use compressed data

    return data


def _getStorageClient(app):
    """get storage client s3 or azure blob"""

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
    """Return name of storage driver that is being used"""
    if config.get("aws_s3_gateway"):
        driver = "S3Client"
    elif config.get("azure_connection_string"):
        driver = "AzureBlobClient"
    else:
        driver = "FileClient"
    return driver


async def releaseStorageClient(app):
    """release the client storage connection
    (Used for cleanup on application exit)
    """
    client = _getStorageClient(app)
    await client.releaseClient()

    if "storage_client" in app:
        del app["storage_client"]


def _getURIParts(uri):
    """return tuple of (bucket, path) for given URI"""
    if uri.startswith("s3://"):
        uri = uri[5:]
    if uri.startswith("/"):
        raise ValueError("invalid uri")
    n = uri.find("/")
    if n < 0:
        raise ValueError("invalid uri")
    fields = (uri[:n], uri[n:])
    return fields


def getBucketFromStorURI(uri):
    """Return a bucket name given a storage URI
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
    """Return a key (path within a bucket) given a storage URI
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


def getURIFromKey(app, bucket=None, key=None):
    """ return URI for given bucket and key """
    client = _getStorageClient(app)
    if not bucket:
        bucket = app["bucket_name"]
    if key[0] == "/":
        key = key[1:]  # no leading slash
    uri = client.getURIFromKey(key, bucket=bucket)
    return uri


async def getStorJSONObj(app, key, bucket=None):
    """Get object identified by key and read as JSON"""

    client = _getStorageClient(app)
    if not bucket:
        bucket = app["bucket_name"]
    if key[0] == "/":
        key = key[1:]  # no leading slash
    log.info(f"getStorJSONObj({bucket})/{key}")

    data = await client.get_object(key, bucket=bucket)

    try:
        json_dict = json.loads(data.decode("utf8"))
    except UnicodeDecodeError:
        log.error(f"Error loading JSON at key: {key}")
        raise HTTPInternalServerError()

    msg = f"storage key {key} returned json object "
    msg += f"with {len(json_dict)} keys"
    log.debug(msg)
    return json_dict


async def getStorBytes(app,
                       key,
                       filter_ops=None,
                       offset=0,
                       length=-1,
                       chunk_locations=None,
                       chunk_bytes=None,
                       h5_size=None,
                       bucket=None,
                       ):
    """Get object identified by key and read as bytes"""

    client = _getStorageClient(app)
    if not bucket:
        bucket = app["bucket_name"]
    if key[0] == "/":
        key = key[1:]  # no leading slash
    if offset is None:
        offset = 0
    if length is None:
        length = 0
    msg = f"getStorBytes({bucket}/{key}, offset={offset}, length: {length})"
    log.info(msg)

    kwargs = {"bucket": bucket, "key": key, "offset": offset, "length": length}
    data = await client.get_object(**kwargs)
    if data is None or len(data) == 0:
        log.info(f"no data found for {key}")
        return data

    log.info(f"read: {len(data)} bytes for key: {key}")
    if length > 0 and len(data) != length:
        log.warn(f"requested {length} bytes but got {len(data)} bytes")

    if chunk_locations:
        log.debug(f"getStorBytes - got {len(chunk_locations)} chunk locations")
        # uncompress chunks within the fetched data and store to
        # chunk bytes
        if not h5_size:
            log.error("getStorBytes - h5_size not set")
            raise HTTPInternalServerError()
        if not chunk_bytes:
            log.error("getStorBytes - chunk_bytes not set")
            raise HTTPInternalServerError()
        if len(chunk_locations) * h5_size < len(chunk_bytes):
            log.error(f"getStorBytes - invalid chunk_bytes length: {len(chunk_bytes)}")

        for chunk_location in chunk_locations:
            log.debug(f"getStoreBytes - processing chunk_location: {chunk_location}")
            n = chunk_location.offset - offset
            if n < 0:
                log.warn(f"getStorBytes - unexpected offset for chunk_location: {chunk_location}")
                continue
            m = n + chunk_location.length
            log.debug(f"getStorBytes - extracting chunk from data[{n}:{m}]")
            h5_bytes = data[n:m]

            if filter_ops:
                h5_bytes = _uncompress(h5_bytes, **filter_ops)

            if len(h5_bytes) != h5_size:
                msg = f"expected chunk index: {chunk_location.index} to have size: "
                msg += f"{h5_size} but got: {len(h5_bytes)}"
                log.warning(msg)
                continue
            # slot into the hsds chunk
            hs_offset = chunk_location.index * h5_size
            if hs_offset + h5_size > len(chunk_bytes):
                msg = f"expected chunk index: {chunk_location.index} to have offset: "
                msg += f"less than {len(chunk_bytes) - h5_size} but got: {hs_offset}"
                log.warning(msg)
                continue
            chunk_bytes[hs_offset:(hs_offset + h5_size)] = h5_bytes
        # chunk_bytes got updated, so just return None
        return None
    elif filter_ops:
        # uncompress and return
        data = _uncompress(data, **filter_ops)
        return data
    else:
        return data


async def putStorBytes(app, key, data, filter_ops=None, bucket=None):
    """Store byte string as S3 object with given key"""

    client = _getStorageClient(app)
    if not bucket:
        bucket = app["bucket_name"]
    if key[0] == "/":
        key = key[1:]  # no leading slash

    log.info(f"putStorBytes({bucket}/{key}), {len(data)}")

    if filter_ops:
        data = _compress(data, **filter_ops)

    rsp = await client.put_object(key, data, bucket=bucket)

    return rsp


async def putStorJSONObj(app, key, json_obj, bucket=None):
    """Store JSON data as storage object with given key"""

    client = _getStorageClient(app)
    if not bucket:
        bucket = app["bucket_name"]
    if key[0] == "/":
        key = key[1:]  # no leading slash
    log.info(f"putS3JSONObj({bucket}/{key})")
    data = json.dumps(json_obj)
    data = data.encode("utf8")

    rsp = await client.put_object(key, data, bucket=bucket)

    return rsp


async def deleteStorObj(app, key, bucket=None):
    """Delete storage object identfied by given key"""

    client = _getStorageClient(app)
    if not bucket:
        bucket = app["bucket_name"]
    if key[0] == "/":
        key = key[1:]  # no leading slash
    log.info(f"deleteStorObj({key})")

    await client.delete_object(key, bucket=bucket)

    log.debug("deleteStorObj complete")


async def getStorObjStats(app, key, bucket=None):
    """Return etag, size, and last modified time for given object"""
    # TBD - will need to be refactored to handle azure responses

    client = _getStorageClient(app)
    if not bucket:
        bucket = app["bucket_name"]
    stats = {}

    if key[0] == "/":
        # key = key[1:]  # no leading slash
        msg = f"key with leading slash: {key}"
        log.error(msg)
        raise KeyError(msg)

    log.info(f"getStorObjStats({key}, bucket={bucket})")

    stats = await client.get_key_stats(key, bucket=bucket)

    return stats


async def isStorObj(app, key, bucket=None):
    """Test if the given key maps to S3 object"""
    found = False
    client = _getStorageClient(app)
    if not bucket:
        bucket = app["bucket_name"]
    else:
        log.debug(f"using bucket: [{bucket}]")
    log.debug(f"isStorObj {bucket}/{key}")

    found = await client.is_object(bucket=bucket, key=key)

    log.debug(f"isStorObj {key} returning {found}")
    return found


async def getStorKeys(
    app,
    prefix="",
    deliminator="",
    suffix="",
    include_stats=False,
    callback=None,
    bucket=None,
    limit=None,
):
    # return keys matching the arguments
    client = _getStorageClient(app)
    if not bucket:
        bucket = app["bucket_name"]
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

    msg = f"getStorKeys done for prefix: {prefix}"
    if not callback:
        msg += f", got {len(key_names)} keys"
    log.info(msg)

    return key_names
