import asyncio
import hashlib
import os.path as pp
from os import mkdir, rmdir, listdir, stat, remove, walk
from asyncio import CancelledError
from  inspect import iscoroutinefunction
import time
import aiofiles
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError, HTTPBadRequest
from .. import hsds_logger as log
from .. import config

class FileClient():
    """
     Utility class for reading and storing data to local files using aiofiles package
    """

    def __init__(self, app):

        self._app = app
        self._root_dir = config.get("root_dir")
        if not self._root_dir:
            log.error("FileClient init: root_dir config not set")
            raise HTTPInternalServerError()
        if not pp.isdir(self._root_dir):
            log.error("FileClient init: root folder does not exist")
            raise HTTPInternalServerError()
        if not pp.isabs(self._root_dir):
            log.error("FileClient init: root dir most have absolute path")
            raise HTTPInternalServerError()

    def _validateBucket(self, bucket):
        if not bucket or pp.isabs(bucket) or pp.dirname(bucket):
            msg = "invalid bucket name"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    def _validateKey(self, key):
        if not key or pp.isabs(key):
            msg = "invalid key name"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)


    def _getFilePath(self, bucket, key=''):
        filepath = pp.join(self._root_dir, bucket, key)
        return pp.normpath(filepath)

    def _getFileStats(self, filepath, data=None):
        log.debug(f"_getFileStats({filepath})")
        if data is not None:
            if not isinstance(data, bytes):
                log.warn("_getFileStats - expected data to be bytes, not computing ETag")
                ETag = ""
            else:
                hash_object = hashlib.md5(data)
                ETag =  hash_object.hexdigest()
        else:
            log.debug("getFileStats - data is None, so ETag will not be computed")
            ETag = ""
        try:
            file_stats = stat(filepath)
            key_stats = {"ETag": ETag, "Size": file_stats.st_size, "LastModified": file_stats.st_mtime}
            log.info(f"_getFileStats({filepath}) returning: {key_stats}")
        except FileNotFoundError:
            raise HTTPNotFound()
        return key_stats


    def _file_stats_increment(self, counter, inc=1):
        """ Incremenet the indicated connter
        """
        if "file_stats" not in self._app:
            # setup stats
            file_stats = {}
            file_stats["get_count"] = 0
            file_stats["put_count"] = 0
            file_stats["delete_count"] = 0
            file_stats["list_count"] = 0
            file_stats["error_count"] = 0
            file_stats["bytes_in"] = 0
            file_stats["bytes_out"] = 0
            self._app["file_stats"] = file_stats
        file_stats = self._app['file_stats']
        if counter not in file_stats:
            log.error(f"unexpected counter for file_stats: {counter}")
            return
        if inc < 1:
            log.error(f"unexpected inc for file_stats: {inc}")
            return

        file_stats[counter] += inc

    async def get_object(self, key, bucket=None, offset=0, length=None):
        """ Return data for object at given key.
           If Range is set, return the given byte range.
        """
        self._validateBucket(bucket)
        self._validateKey(key)

        if offset or length:
            range = f"bytes={offset}-{offset+length-1}"
            log.info(f"storage range request: {range}")

        filepath = self._getFilePath(bucket, key)
        log.info(f"get_object - filepath: {filepath}")

        start_time = time.time()
        log.debug(f"fileClient.get_object({bucket}/{key} start: {start_time}")
        loop = asyncio.get_event_loop()

        try:
            async with aiofiles.open(filepath, loop=loop, mode='rb') as f:
                if offset:
                    await f.seek(offset)
                if length:
                    data = await f.read(length)
                else:
                    data = await f.read()
            finish_time = time.time()
            log.info(f"fileClient.get_object({key} bucket={bucket}) start={start_time:.4f} finish={finish_time:.4f} elapsed={finish_time-start_time:.4f} bytes={len(data)}")
        except FileNotFoundError:
            msg = f"fileClient: {key} not found "
            log.warn(msg)
            raise HTTPNotFound()
        except IOError as ioe:
            msg = f"fileClient: IOError reading {bucket}/{key}: {ioe}"
            log.warn(msg)
            raise HTTPInternalServerError()

        except CancelledError as cle:
            self._file_stats_increment("error_count")
            msg = f"CancelledError for get file obj {key}: {cle}"
            log.error(msg)
            raise HTTPInternalServerError()
        except Exception as e:
            self._file_stats_increment("error_count")
            msg = f"Unexpected Exception {type(e)} get get_object {key}: {e}"
            log.error(msg)
            raise HTTPInternalServerError()
        return data

    async def put_object(self, key, data, bucket=None):
        """ Write data to given key.
            Returns client specific dict on success
        """
        self._validateBucket(bucket)
        self._validateKey(key)

        dirpath = self._getFilePath(bucket)
        if not pp.isdir(dirpath):
            msg = f"fileClient.put_object - bucket at path: {dirpath} not found"
            log.warn(msg)
            raise HTTPNotFound()

        start_time = time.time()
        filepath = self._getFilePath(bucket, key)
        log.debug(f"fileClient.put_object({bucket}/{key} start: {start_time}")
        loop = asyncio.get_event_loop()
        try:
            key_dirs = key.split("/")
            log.debug(f"key_dirs: {key_dirs}")
            if len(key_dirs) > 1:
                # create directories in the path if they don't already exist
                key_dirs = key_dirs[:-1]
                for key_dir in key_dirs:
                    dirpath = pp.join(dirpath, key_dir)
                    log.debug(f"pp.join({key_dir}) => {dirpath}")

                    dirpath = pp.normpath(dirpath)
                    log.debug(f"normpath: {dirpath}")

                    if not pp.isdir(dirpath):
                        log.debug(f"mkdir({dirpath})")
                        mkdir(dirpath)
                    else:
                        log.debug(f"isdir {dirpath} found")
            log.debug(f"open({filepath}, 'wb')")
            async with aiofiles.open(filepath, loop=loop, mode='wb') as f:
                await f.write(data)
            finish_time = time.time()
            log.info(f"fileClient.put_object({key} bucket={bucket}) start={start_time:.4f} finish={finish_time:.4f} elapsed={finish_time-start_time:.4f} bytes={len(data)}")
            write_rsp = self._getFileStats(filepath, data=data)
        except IOError as ioe:
            msg = f"fileClient: IOError writing {bucket}/{key}: {ioe}"
            log.warn(msg)
            raise HTTPInternalServerError()
        except CancelledError as cle:
            #file_stats_increment(app, "error_count")
            msg = f"CancelledError for put s3 obj {key}: {cle}"
            log.error(msg)
            raise HTTPInternalServerError()

        except Exception as e:
            #file_stats_increment(app, "error_count")
            msg = f"fileClient Unexpected Exception {type(e)} writing  {bucket}/{key}: {e}"
            log.error(msg)
            raise HTTPInternalServerError()

        if data and len(data) > 0:
            self._file_stats_increment("bytes_out", inc=len(data))
        log.debug(f"fileClient.put_object {key} complete, write_rsp: {write_rsp}")
        return write_rsp

    async def delete_object(self, key, bucket=None):
        """ Deletes the object at the given key
        """
        self._validateBucket(bucket)
        self._validateKey(key)
        filepath = self._getFilePath(bucket, key)

        start_time = time.time()
        log.debug(f"fileClient.delete_object({bucket}/{key} start: {start_time}")
        try:
            log.debug(f"os.remove({filepath})")
            remove(filepath)
            dir_name = pp.dirname(filepath)
            if not listdir(dir_name) and pp.basename(dir_name) != bucket:
                # direcctory is empty, remove
                rmdir(dir_name)
            finish_time = time.time()
            log.info(f"fileClient.delete_object({key} bucket={bucket}) start={start_time:.4f} finish={finish_time:.4f} elapsed={finish_time-start_time:.4f}")

        except IOError as ioe:
            msg = f"fileClient: IOError deleting {bucket}/{key}: {ioe}"
            log.warn(msg)
            raise HTTPInternalServerError()
        except CancelledError as cle:
            self._file_stats_increment("error_count")
            msg = f"CancelledError deleting s3 obj {key}: {cle}"
            log.error(msg)
            raise HTTPInternalServerError()
        except Exception as e:
            self._file_stats_increment("error_count")
            msg = f"Unexpected Exception {type(e)} deleting s3 obj {key}: {e}"
            log.error(msg)
            raise HTTPInternalServerError()
        await asyncio.sleep(0)  # for async compat

    async def is_object(self, key, bucket=None):
        self._validateBucket(bucket)
        self._validateKey(key)

        filepath = self._getFilePath(bucket, key)
        log.info(f"is_key - filepath: {filepath}")
        await asyncio.sleep(0)  # for async compat
        is_key = pp.isfile(filepath)
        return is_key

    async def get_key_stats(self, key, bucket=None):
        if not await self.is_object(key, bucket):
            log.warn(f"get_key_stats - key: {key} not found")
            raise HTTPNotFound()
        filepath = self._getFilePath(bucket, key)
        key_stats = self._getFileStats(filepath)

        return key_stats

    async def list_keys(self, prefix='', deliminator='', suffix='', include_stats=False, callback=None, bucket=None, limit=None):
        """ return keys matching the arguments
        """
        self._validateBucket(bucket)
        if deliminator and deliminator != '/':
            msg = "Only '/' is supported as deliminator"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        log.info(f"list_keys('{prefix}','{deliminator}','{suffix}', include_stats={include_stats}, bucket={bucket}")

        await asyncio.sleep(0)  # for async compat
        basedir = pp.join(self._root_dir, bucket)
        if prefix:
            basedir = pp.join(basedir,prefix)
        log.debug(f"fileClient listKeys for directory: {basedir}")

        if not pp.isdir(basedir):
            msg = f"listkeys - {basedir} not found"
            log.warn(msg)
            raise HTTPNotFound()

        # return all files (but not directories) under basedir
        files = []

        for root, dirs, filelist in walk(basedir):
            if deliminator:
                dirs.sort()
                for dirname in dirs:
                    if suffix and not dirname.endswith(suffix):
                        continue
                    log.debug(f"got dirname: {dirname}")
                    filename = pp.join(root[len(basedir):], dirname)
                    filename += '/'
                    files.append(filename)
                    if limit and len(files) >= limit:
                        break
                break  # don't recurse into subdirs

            else:
                filelist.sort()
                for filename in filelist:
                    if suffix and not filename.endswith(suffix):
                        continue
                    filepath = pp.join(root[len(basedir):], filename)
                    files.append(filepath)
                    if limit and len(files) >= limit:
                        break

        if include_stats:
            key_names = {}
        else:
            key_names = []
        for filename in files:
            if filename.startswith('/'):
                filename = filename[1:]
            log.debug(f"filename: {filename}, basedir: {basedir}")
            if suffix and not filename.endswith(suffix):
                continue
            if include_stats:
                filepath = pp.join(basedir, filename)
                with open(filepath, "rb") as f:
                    data = f.read()
                    log.debug(f"list_keys: read file: {filepath}, {len(data)} bytes, for getFileStats")
                    key_stats = self._getFileStats(filepath, data=data)
                key_name = pp.join(prefix, filename)
                key_names[key_name] = key_stats
            else:
                key_names.append(pp.join(prefix, filename))
            if limit and len(key_names) == limit:
                break

        if callback:
            if iscoroutinefunction(callback):
                await callback(self._app, key_names)
            else:
                callback(self._app, key_names)

        log.info(f"listKeys done, got {len(key_names)} keys")

        return key_names


    async def releaseClient(self):
        """ release the client collection to s3
           (Used for cleanup on application exit)
        """
        await asyncio.sleep(0)  # for async compat
        log.info("release fileClient")
