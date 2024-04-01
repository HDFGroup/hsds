import asyncio
import hashlib
from os import mkdir, rmdir, listdir, stat, remove, walk
import os.path as pp
from asyncio import CancelledError
from inspect import iscoroutinefunction
import time
import aiofiles
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError
from aiohttp.web_exceptions import HTTPBadRequest
from .. import hsds_logger as log
from .. import config


class FileClient:
    """
    Utility class for reading and storing data to local files
    using aiofiles package
    """

    def __init__(self, app):
        self._app = app
        root_dir = config.get("root_dir")
        if not root_dir:
            log.error("FileClient init: root_dir config not set")
            raise HTTPInternalServerError()
        if not pp.isdir(root_dir):
            log.error("FileClient init: root folder does not exist")
            raise HTTPInternalServerError()
        if not pp.isabs(root_dir):
            log.error("FileClient init: root dir most have absolute path")
            raise HTTPInternalServerError()
        self._root_dir = pp.normpath(root_dir)

    def _validateBucket(self, bucket):
        if not bucket:
            msg = "bucket not set"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if bucket.find("\\") != -1:
            msg = f"bucket: {bucket} contains invalid character, backslash"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if bucket.find("/") != -1:
            msg = f"bucket: {bucket} contains invalid character, slash"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    def _validateKey(self, key):
        if not key:
            msg = "key not set"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if key.startswith("/") or key.startswith("\\"):
            msg = f"invalid key: {key}, cannot start with slash"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    def _getFilePath(self, bucket, key=""):
        filepath = pp.join(self._root_dir, bucket, key)
        return pp.normpath(filepath)

    def _getFileStats(self, filepath, data=None):
        log.debug(f"_getFileStats({filepath})")
        if data is not None:
            if not isinstance(data, bytes):
                msg = "_getFileStats - expected data to be bytes, "
                msg += "not computing ETag"
                log.warn(msg)
                ETag = ""
            else:
                hash_object = hashlib.md5(data)
                ETag = hash_object.hexdigest()
        else:
            msg = "getFileStats - data is None, so ETag will not be computed"
            log.debug(msg)
            ETag = ""
        try:
            file_stats = stat(filepath)
            key_stats = {
                "ETag": ETag,
                "Size": file_stats.st_size,
                "LastModified": file_stats.st_mtime,
            }
            log.info(f"_getFileStats({filepath}) returning: {key_stats}")
        except FileNotFoundError:
            raise HTTPNotFound()
        return key_stats

    def _file_stats_increment(self, counter, inc=1):
        """Incremenet the indicated connter"""
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
        file_stats = self._app["file_stats"]
        if counter not in file_stats:
            log.error(f"unexpected counter for file_stats: {counter}")
            return
        if inc < 1:
            log.error(f"unexpected inc for file_stats: {inc}")
            return

    def getURIFromKey(self, key, bucket=None):
        """ return filesystem specific URI for given key and bucket """
        if not bucket:
            log.error("getURIFromKey, bucket not set")
            raise HTTPInternalServerError()
        if not key:
            log.error("getURIFromKey, key not set")
            raise HTTPInternalServerError()
        if key[0] == "/":
            key = key[1:]

        uri = self._getFilePath(key=key, bucket=bucket)

        return uri

    async def get_object(self, key, bucket=None, offset=0, length=-1):
        """Return data for object at given key.
        If Range is set, return the given byte range.
        """
        self._validateBucket(bucket)
        self._validateKey(key)

        if length > 0:
            range = f"bytes={offset} - {offset + length - 1}"
            log.info(f"storage range request: {range}")

        filepath = self._getFilePath(bucket, key)
        log.info(f"get_object - filepath: {filepath}")

        start_time = time.time()
        log.debug(f"fileClient.get_object({filepath} start: {start_time}")
        loop = asyncio.get_event_loop()

        try:
            async with aiofiles.open(filepath, loop=loop, mode="rb") as f:
                if offset:
                    await f.seek(offset)
                if length > 0:
                    data = await f.read(length)
                else:
                    data = await f.read()
            finish_time = time.time()
            msg = f"fileClient.get_object({key} bucket={bucket}) "
            msg += f"start={start_time:.4f} finish={finish_time:.4f} "
            msg += f"elapsed={finish_time - start_time:.4f}  bytes={len(data)}"
            log.info(msg)
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
        """Write data to given key.
        Returns client specific dict on success
        """
        self._validateBucket(bucket)
        self._validateKey(key)

        dirpath = self._getFilePath(bucket)
        if not pp.isdir(dirpath):
            msg = "fileClient.put_object - bucket at path: "
            msg += f"{dirpath} not found"
            log.warn(msg)
            raise HTTPNotFound()

        start_time = time.time()
        filepath = pp.normpath(self._getFilePath(bucket, key))
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
            async with aiofiles.open(filepath, loop=loop, mode="wb") as f:
                await f.write(data)
            finish_time = time.time()
            msg = f"fileClient.put_object({key} bucket={bucket}) "
            msg += f"start={start_time:.4f} finish={finish_time:.4f} "
            msg += f"elapsed={finish_time - start_time:.4f} bytes={len(data)}"
            log.info(msg)
            write_rsp = self._getFileStats(filepath, data=data)
        except IOError as ioe:
            msg = f"fileClient: IOError writing {bucket}/{key}: {ioe}"
            log.warn(msg)
            raise HTTPInternalServerError()
        except CancelledError as cle:
            # file_stats_increment(app, "error_count")
            msg = f"CancelledError for put file obj {key}: {cle}"
            log.error(msg)
            raise HTTPInternalServerError()

        except Exception as e:
            # file_stats_increment(app, "error_count")
            msg = f"fileClient Unexpected Exception {type(e)} "
            msg += f"writing {bucket}/{key}: {e}"
            log.error(msg)
            raise HTTPInternalServerError()

        if data and len(data) > 0:
            self._file_stats_increment("bytes_out", inc=len(data))
            msg = f"fileClient.put_object {key} complete, "
            msg += f"write_rsp: {write_rsp}"
            log.debug(msg)
        return write_rsp

    async def delete_object(self, key, bucket=None):
        """Deletes the object at the given key"""
        self._validateBucket(bucket)
        self._validateKey(key)
        filepath = self._getFilePath(bucket, key)

        start_time = time.time()
        msg = f"fileClient.delete_object({bucket}/{key} start: {start_time}"
        log.debug(msg)
        try:
            log.debug(f"os.remove({filepath})")
            remove(filepath)
            dir_name = pp.dirname(filepath)
            if not listdir(dir_name) and pp.basename(dir_name) != bucket:
                # direcctory is empty, remove
                rmdir(dir_name)
            finish_time = time.time()
            msg = f"fileClient.delete_object({key} bucket={bucket}) "
            msg += f"start={start_time:.4f} finish={finish_time:.4f} "
            msg += f"elapsed={finish_time - start_time:.4f}"
            log.info(msg)

        except IOError as ioe:
            msg = f"fileClient: IOError deleting {bucket}/{key}: {ioe}"
            log.warn(msg)
            raise HTTPInternalServerError()

        except CancelledError as cle:
            self._file_stats_increment("error_count")
            msg = f"CancelledError deleting file obj {key}: {cle}"
            log.error(msg)
            raise HTTPInternalServerError()

        except Exception as e:
            self._file_stats_increment("error_count")
            msg = f"Unexpected Exception {type(e)} deleting file obj {key}: {e}"
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

    async def list_keys(
        self,
        prefix="",
        deliminator="",
        suffix="",
        include_stats=False,
        callback=None,
        bucket=None,
        limit=None,
    ):
        """return keys matching the arguments"""
        self._validateBucket(bucket)
        if deliminator and deliminator != "/":
            msg = "Only '/' is supported as deliminator"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        msg = f"list_keys('{prefix}','{deliminator}','{suffix}' "
        msg += f"include_stats={include_stats}, bucket={bucket}, "
        msg += f"callback {'set' if callback is not None else 'not set'}"
        log.info(msg)

        filesep = pp.normpath("/")  # '/' on linux, '\\' on windows

        basedir = pp.join(self._root_dir, bucket)
        if prefix:
            basedir = pp.join(basedir, prefix)
        basedir = pp.normpath(basedir)
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
                    nlen = len(basedir)
                    filename = f"{root[nlen:]}{dirname}{filesep}"
                    files.append(filename)
                    if limit and len(files) >= limit:
                        break
                    if len(files) % 1000 == 0:
                        await asyncio.sleep(0)
                break  # don't recurse into subdirs

            else:
                filelist.sort()
                for filename in filelist:
                    if suffix and not filename.endswith(suffix):
                        continue
                    nlen = len(basedir)
                    filepath = pp.join(root[nlen:], filename)
                    files.append(filepath)
                    if limit and len(files) >= limit:
                        break
                    if len(files) % 1000 == 0:
                        await asyncio.sleep(0)

        # use a dictionary to hold return values if stats are needed
        key_names = {} if include_stats else []
        count = 0
        for filename in files:
            if filename.startswith(filesep):
                filename = filename[1:]
            log.debug(f"filename: {filename}, basedir: {basedir}")
            if suffix and not filename.endswith(suffix):
                continue
            if include_stats:
                filepath = pp.join(basedir, filename)
                with open(filepath, "rb") as f:
                    data = f.read()
                    msg = f"list_keys: read file: {filepath}, "
                    msg += f"{len(data)} bytes, for getFileStats"
                    log.debug(msg)
                    key_stats = self._getFileStats(filepath, data=data)
                key_name = pp.join(prefix, filename)
                # replace any windows-style sep with linux
                key_name = key_name.replace("\\", "/")
                key_names[key_name] = key_stats
            else:
                key_name = pp.join(prefix, filename)
                # replace any windows-style sep with linux
                key_name = key_name.replace("\\", "/")
                key_names.append(key_name)

            if limit and len(key_names) == limit:
                break
        count += len(key_names)
        if callback:
            if iscoroutinefunction(callback):
                await callback(self._app, key_names)
            else:
                callback(self._app, key_names)
            key_names = {} if include_stats else []  # reset

        log.info(f"listKeys done, got {count} keys")
        for key in key_names:
            log.debug(key)
        if not callback and count != len(key_names):
            msg = f"expected {count} keys in return list but "
            msg == f"got {len(key_names)}"
            log.warning(msg)

        return key_names

    async def releaseClient(self):
        """release the client collection
        (Used for cleanup on application exit)
        """
        await asyncio.sleep(0)  # for async compat
        log.info("release fileClient")
