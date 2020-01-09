import asyncio
import time
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError
import hsds_logger as log
import config

class MemClient():
    """
     Utility class for reading and storing data in RAM
    """

    def __init__(self, app):

        self._app = app

        if "s3" in app:
            self._client = app["s3"]
            return

        if config.get('aws_s3_gateway'):
            msg="Unexpected aws s3 gateway"
            log.error(msg)
            raise KeyError(msg)

        log.info("MemClient - initializing bucket map")
        self._client = {}  # map of buckets

        app['s3'] = self._client  # save so same client can be returned in subsequent calls

    def _s3_stats_increment(self, counter, inc=1):
        """ Incremenet the indicated connter
        """
        if "s3_stats" not in self._app:
            # setup stats
            s3_stats = {}
            s3_stats["get_count"] = 0
            s3_stats["put_count"] = 0
            s3_stats["delete_count"] = 0
            s3_stats["list_count"] = 0
            s3_stats["error_count"] = 0
            s3_stats["bytes_in"] = 0
            s3_stats["bytes_out"] = 0
            self._app["s3_stats"] = s3_stats
        s3_stats = self._app['s3_stats']
        if counter not in s3_stats:
            log.error(f"unexpected counter for s3_stats: {counter}")
            return
        if inc < 1:
            log.error(f"unexpected inc for s3_stats: {inc}")
            return

        s3_stats[counter] += inc


    async def get_object(self, key, bucket=None, range=''):
        """ Return data for object at given key.
           If Range is set, return the given byte range.
        """
        if not bucket:
            log.error("get_object - bucket not set")
            raise HTTPInternalServerError()

        buckets = self._client

        if bucket not in buckets:
            msg = f"s3_bucket: {bucket} not found"
            log.info(msg)
            raise HTTPNotFound()

        bucket_map = buckets[bucket]
        if key not in bucket_map:
            msg = f"keyu: {key} not found in bucket: {bucket}"
            log.info(msg)
            raise HTTPNotFound()

        await asyncio.sleep(0) # 0 sec sleep to make the function async
        data = bucket_map[key]
        return data

    async def put_object(self, key, data, bucket=None):
        """ Write data to given key.
            Returns client specific dict on success
        """
        if not bucket:
            log.error("put_object - bucket not set")
            raise HTTPInternalServerError()

        if not isinstance(data, bytes):
            log.error("put_object - expected bytes type")
            raise HTTPInternalServerError()

        buckets = self._client
        if bucket not in buckets:
            bucket_map = {}
            buckets[bucket] = bucket_map
        else:
            bucket_map = buckets[bucket]

        start_time = time.time()
        log.debug(f"memClient.put_object({bucket}/{key} start: {start_time}")
        try:
            await asyncio.sleep(0) # 0 sec sleep to make the function async
            bucket_map[key] = data
            finish_time = time.time()
            log.info(f"s3Client.put_object({key} bucket={bucket}) start={start_time:.4f} finish={finish_time:.4f} elapsed={finish_time-start_time:.4f} bytes={len(data)}")
            s3_rsp = {"etag": "abcd", "size": len(data), "lastModified": int(finish_time)}
        except Exception as e:
            msg = f"Unexpected Exception {type(e)} putting s3 obj {key}: {e}"
            log.error(msg)
            raise HTTPInternalServerError()
        if data and len(data) > 0:
            self._s3_stats_increment("bytes_out", inc=len(data))
        log.debug(f"s3Client.put_object {key} complete, s3_rsp: {s3_rsp}")
        return s3_rsp

    async def delete_object(self, key, bucket=None):
        """ Deletes the object at the given key
        """
        if not bucket:
            log.error("delete_object - bucket not set")
            raise HTTPInternalServerError()

        buckets = self._client
        if bucket not in buckets:
            msg = f"s3_bucket: {bucket} not found"
            log.info(msg)
            raise HTTPNotFound()

        bucket_map = buckets[bucket]
        if key not in bucket_map:
            msg = f"keyu: {key} not found in bucket: {bucket}"
            log.info(msg)
            raise HTTPNotFound()

        start_time = time.time()
        log.debug(f"memClient.delete_object({bucket}/{key} start: {start_time}")
        try:
            await asyncio.sleep(0) # 0 sec sleep to make the function async
            del bucket_map[key]
        except Exception as e:
            msg = f"Unexpected Exception {type(e)} putting s3 obj {key}: {e}"
            log.error(msg)
            raise HTTPInternalServerError()


    def _getPageItems(self, response, items, include_stats=False):

        log.info("getPageItems")

        if 'CommonPrefixes' in response:
            log.debug("got CommonPrefixes in s3 response")
            common = response["CommonPrefixes"]
            for item in common:
                if 'Prefix' in item:
                    log.debug(f"got s3 prefix: {item['Prefix']}")
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
                        log.warn(f"No ETag for key: {key_name}")
                    if "Size" in item:
                        stats["Size"] = item["Size"]
                    else:
                        log.warn(f"No Size for key: {key_name}")
                    if "LastModified" in item:
                        stats["LastModified"] = int(item["LastModified"].timestamp())
                    else:
                        log.warn(f"No LastModified for key: {key_name}")
                    log.debug(f"key: {key_name} stats: {stats}")
                    items[key_name] = stats
                else:
                    items.append(key_name)

    async def list_keys(self, prefix='', deliminator='', suffix='', include_stats=False, callback=None, bucket=None, limit=None):
        """ return keys matching the arguments
        """
        if not bucket:
            log.error("putt_object - bucket not set")
            raise HTTPInternalServerError()
        log.info(f"list_keys('{prefix}','{deliminator}','{suffix}', include_stats={include_stats}")
        buckets = self._client
        if bucket not in buckets:
            return []

        bucket_map = buckets[bucket]

        key_set = set()

        for key in bucket_map:
            if prefix and not key.startswith(prefix):
                continue  # skip any keys without the prefix
            if deliminator:
                index = key[len(prefix):].find(deliminator)
                if index > 0:
                    num_chars = index + len(prefix)
                    key = key[:num_chars]
            key_set.add(key)

        key_list = list(key_set)
        key_list.sort()
        if limit and len(key_list) > limit:
            key_list = key_list[:limit]

        if include_stats:
            now = time.time()
            # add ETag, modified time, and size to each item
            items = {}

            for key in key_list:
                item = {"ETag": "ABCD", "LastModified": now}
                if key in bucket_map:
                    obj_size = len(bucket_map[key])
                else:
                    obj_size = 0
                item["Size"] = obj_size
                items[key] = item
        else:
            # just return the list
            items = key_list
        log.info(f"getS3Keys done, got {len(items)} keys")
        return items

    async def releaseClient(self):
        """ release the client collection to s3
           (Used for cleanup on application exit)
        """
        log.info("release memClient")
