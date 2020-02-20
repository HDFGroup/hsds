import time
import boto3
from botocore.exceptions import ClientError
from .. import hsds_logger as log

class S3Client():
    """
     Utility class for reading and storing data to AWS S3
    """

    def __init__(self, app):
        if "s3" in app:
            self._client = app["s3"]
        else:
            s3 = boto3.client('s3')
            if s3:
                self._client = s3 # save so same client can be returned in subsequent calls
                if app:
                    app["s3"] = s3
            else:
                log.error("unable to get s3 client")

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

    def get_object(self, key, bucket=None, offset=0, length=None):
        """ Return data for object at given key.
           If Range is set, return the given byte range.
        """

        range=""
        if length:
            range = f"bytes={offset}-{offset+length-1}"
            log.info(f"storage range request: {range}")

        if not bucket:
            log.error("get_object - bucket not set")
            raise KeyError()

        start_time = time.time()
        log.debug(f"s3CLient.get_object({bucket}/{key} start: {start_time}")
        try:
            resp =  self._client.get_object(Bucket=bucket, Key=key, Range=range)
            body = resp["Body"]
            data = body.read()
            finish_time = time.time()
            log.info(f"s3Client.getS3Bytes({key} bucket={bucket}) start={start_time:.4f} finish={finish_time:.4f} elapsed={finish_time-start_time:.4f} bytes={len(data)}")

            resp['Body'].close()
        except ClientError as ce:
            # key does not exist?
            # check for not found status
            response_code = ce.response["Error"]["Code"]
            if response_code in ("NoSuchKey", "404") or response_code == 404:
                msg = f"s3_key: {key} not found "
                log.warn(msg)
                raise
            elif response_code == "NoSuchBucket":
                msg = f"s3_bucket: {bucket} not found"
                log.info(msg)
                raise
            elif response_code in ("AccessDenied", "401", "403") or response_code in (401, 403):
                msg = f"access denied for s3_bucket: {bucket}"
                log.info(msg)
                raise
            else:
                self._s3_stats_increment("error_count")
                log.error(f"got unexpected ClientError on s3 get {key}: {response_code}")
                raise
        except Exception as e:
            self._s3_stats_increment("error_count")
            msg = f"Unexpected Exception {type(e)} get s3 obj {key}: {e}"
            log.error(msg)
            raise
        return data



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

    def list_keys(self, prefix='', deliminator='', suffix='', include_stats=False, callback=None, bucket=None, limit=None):
        """ return keys matching the arguments
        """
        if not bucket:
            log.error("list_keys - bucket not set")
            raise KeyError()
        log.info(f"list_keys('{prefix}','{deliminator}','{suffix}', include_stats={include_stats}")
        paginator = self._client.get_paginator('list_objects')
        if include_stats:
            # use a dictionary to hold return values
            key_names = {}
        else:
            # just use a list
            key_names = []
        count = 0

        try:
            for page in paginator.paginate(
                PaginationConfig={'PageSize': 1000}, Bucket=bucket,  Prefix=prefix, Delimiter=deliminator):
                self._getPageItems(page, key_names, include_stats=include_stats)
                count += len(key_names)
                if callback:
                    callback(self._app, key_names)
                if limit and count >= limit:
                    log.info(f"list_keys - reached limit {limit}")
                    break
        except ClientError as ce:
            log.warn(f"bucket: {bucket} does not exist, exception: {ce}")
            raise
        except Exception as e:
             log.error(f"s3 paginate got exception {type(e)}: {e}")
             raise

        log.info(f"getS3Keys done, got {len(key_names)} keys")

        return key_names

    def releaseClient(self):
        """ release the client collection to s3
           (Used for cleanup on application exit)
        """
        log.info("release S3Client")
