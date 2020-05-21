import asyncio
from asyncio import CancelledError
from  inspect import iscoroutinefunction
import subprocess
import datetime
import json
import time
from aiobotocore.config import AioConfig
from aiobotocore import get_session
from botocore.exceptions import ClientError
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError, HTTPForbidden, HTTPBadRequest
from .. import hsds_logger as log
from .. import config

class S3Client():
    """
     Utility class for reading and storing data to AWS S3
    """

    def __init__(self, app):
        if "session" not in app:
            session = get_session()
            app["session"] = session
        else:
            session = app["session"]
        self._app = app

        if "s3" in app:
            if "token_expiration" in app:
                # check that our token is not about to expire
                expiration = app["token_expiration"]
                now = datetime.datetime.now()
                delta = expiration - now
                if delta.total_seconds() > 10:
                    self._client = app["s3"]
                    return
                # otherwise, fall through and get a new token
                log.info("S3 access token has expired - renewing")
            else:
                self._client = app["s3"]
                return

        # first time setup of s3 client or limited time token has expired

        aws_region = None
        aws_secret_access_key = None
        aws_access_key_id = None
        aws_iam_role = None
        max_pool_connections = 64
        aws_session_token = None
        try:
            aws_iam_role = config.get("aws_iam_role")
        except KeyError:
            pass
        try:
            aws_secret_access_key = config.get("aws_secret_access_key")
        except KeyError:
            pass
        try:
            aws_access_key_id = config.get("aws_access_key_id")
        except KeyError:
            pass
        try:
            aws_region = config.get("aws_region")
        except KeyError:
            pass
        try:
            max_pool_connections = config.get('aio_max_pool_connections')
        except KeyError:
            pass
        log.info(f"S3Client init - aws_region {aws_region}")

        s3_gateway = config.get('aws_s3_gateway')
        if not s3_gateway:
            msg="Invalid aws s3 gateway"
            log.error(msg)
            raise ValueError(msg)
        log.info(f"Using S3Gateway: {s3_gateway}")

        use_ssl = False
        if s3_gateway.startswith("https"):
            use_ssl = True

        if not aws_secret_access_key or aws_secret_access_key == 'xxx':
            log.info("aws secret access key not set")
            aws_secret_access_key = None
        if not aws_access_key_id or aws_access_key_id == 'xxx':
            log.info("aws access key id not set")
            aws_access_key_id = None

        if aws_iam_role and not aws_secret_access_key:
            log.info(f"using iam role: {aws_iam_role}")
            log.info("getting EC2 IAM role credentials")
            # Use EC2 IAM role to get credentials
            # See: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html?icmpid=docs_ec2_console
            curl_cmd = ["curl", f"http://169.254.169.254/latest/meta-data/iam/security-credentials/{aws_iam_role}"]
            p = subprocess.run(curl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if p.returncode != 0:
                msg = f"Error getting IAM role credentials: {p.stderr}"
                log.error(msg)
            else:
                stdout = p.stdout.decode("utf-8")
                try:
                    cred = json.loads(stdout)
                    aws_secret_access_key = cred["SecretAccessKey"]
                    aws_access_key_id = cred["AccessKeyId"]
                    aws_cred_expiration = cred["Expiration"]
                    aws_session_token = cred["Token"]
                    log.info(f"Got Expiration of: {aws_cred_expiration}")
                    expiration_str = aws_cred_expiration[:-1] + "UTC" # trim off 'Z' and add 'UTC'
                    # save the expiration
                    app["token_expiration"] = datetime.datetime.strptime(expiration_str, "%Y-%m-%dT%H:%M:%S%Z")
                except json.JSONDecodeError:
                    msg = "Unexpected error decoding EC2 meta-data response"
                    log.error(msg)
                except KeyError:
                    msg = "Missing expected key from EC2 meta-data response"
                    log.error(msg)

        aio_config = AioConfig(max_pool_connections=max_pool_connections)
        self._client = session.create_client('s3', region_name=aws_region,
                                    aws_secret_access_key=aws_secret_access_key,
                                    aws_access_key_id=aws_access_key_id,
                                    aws_session_token=aws_session_token,
                                    endpoint_url=s3_gateway,
                                    use_ssl=use_ssl,
                                    config=aio_config)

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

    async def get_object(self, key, bucket=None, offset=0, length=None):
        """ Return data for object at given key.
           If Range is set, return the given byte range.
        """

        range=""
        if length:
            range = f"bytes={offset}-{offset+length-1}"
            log.info(f"storage range request: {range}")

        if not bucket:
            log.error("get_object - bucket not set")
            raise HTTPInternalServerError()

        start_time = time.time()
        log.debug(f"s3Client.get_object({bucket}/{key}) start: {start_time}")
        try:
            resp = await self._client.get_object(Bucket=bucket, Key=key, Range=range)
            data = await resp['Body'].read()
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
                raise HTTPNotFound()
            elif response_code == "NoSuchBucket":
                msg = f"s3_bucket: {bucket} not found"
                log.info(msg)
                raise HTTPNotFound()
            elif response_code in ("AccessDenied", "401", "403") or response_code in (401, 403):
                msg = f"access denied for s3_bucket: {bucket}"
                log.info(msg)
                raise HTTPForbidden()
            else:
                self._s3_stats_increment("error_count")
                log.error(f"got unexpected ClientError on s3 get {key}: {response_code}")
                raise HTTPInternalServerError()
        except CancelledError as cle:
            self._s3_stats_increment("error_count")
            msg = f"CancelledError for get s3 obj {key}: {cle}"
            log.error(msg)
            raise HTTPInternalServerError()
        except Exception as e:
            self._s3_stats_increment("error_count")
            msg = f"Unexpected Exception {type(e)} get s3 obj {key}: {e}"
            log.error(msg)
            raise HTTPInternalServerError()
        return data

    async def put_object(self, key, data, bucket=None):
        """ Write data to given key.
            Returns client specific dict on success
        """
        if not bucket:
            log.error("put_object - bucket not set")
            raise HTTPInternalServerError()

        start_time = time.time()
        log.debug(f"s3Client.put_object({bucket}/{key} start: {start_time}")
        try:
            rsp = await self._client.put_object(Bucket=bucket, Key=key, Body=data)
            finish_time = time.time()
            log.info(f"s3Client.put_object({key} bucket={bucket}) start={start_time:.4f} finish={finish_time:.4f} elapsed={finish_time-start_time:.4f} bytes={len(data)}")
            s3_rsp = {"etag": rsp["ETag"], "size": len(data), "lastModified": int(finish_time)}
        except ClientError as ce:
            response_code = ce.response["Error"]["Code"]
            if response_code == "NoSuchBucket":
                msg = f"s3_bucket: {bucket} not found"
                log.warn(msg)
                raise HTTPNotFound()
            else:
                self._s3_stats_increment("error_count")
                msg = f"Error putting s3 obj {key}: {ce}"
                log.error(msg)
                raise HTTPInternalServerError()
        except CancelledError as cle:
            #s3_stats_increment(app, "error_count")
            msg = f"CancelledError for put s3 obj {key}: {cle}"
            log.error(msg)
            raise HTTPInternalServerError()
        except Exception as e:
            #s3_stats_increment(app, "error_count")
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

        start_time = time.time()
        log.debug(f"s3Client.delete_object({bucket}/{key} start: {start_time}")
        try:
            await self._client.delete_object(Bucket=bucket, Key=key)
            finish_time = time.time()
            log.info(f"s3Client.delete_object({key} bucket={bucket}) start={start_time:.4f} finish={finish_time:.4f} elapsed={finish_time-start_time:.4f}")

        except ClientError as ce:
            # key does not exist?
            key_found = await self.isS3Obj(key)
            if not key_found:
                log.warn(f"delete on s3key {key} but not found")
                raise HTTPNotFound()
            # else some other error
            self._s3_stats_increment("error_count")
            msg = f"Error deleting s3 obj: {ce}"
            log.error(msg)
            raise HTTPInternalServerError()
        except CancelledError as cle:
            self._s3_stats_increment("error_count")
            msg = f"CancelledError deleting s3 obj {key}: {cle}"
            log.error(msg)
            raise HTTPInternalServerError()
        except Exception as e:
            self._s3_stats_increment("error_count")
            msg = f"Unexpected Exception {type(e)} deleting s3 obj {key}: {e}"
            log.error(msg)
            raise HTTPInternalServerError()

    async def is_object(self, key, bucket=None):
        """ Return true if the given object exists
        """
        if not bucket:
            log.error("is_object - bucket not set")
            raise HTTPInternalServerError()
        start_time = time.time()
        found = False
        try:
            head_data = await self._client.head_object(Bucket=bucket, Key=key)
            finish_time = time.time()
            found = True
            log.info(f"head: {head_data}")
        except ClientError:
            # key does not exist?
            msg = f"Key: {key} not found"
            log.info(msg)
            finish_time = time.time()
        except CancelledError as cle:
            self._s3_stats_increment("error_count")
            msg = f"CancelledError getting head for s3 obj {key}: {cle}"
            log.error(msg)
            raise HTTPInternalServerError()
        except Exception as e:
            self._s3_stats_increment("error_count")
            msg = f"Unexpected Exception {type(e)} getting head for s3 obj {key}: {e}"
            log.error(msg)
            raise HTTPInternalServerError()
        log.info(f"s3Client.is_object({key} bucket={bucket}) start={start_time:.4f} finish={finish_time:.4f} elapsed={finish_time-start_time:.4f}")

        return found

    async def get_key_stats(self, key, bucket=None):
        """ Get ETag, size, and last modified time for given objecct
        """
        start_time = time.time()
        try:
            head_data = await self._client.head_object(Bucket=bucket, Key=key)
            finish_time = time.time()
            log.info(f"head: {head_data}")
        except ClientError:
            # key does not exist?
            msg = f"s3Client.get_key_stats: Key: {key} not found"
            log.info(msg)
            finish_time = time.time()
            raise HTTPNotFound()
        except CancelledError as cle:
            self._s3_stats_increment("error_count")
            msg = f"s3Client.get_key_stats: CancelledError getting head for s3 obj {key}: {cle}"
            log.error(msg)
            raise HTTPInternalServerError()
        except Exception as e:
            self._s3_stats_increment("error_count")
            msg = f"s3Client.get_key_stats: Unexpected Exception {type(e)} getting head for s3 obj {key}: {e}"
            log.error(msg)
            raise HTTPInternalServerError()

        for head_key in ("ContentLength", "ETag", "LastModified"):
            if head_key not in head_data:
                msg = f"s3Client.get_key_stats, expected to find key: {head_key} in head_data"
                log.error(msg)
                raise HTTPInternalServerError()

        last_modified_dt = head_data["LastModified"]
        if not isinstance(last_modified_dt, datetime.datetime):
            msg ="S3Client.get_key_stats, expected datetime object in head data"
            log.error(msg)
            raise HTTPInternalServerError()
        key_stats = {}
        key_stats["Size"] = head_data['ContentLength']
        key_stats["ETag"] = head_data["ETag"]
        key_stats["LastModified"] = datetime.datetime.timestamp(last_modified_dt)
        log.info(f"s3Client.get_key_stats({key} bucket={bucket}) start={start_time:.4f} finish={finish_time:.4f} elapsed={finish_time-start_time:.4f}")


        return key_stats

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
            log.error("list_keys - bucket not set")
            raise HTTPInternalServerError()
        log.info(f"list_keys('{prefix}','{deliminator}','{suffix}', include_stats={include_stats}")
        if deliminator and deliminator != '/':
            msg = "Only '/' is supported as deliminator"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        paginator = self._client.get_paginator('list_objects')
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
                self._getPageItems(page, key_names, include_stats=include_stats)
                count += len(key_names)
                if callback:
                    if iscoroutinefunction(callback):
                        await callback(self._app, key_names)
                    else:
                        callback(self._app, key_names)
                if limit and count >= limit:
                    log.info(f"list_keys - reached limit {limit}")
                    break
        except ClientError as ce:
            log.warn(f"bucket: {bucket} does not exist, exception: {ce}")
            raise HTTPNotFound()
        except Exception as e:
             log.error(f"s3 paginate got exception {type(e)}: {e}")
             raise HTTPInternalServerError()

        log.info(f"getS3Keys done, got {len(key_names)} keys")

        return key_names

    async def releaseClient(self):
        """ release the client collection to s3
           (Used for cleanup on application exit)
        """
        log.info("release S3Client")
        if 's3' in self._app:
            client = self._app['s3']
            await client.close()
            del self._app['s3']
