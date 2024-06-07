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
import asyncio
import os
from asyncio import CancelledError
from inspect import iscoroutinefunction
import subprocess
import datetime
import json
import time
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from botocore.exceptions import ClientError
from botocore import UNSIGNED
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError
from aiohttp.web_exceptions import HTTPForbidden, HTTPBadRequest
from .. import hsds_logger as log
from .. import config

S3_URI = "s3://"
S3_INVALID_ACCESS_CODES = ("AccessDenied", "InvalidAccessKeyId", "401", "403", 401, 403)


class S3Client:
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
                    _client = app["s3"]
                    return _client
                # otherwise, fall through and get a new token
                log.info("S3 access token has expired - renewing")
            else:
                _client = app["s3"]
                return _client

        # first time setup of s3 client or limited time token has expired

        self._aws_region = None
        self._aws_iam_role = None
        self._aws_secret_access_key = None
        self._aws_access_key_id = None
        max_pool_connections = 64
        self._aws_session_token = None
        self._aws_role_arn = None
        self._aws_session_token = None
        self._aws_no_sign_request = False
        signature_version = None

        try:
            self._aws_iam_role = config.get("aws_iam_role")
        except KeyError:
            pass

        try:
            self._aws_secret_access_key = config.get("aws_secret_access_key")
        except KeyError:
            pass
        try:
            self._aws_access_key_id = config.get("aws_access_key_id")
        except KeyError:
            pass
        try:
            self._aws_region = config.get("aws_region")
        except KeyError:
            pass
        try:
            max_pool_connections = config.get("aio_max_pool_connections")
        except KeyError:
            pass
        self._aws_role_arn = None
        if "AWS_ROLE_ARN" in os.environ:
            # Assume IAM roles for EKS is being used.  See:
            # https://aws.amazon.com/blogs/opensource/\
            # introducing-fine-grained-iam-roles-service-accounts/
            self._aws_role_arn = os.environ["AWS_ROLE_ARN"]
            log.info(f"AWS_ROLE_ARN set to: {self._aws_role_arn}")
            if "AWS_WEB_IDENTITY_TOKEN_FILE" in os.environ:
                token_file = os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"]
                log.debug(f"AWS_WEB_IDENTITY_TOKEN_FILE is: {token_file}")
        if "AWS_SESSION_TOKEN" in os.environ:
            self._aws_session_token = os.environ["AWS_SESSION_TOKEN"]
            log.debug(f"got AWS_SESSION_TOKEN: {self._aws_session_token}")

        try:
            self._aws_no_sign_request = config.get("aws_s3_no_sign_request", False)
            if self._aws_no_sign_request:
                signature_version = UNSIGNED
        except KeyError:
            pass

        self._aio_config = AioConfig(max_pool_connections=max_pool_connections,
                                     signature_version=signature_version)

        log.debug(f"S3Client init - aws_region {self._aws_region}")

        self._s3_gateway = config.get("aws_s3_gateway")
        if not self._s3_gateway:
            msg = "Invalid aws s3 gateway"
            log.error(msg)
            raise ValueError(msg)
        log.debug(f"Using S3Gateway: {self._s3_gateway}")

        self._use_ssl = False
        if self._s3_gateway.startswith("https"):
            self._use_ssl = True

        if not self._aws_secret_access_key or self._aws_secret_access_key == "xxx":
            log.debug("aws secret access key not set")
            self._aws_secret_access_key = None
        if not self._aws_access_key_id or self._aws_access_key_id == "xxx":
            log.debug("aws access key id not set")
            self._aws_access_key_id = None
        else:
            log.debug(f"using aws key id: {self._aws_access_key_id}")
        self._renewToken()

    def _get_client_kwargs(self):
        kwargs = {}
        kwargs["region_name"] = self._aws_region
        kwargs["aws_secret_access_key"] = self._aws_secret_access_key
        kwargs["aws_access_key_id"] = self._aws_access_key_id
        kwargs["aws_session_token"] = self._aws_session_token
        kwargs["endpoint_url"] = self._s3_gateway
        kwargs["use_ssl"] = self._use_ssl
        kwargs["config"] = self._aio_config
        # log.debug(f"s3 kwargs: {kwargs}")
        return kwargs

    def _renewToken(self):
        """if using an aws_iam_role, fetch credentials if our token is about
        to expire, otherwise just return
        """
        app = self._app

        if self._aws_no_sign_request:
            # no need to get a token
            return

        if not self._aws_iam_role:
            # need this to get a token
            return
        if self._aws_role_arn:
            # this is set for running in EKS, shouldn't need a token
            return

        if "token_expiration" in app:
            # check that our token is not about to expire
            expiration = app["token_expiration"]
        else:
            expiration = None

        if expiration:
            now = datetime.datetime.now()
            delta = expiration - now
            if delta.total_seconds() > 10:
                renew_token = False
                self._aws_session_token = app["aws_session_token"]
            else:
                renew_token = True

        elif self._aws_access_key_id:
            renew_token = False  # access key set by config
        else:
            renew_token = True  # first time getting token

        if renew_token:
            msg = f"get S3 access token using iam role: {self._aws_iam_role}"
            log.info(msg)
            # Use EC2 IAM role to get credentials
            # See: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/\
            # iam-roles-for-amazon-ec2.html?icmpid=docs_ec2_console
            url = "http://169.254.169.254/latest/meta-data/iam/"
            url += f"security-credentials/{self._aws_iam_role}"
            curl_cmd = ["curl", "--no-progress-meter", url]
            kwargs = {"stdout": subprocess.PIPE, "stderr": subprocess.PIPE}
            p = subprocess.run(curl_cmd, **kwargs)
            if p.returncode != 0:
                msg = f"Error getting IAM role credentials: {p.stderr}"
                log.error(msg)
            else:
                stdout = p.stdout.decode("utf-8")
                try:
                    cred = json.loads(stdout)
                    self._aws_secret_access_key = cred["SecretAccessKey"]
                    self._aws_access_key_id = cred["AccessKeyId"]
                    aws_cred_expiration = cred["Expiration"]
                    self._aws_session_token = cred["Token"]
                    msg = "renew token: got Expiration of: "
                    msg += f"{aws_cred_expiration}"
                    log.info(msg)
                    # trim off 'Z' and add 'UTC'
                    s = aws_cred_expiration[:-1] + "UTC"
                    # save the expiration
                    t_e = datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%Z")
                    app["token_expiration"] = t_e
                    app["aws_session_token"] = self._aws_session_token
                except json.JSONDecodeError:
                    msg = "Unexpected error decoding EC2 meta-data "
                    msg += f"response: {stdout}"
                    log.error(msg)
                except KeyError:
                    msg = "Missing expected key from EC2 meta-data "
                    msg += f"response: {stdout}"
                    log.error(msg)

    def _s3_stats_increment(self, counter, inc=1):
        """Incremenet the indicated connter"""
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
        s3_stats = self._app["s3_stats"]
        if counter not in s3_stats:
            log.error(f"unexpected counter for s3_stats: {counter}")
            return
        if inc < 1:
            log.error(f"unexpected inc for s3_stats: {inc}")
            return

        s3_stats[counter] += inc

    def getURIFromKey(self, key, bucket=None):
        """ return S3 specific URI for given key and bucket """
        if not bucket:
            log.error("getURIFromKey, bucket not set")
            raise HTTPInternalServerError()
        if not key:
            log.error("getURIFromKey, key not set")
            raise HTTPInternalServerError()
        if key[0] == "/":
            key = key[1:]

        uri = f"s3://{bucket}/{key}"
        return uri

    async def get_object(self, key, bucket=None, offset=0, length=-1):
        """Return data for object at given key.
        If Range is set, return the given byte range.
        """

        range = ""

        if not bucket:
            log.error("get_object - bucket not set")
            raise HTTPInternalServerError()

        # remove s3:// prefix if present
        if bucket.startswith(S3_URI):
            bucket = bucket[len(S3_URI):]

        start_time = time.time()
        if length > 0:
            range = f"bytes={offset}-{offset + length - 1}"
            log.info(f"storage range request: {range}")
        log.debug(f"s3Client.get_object({bucket}/{key}) range: {range} start: {start_time}")
        session = self._app["session"]
        self._renewToken()
        kwargs = self._get_client_kwargs()
        async with session.create_client("s3", **kwargs) as _client:
            try:
                kwargs = {"Bucket": bucket, "Key": key}
                if range:
                    kwargs["Range"] = range
                resp = await _client.get_object(**kwargs)
                data = await resp["Body"].read()
                finish_time = time.time()
                if offset > 0:
                    range_key = f"{key}[{offset}:{offset + length}]"
                else:
                    range_key = key
                msg = f"s3Client.get_object({range_key} bucket={bucket}) "
                msg += f"start={start_time:.4f} finish={finish_time:.4f} "
                msg += f"elapsed={finish_time - start_time:.4f} "
                msg += f"bytes={len(data)}"
                log.info(msg)

                resp["Body"].close()
            except ClientError as ce:
                # key does not exist?
                # check for not found status
                response_code = ce.response["Error"]["Code"]
                if response_code in ("NoSuchKey", "404", 404):
                    msg = f"s3_key: {bucket}/{key} not found "
                    log.info(msg)
                    raise HTTPNotFound()
                elif response_code in ("NoSuchBucket", "PermanentRedirect"):
                    msg = f"s3_bucket: {bucket} not found"
                    log.info(msg)
                    raise HTTPNotFound()
                elif response_code in S3_INVALID_ACCESS_CODES:
                    msg = f"access denied for s3_bucket: {bucket}, response code: {response_code}"
                    log.info(msg)
                    raise HTTPForbidden()
                else:
                    self._s3_stats_increment("error_count")
                    msg = f"got unexpected ClientError on s3 get {bucket}/{key}: "
                    msg += f"{response_code}"
                    log.error(msg)
                    raise HTTPInternalServerError()
            except CancelledError as cle:
                self._s3_stats_increment("error_count")
                msg = f"CancelledError for get s3 obj {bucket}/{key}: {cle}"
                log.error(msg)
                raise HTTPInternalServerError()
            except Exception as e:
                self._s3_stats_increment("error_count")
                msg = f"Unexpected Exception {type(e)} get s3 obj {bucket}/{key}: {e}"
                log.error(msg)
                raise HTTPInternalServerError()
        return data

    async def put_object(self, key, data, bucket=None):
        """Write data to given key.
        Returns client specific dict on success
        """
        if not bucket:
            log.error("put_object - bucket not set")
            raise HTTPInternalServerError()

        # remove s3:// prefix if present
        if bucket.startswith(S3_URI):
            bucket = bucket[len(S3_URI):]

        start_time = time.time()
        log.debug(f"s3Client.put_object({bucket}/{key} start: {start_time}")
        session = self._app["session"]
        self._renewToken()
        kwargs = self._get_client_kwargs()
        async with session.create_client("s3", **kwargs) as _client:
            try:
                kwargs = {"Bucket": bucket, "Key": key, "Body": data}
                rsp = await _client.put_object(**kwargs)
                finish_time = time.time()
                msg = f"s3Client.put_object({key} bucket={bucket}) "
                msg += f"start={start_time:.4f} finish={finish_time:.4f} "
                msg += f"elapsed={finish_time - start_time:.4f} "
                msg += f"bytes={len(data)}"
                log.info(msg)
                s3_rsp = {
                    "etag": rsp["ETag"],
                    "size": len(data),
                    "lastModified": int(finish_time),
                }
            except ClientError as ce:
                response_code = ce.response["Error"]["Code"]
                if response_code in ("NoSuchBucket", "PermanentRedirect"):
                    msg = f"s3_bucket: {bucket} not found"
                    log.warn(msg)
                    raise HTTPNotFound()
                elif response_code in S3_INVALID_ACCESS_CODES:
                    msg = f"access denied for s3_bucket: {bucket}, response_code: {response_code}"
                    log.info(msg)
                    raise HTTPForbidden()
                else:
                    self._s3_stats_increment("error_count")
                    msg = f"Error putting s3 obj {key}: {ce}"
                    log.error(msg)
                    raise HTTPInternalServerError()
            except CancelledError as cle:
                # s3_stats_increment(app, "error_count")
                msg = f"CancelledError for put s3 obj {key}: {cle}"
                log.error(msg)
                raise HTTPInternalServerError()
            except Exception as e:
                # s3_stats_increment(app, "error_count")
                msg = f"Unexpected Exception {type(e)} putting s3 obj "
                msg += f"{key}: {e}"
                log.error(msg)
                raise HTTPInternalServerError()
        if data and len(data) > 0:
            self._s3_stats_increment("bytes_out", inc=len(data))
        log.debug(f"s3Client.put_object {key} complete, s3_rsp: {s3_rsp}")
        return s3_rsp

    async def delete_object(self, key, bucket=None):
        """Deletes the object at the given key"""

        if not bucket:
            log.error("delete_object - bucket not set")
            raise HTTPInternalServerError()

        # remove s3:// prefix if present
        if bucket.startswith(S3_URI):
            bucket = bucket[len(S3_URI):]

        start_time = time.time()
        log.debug(f"s3Client.delete_object({bucket}/{key} start: {start_time}")
        session = self._app["session"]
        self._renewToken()
        kwargs = self._get_client_kwargs()
        async with session.create_client("s3", **kwargs) as _client:
            try:
                await _client.delete_object(Bucket=bucket, Key=key)
                finish_time = time.time()
                msg = f"s3Client.delete_object({key} bucket={bucket}) "
                msg += f"start={start_time:.4f} finish={finish_time:.4f} "
                msg += f"elapsed={finish_time - start_time:.4f}"
                log.info(msg)

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
                msg = f"Unexpected Exception {type(e)} deleting s3 obj "
                msg += f"{key}: {e}"
                log.error(msg)
                raise HTTPInternalServerError()

    async def is_object(self, key, bucket=None):
        """Return true if the given object exists"""
        if not bucket:
            log.error("is_object - bucket not set")
            raise HTTPInternalServerError()

        # remove s3:// prefix if present
        if bucket.startswith(S3_URI):
            bucket = bucket[len(S3_URI):]

        start_time = time.time()
        found = False
        session = self._app["session"]
        self._renewToken()
        kwargs = self._get_client_kwargs()
        async with session.create_client("s3", **kwargs) as _client:
            try:
                head_data = await _client.head_object(Bucket=bucket, Key=key)
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
                msg = f"Unexpected Exception {type(e)} getting head for s3 obj"
                msg += f"{key}: {e}"
                log.error(msg)
                raise HTTPInternalServerError()
        msg = f"s3Client.is_object({key} bucket={bucket}) "
        msg += f"start={start_time:.4f} finish={finish_time:.4f} "
        msg += f"elapsed={finish_time - start_time:.4f}"
        log.info(msg)

        return found

    async def get_key_stats(self, key, bucket=None):
        """Get ETag, size, and last modified time for given object"""

        if not bucket:
            log.error("get_key_stats - bucket not set")
            raise HTTPInternalServerError()

        # remove s3:// prefix if present
        if bucket.startswith(S3_URI):
            bucket = bucket[len(S3_URI):]

        start_time = time.time()
        session = self._app["session"]
        self._renewToken()
        kwargs = self._get_client_kwargs()
        async with session.create_client("s3", **kwargs) as _client:
            try:
                head_data = await _client.head_object(Bucket=bucket, Key=key)
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
                msg = "s3Client.get_key_stats: CancelledError getting head "
                msg += f"for s3 obj {key}: {cle}"
                log.error(msg)
                raise HTTPInternalServerError()
            except Exception as e:
                self._s3_stats_increment("error_count")
                msg = f"s3Client.get_key_stats: Unexpected Exception {type(e)}"
                msg += f" getting head for s3 obj {key}: {e}"
                log.error(msg)
                raise HTTPInternalServerError()

        for head_key in ("ContentLength", "ETag", "LastModified"):
            if head_key not in head_data:
                msg = "s3Client.get_key_stats, expected to find key: "
                msg += f"{head_key} in head_data"
                log.error(msg)
                raise HTTPInternalServerError()

        last_modified_dt = head_data["LastModified"]
        if not isinstance(last_modified_dt, datetime.datetime):
            msg = "S3Client.get_key_stats, expected datetime object "
            msg += "in head data"
            log.error(msg)
            raise HTTPInternalServerError()
        key_stats = {}
        key_stats["Size"] = head_data["ContentLength"]
        key_stats["ETag"] = head_data["ETag"]
        LastModified = datetime.datetime.timestamp(last_modified_dt)
        key_stats["LastModified"] = LastModified
        msg = f"s3Client.get_key_stats({key} bucket={bucket}) "
        msg += f"start={start_time:.4f} finish={finish_time:.4f} "
        msg += f"elapsed={finish_time - start_time:.4f}"
        log.info(msg)

        return key_stats

    def _getPageItems(self, response, items, include_stats=False):
        """internl method for list pagination"""
        log.info("getPageItems")

        if "CommonPrefixes" in response:
            log.debug("got CommonPrefixes in s3 response")
            common = response["CommonPrefixes"]
            for item in common:
                if "Prefix" in item:
                    log.debug(f"got s3 prefix: {item['Prefix']}")
                    items.append(item["Prefix"])

        elif "Contents" in response:
            log.debug("got Contents in s3 response")
            contents = response["Contents"]
            for item in contents:
                key_name = item["Key"]
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
                        LastModified = int(item["LastModified"].timestamp())
                        stats["LastModified"] = LastModified
                    else:
                        log.warn(f"No LastModified for key: {key_name}")
                    log.debug(f"key: {key_name} stats: {stats}")
                    items[key_name] = stats
                else:
                    items.append(key_name)

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
        if not bucket:
            log.error("list_keys - bucket not set")
            raise HTTPInternalServerError()

        # remove s3:// prefix if present
        if bucket.startswith(S3_URI):
            bucket = bucket[len(S3_URI):]

        msg = f"list_keys('{prefix}','{deliminator}','{suffix}', "
        msg += f"include_stats={include_stats}, "
        msg += f"callback {'set' if callback is not None else 'not set'}"
        log.info(msg)
        if deliminator and deliminator != "/":
            msg = "Only '/' is supported as deliminator"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        session = self._app["session"]
        self._renewToken()
        kwargs = self._get_client_kwargs()
        if prefix and prefix[-1] != "/":
            prefix += "/"  # list_v2 requires prefix end with slash
        async with session.create_client("s3", **kwargs) as _client:
            paginator = _client.get_paginator("list_objects_v2")

            # use a dictionary to hold return values if stats are needed
            key_names = {} if include_stats else []
            count = 0

            try:
                async for page in paginator.paginate(
                    PaginationConfig={"PageSize": 1000},
                    Bucket=bucket,
                    Prefix=prefix,
                    Delimiter=deliminator,
                ):
                    assert not asyncio.iscoroutine(page)
                    kwargs = {"include_stats": include_stats}
                    self._getPageItems(page, key_names, **kwargs)
                    count += len(key_names)
                    if callback:
                        if iscoroutinefunction(callback):
                            await callback(self._app, key_names)
                        else:
                            callback(self._app, key_names)
                        key_names = {} if include_stats else []  # reset
                    if limit and count >= limit:
                        log.info(f"list_keys - reached limit {limit}")
                        break
            except ClientError as ce:
                log.warn(f"bucket: {bucket} does not exist, exception: {ce}")
                raise HTTPNotFound()
            except Exception as e:
                log.error(f"s3 paginate got exception {type(e)}: {e}")
                raise HTTPInternalServerError()

        log.info(f"getS3Keys done, got {count} keys")
        if not callback and count != len(key_names):
            msg = f"expected {count} keys in return list "
            msg += f"but got {len(key_names)}"
            log.warning(msg)

        return key_names

    async def releaseClient(self):
        """release the client collection to s3
        (Used for cleanup on application exit)
        """
        log.info("release S3Client")
        await asyncio.sleep(0)  # nothing to do
