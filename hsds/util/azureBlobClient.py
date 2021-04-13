from  inspect import iscoroutinefunction
from asyncio import CancelledError
import datetime
import time
from .. import hsds_logger as log
try:
    from azure.storage.blob.aio import BlobServiceClient
    from azure.storage.blob import BlobPrefix
    from azure.core.exceptions import AzureError
except ImportError:
    log.warning("unable to import Azure blob packages")
except ModuleNotFoundError:
    log.warning("unable to import Azure blob packages")
from aiohttp.web_exceptions import HTTPNotFound, HTTPForbidden, HTTPInternalServerError, HTTPBadRequest
from .. import config

CALLBACK_MAX_COUNT=1000 # compatible with S3 batch size

class AzureBlobClient():
    """
     Utility class for reading and storing data to AzureStorage Blobs
    """

    def __init__(self, app):

        self._app = app

        if "azureBlobClient" in app:
            if "token_expiration" in app:
                # TBD - does this apply for Azure?
                # check that our token is not about to expire
                expiration = app["token_expiration"]
                now = datetime.datetime.now()
                delta = expiration - now
                if delta.total_seconds() > 10:
                    self._client = app["azureBlobClient"]
                    return
                # otherwise, fall through and get a new token
                log.info("Azure access token has expired - renewing")
            else:
                self._client = app["azureBlobClient"]
                return

        # first time setup of Azure client or limited time token has expired

        # TBD - what do do about region?
        log.info("AzureBlobClient init")

        azure_connection_string = config.get('azure_connection_string')
        if not azure_connection_string:
            msg="No connection string specified"
            log.error(msg)
            raise ValueError(msg)
        log.info(f"Using azure_connection_string: {azure_connection_string}")

        self._client = BlobServiceClient.from_connection_string(azure_connection_string)

        app['azureBlobClient'] = self._client  # save so same client can be returned in subsequent calls

    def _azure_stats_increment(self, counter, inc=1):
        """ Incremenet the indicated connter
        """
        if "azure_stats" not in self._app:
            # setup stats
            azure_stats = {}
            azure_stats["get_count"] = 0
            azure_stats["put_count"] = 0
            azure_stats["delete_count"] = 0
            azure_stats["list_count"] = 0
            azure_stats["error_count"] = 0
            azure_stats["bytes_in"] = 0
            azure_stats["bytes_out"] = 0
            self._app["azure_stats"] = azure_stats
        azure_stats = self._app['azure_stats']
        if counter not in azure_stats:
            log.error(f"unexpected counter for azure_stats: {counter}")
            return
        if inc < 1:
            log.error(f"unexpected inc for azure_stats: {inc}")
            return

        azure_stats[counter] += inc

    async def get_object(self, key, bucket=None, offset=0, length=-1):
        """ Return data for object at given key.
           If Range is set, return the given byte range.
        """
        if not bucket:
            log.error("get_object - bucket not set")
            raise HTTPInternalServerError()

        if length > 0:
            log.info(f"storage range request -- offset: {offset} length: {length}")
        else:
            offset = None
            length = None

        start_time = time.time()
        log.debug(f"azureBlobClient.get_object({bucket}/{key} start: {start_time}")
        try:
            async with self._client.get_blob_client(container=bucket, blob=key) as blob_client:
                blob_rsp = await blob_client.download_blob(offset=offset, length=length)
            data = await blob_rsp.content_as_bytes()
            finish_time = time.time()
            log.info(f"azureBlobClient.get_object({key} bucket={bucket}) start={start_time:.4f} finish={finish_time:.4f} elapsed={finish_time-start_time:.4f} bytes={len(data)}")
        except CancelledError as cle:
            self._azure_stats_increment("error_count")
            msg = f"azureBlobClient.CancelledError getting get_object {key}: {cle}"
            log.error(msg)
            raise HTTPInternalServerError()
        except Exception as e:
            if isinstance(e, AzureError):
                if e.status_code == 404:
                    msg = f"storage key: {key} not found "
                    log.warn(msg)
                    raise HTTPNotFound()
                elif e.status_code in (401, 403):
                    msg = f"azureBlobClient.access denied for get key: {key}"
                    log.info(msg)
                    raise HTTPForbidden()
                else:
                    self._azure_stats_increment("error_count")
                    log.error(f"azureBlobClient.got unexpected AzureError for get_object {key}: {e.message}")
                    raise HTTPInternalServerError()
            else:
                log.error(f"azureBlobClient.Unexpected exception for get_object {key}: {e}")
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
        log.debug(f"azureBlobClient.put_object({bucket}/{key} start: {start_time}")
        try:
            async with self._client.get_blob_client(container=bucket, blob=key) as blob_client:
                blob_rsp = await blob_client.upload_blob(data, blob_type='BlockBlob', overwrite=True)

            finish_time = time.time()
            ETag = blob_rsp["etag"]
            lastModified = int(blob_rsp["last_modified"].timestamp())
            data_size = len(data)
            rsp = {"ETag": ETag, "size": data_size, "LastModified": lastModified }
            log.debug(f"put_object {key} returning: {rsp}")

            log.info(f"azureBlobClient.put_object({key} bucket={bucket}) start={start_time:.4f} finish={finish_time:.4f} elapsed={finish_time-start_time:.4f} bytes={len(data)}")

        except CancelledError as cle:
            self._azure_stats_increment("error_count")
            msg = f"azureBlobClient.CancelledError for put_object {key}: {cle}"
            log.error(msg)
            raise HTTPInternalServerError()
        except Exception as e:
            if isinstance(e, AzureError):
                if e.status_code == 404:
                    msg = f"azureBlobClient.key: {key} not found "
                    log.warn(msg)
                    raise HTTPNotFound()
                elif e.status_code in (401, 403):
                    msg = f"azureBlobClient.access denied for get key: {key}"
                    log.info(msg)
                    raise HTTPForbidden()
                else:
                    self._azure_stats_increment("error_count")
                    log.error(f"azureBlobClient.got unexpected AzureError for get_object {key}: {e.message}")
                    raise HTTPInternalServerError()
            else:
                log.error(f"azureBlobClient.Unexpected exception for put_object {key}: {e}")
                raise HTTPInternalServerError()

        if data and len(data) > 0:
            self._azure_stats_increment("bytes_out", inc=len(data))
        log.debug(f"azureBlobClient.put_object {key} complete, rsp: {rsp}")
        return rsp

    async def delete_object(self, key, bucket=None):
        """ Deletes the object at the given key
        """
        if not bucket:
            log.error("delete_object - bucket not set")
            raise HTTPInternalServerError()

        start_time = time.time()
        log.debug(f"azureBlobClient.delete_object({bucket}/{key} start: {start_time}")
        try:
            async with self._client.get_container_client(container=bucket) as container_client:
                await container_client.delete_blob(blob=key)
            finish_time = time.time()
            log.info(f"azureBlobClient.delete_object({key} bucket={bucket}) start={start_time:.4f} finish={finish_time:.4f} elapsed={finish_time-start_time:.4f}")

        except CancelledError as cle:
            self._azure_stats_increment("error_count")
            msg = f"azureBlobClient.CancelledError for delete_object {key}: {cle}"
            log.error(msg)
            raise HTTPInternalServerError()
        except Exception as e:
            if isinstance(e, AzureError):
                if e.status_code == 404:
                    msg = f"azureBlobClient.key: {key} not found "
                    log.warn(msg)
                    raise HTTPNotFound()
                elif e.status_code in (401, 403):
                    msg = f"azureBlobClient.access denied for delete key: {key}"
                    log.info(msg)
                    raise HTTPForbidden()
                else:
                    self._azure_stats_increment("error_count")
                    log.error(f"azureBlobClient.got unexpected AzureError for delete_object {key}: {e.message}")
                    raise HTTPInternalServerError()
            else:
                log.error(f"azureBlobClient.Unexpected exception for put_object {key}: {e}")
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
            async with self._client.get_blob_client(container=bucket, blob=key) as blob_client:
                blob_props = await blob_client.get_blob_properties()
            if blob_props:
                found = True
            finish_time = time.time()

        except CancelledError as cle:
            self._azure_stats_increment("error_count")
            msg = f"azureBlobClient.CancelledError get_blob_properties {key}: {cle}"
            log.error(msg)
            raise HTTPInternalServerError()
        except Exception as e:
            if isinstance(e, AzureError):
                if e.status_code == 404:
                    msg = f"storage key: {key} not found "
                    log.warn(msg)
                    finish_time = time.time()
                elif e.status_code in (401, 403):
                    msg = f"azureBlobClient.access denied for get_blob_properties key: {key}"
                    log.info(msg)
                    raise HTTPForbidden()
                else:
                    self._azure_stats_increment("error_count")
                    log.error(f"azureBlobClient.got unexpected AzureError for get_blob_properties {key}: {e.message}")
                    raise HTTPInternalServerError()
            else:
                log.error(f"azureBlobClient.Unexpected exception for get_blob_properties {key}: {e}")
                raise HTTPInternalServerError()
        log.info(f"azureBlobClient.is_object({key} bucket={bucket}) start={start_time:.4f} finish={finish_time:.4f} elapsed={finish_time-start_time:.4f}")

        return found

    async def get_key_stats(self, key, bucket=None):
        """ Get ETag, size, and last modified time for given objecct
        """
        start_time = time.time()
        key_stats = {}
        try:
            async with self._client.get_blob_client(container=bucket, blob=key) as blob_client:
                blob_props = await blob_client.get_blob_properties()
            finish_time = time.time()

        except CancelledError as cle:
            self._azure_stats_increment("error_count")
            msg = f"azureBlobClient.CancelledError get_blob_properties {key}: {cle}"
            log.error(msg)
            raise HTTPInternalServerError()
        except Exception as e:
            if isinstance(e, AzureError):
                if e.status_code == 404:
                    msg = f"storage key: {key} not found "
                    log.warn(msg)
                    raise HTTPNotFound()
                elif e.status_code in (401, 403):
                    msg = f"azureBlobClient.access denied for get_blob_properties key: {key}"
                    log.info(msg)
                    raise HTTPForbidden()
                else:
                    self._azure_stats_increment("error_count")
                    log.error(f"azureBlobClient.got unexpected AzureError for get_blob_properties {key}: {e.message}")
                    raise HTTPInternalServerError()
            else:
                log.error(f"azureBlobClient.Unexpected exception for get_blob_properties {key}: {e}")
                raise HTTPInternalServerError()

        last_modified_dt = blob_props.last_modified
        if not isinstance(last_modified_dt, datetime.datetime):
            msg ="azureBlobClient.get_key_stats, expected datetime object in head data"
            log.error(msg)
            raise HTTPInternalServerError()
        key_stats = {}
        key_stats["Size"] = blob_props.size
        key_stats["ETag"] = blob_props.etag
        key_stats["LastModified"] = datetime.datetime.timestamp(last_modified_dt)
        log.info(f"azureBlobClient.get_key_stats({key} bucket={bucket}) start={start_time:.4f} finish={finish_time:.4f} elapsed={finish_time-start_time:.4f}")

        return key_stats

    async def walk_blobs(self, container_client, prefix="", suffix="", include_stats=False, deliminator='/', callback=None):
        continuation_token = None
        count = 0
        while True:
            keyList = container_client.walk_blobs(name_starts_with=prefix, delimiter=deliminator, results_per_page=CALLBACK_MAX_COUNT).by_page(continuation_token)
            key_names = {} if include_stats else []
            async for key in await keyList.__anext__():
                key_name = key["name"]
                log.debug(f"walk_blobs got: {key_name}")
                if include_stats:
                    ETag = key["etag"]
                    lastModified = int(key["last_modified"].timestamp())
                    data_size = key["size"]
                    key_names[key_name] = {"ETag": ETag, "Size": data_size, "LastModified": lastModified }
                else:
                    if suffix and not key_name.endswith(suffix):
                        log.debug(f"skip name that doesn't end with {suffix}")
                        continue
                    if deliminator and key_name[-1] != '/':
                        log.debug("skip name thaat doesn't end in '/'")
                        # only return folders
                        continue
                    if len(key_names) >= CALLBACK_MAX_COUNT:
                        break
                    key_names.append(key_name)
                    count += 1
            if callback:
                if iscoroutinefunction(callback):
                    await callback(self._app, key_names)
                else:
                    callback(self._app, key_names)
                key_names = {} if include_stats else []
            if not keyList.continuation_token or len(key_names) >= CALLBACK_MAX_COUNT:
                # got all the keys (or as many as requested)
                log.debug("walk_blbs complete")
                break
            else:
                # keep going
                continuation_token = keyList.continuation_token
        log.info(f"walk_blob_hierarchy, returning {count} items")
        if not callback and count != len(key_names):
            log.warning(f"expected {count} keys in return list but got {len(key_names)}")
        return key_names    

    async def walk_blob_hierarchy(self, container_client, prefix="", include_stats=False, callback=None):
        log.info(f"walk_blob_hierarchy, prefix: {prefix}")
        
        key_names = None

        async def do_callback(callback, keynames):
            if iscoroutinefunction(callback):
                await callback(self._app, key_names)
            else:
                callback(self._app, key_names)

        key_names = key_names = {} if include_stats else []
        count = 0
        async for item in container_client.walk_blobs(name_starts_with=prefix):
            short_name = item.name[len(prefix):]
            if isinstance(item, BlobPrefix):
                log.debug(f"walk_blob_hierarchy - BlobPrefix: {short_name}")
                key_names = await self.walk_blob_hierarchy(container_client, prefix=item.name, include_stats=include_stats, callback=callback)
            else:
                async for item in container_client.list_blobs(name_starts_with=item.name): 
                    key_name = item['name']
                    log.debug(f"walk_blob_hierarchy - got name: {key_name}")
                    if include_stats:
                        ETag = item["etag"]
                        lastModified = int(item["last_modified"].timestamp())
                        data_size = item["size"]
                        key_names[key_name] = {"ETag": ETag, "Size": data_size, "LastModified": lastModified }
                    else:
                        # just add the blob name to the list
                        key_names.append(item['name'])
                    count += 1
                    if callback and len(key_names) >= CALLBACK_MAX_COUNT:
                        log.debug(f"walk_blob_hierarchy, invoking callback with {len(key_names)} items")
                        await do_callback(callback, key_names)
                        key_names = key_names = {} if include_stats else []
            if callback:
                log.debug(f"walk_blob_hierarchy, invoking callback with {len(key_names)} items")
                await do_callback(callback, key_names)
                key_names = {} if include_stats else []
                    
        log.info(f"walk_blob_hierarchy, returning {count} items")
        if not callback and count != len(key_names):
            log.warning(f"expected {count} keys in return list but got {len(key_names)}")

        return key_names

    async def list_keys(self, prefix='', deliminator='', suffix='', include_stats=False, callback=None, bucket=None, limit=None):
        """ return keys matching the arguments
        """
        if not bucket:
            log.error("list_keys - bucket not set")
            raise HTTPInternalServerError()

        log.info(f"list_keys('{prefix}','{deliminator}','{suffix}', include_stats={include_stats}, callback {'set' if callback is not None else 'not set'}")
        if deliminator and deliminator != '/':
            msg = "Only '/' is supported as deliminator"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        key_names = None
        
        if prefix == '':
            prefix = None  # azure sdk expects None for no prefix
        
        try:
            async with self._client.get_container_client(container=bucket) as container_client:
                key_names = await self.walk_blobs(container_client, prefix=prefix, deliminator=deliminator, include_stats=include_stats, callback=callback)
                # key_names = await self.walk_blob_hierarchy(container_client, prefix=prefix, include_stats=include_stats, callback=callback)
        except CancelledError as cle:
            self._azure_stats_increment("error_count")
            msg = f"azureBlobClient.CancelledError for list_keys: {cle}"
            log.error(msg)
            raise HTTPInternalServerError()
        except Exception as e:
            if isinstance(e, AzureError):
                if e.status_code == 404:
                    msg = "azureBlobClient not found error for list_keys"
                    log.warn(msg)
                    raise HTTPNotFound()
                elif e.status_code in (401, 403):
                    msg = "azureBlobClient.access denied for list_keys"
                    log.info(msg)
                    raise HTTPForbidden()
                else:
                    self._azure_stats_increment("error_count")
                    log.error(f"azureBlobClient.got unexpected AzureError for list_keys: {e.message}")
                    raise HTTPInternalServerError()
            else:
                log.error(f"azureBlobClient.Unexpected exception for list_keys: {e}")
                raise HTTPInternalServerError()

        log.info(f"list_keys done, got {len(key_names)} keys")
        if limit and len(key_names) > limit:
            # return requested number of keys
            if include_stats:
                keys = list(key_names.keys())
                keys.sort()
                for k in keys[limit:]:
                    del key_names[k]
            else:
                key_names = key_names[:limit]

        return key_names

    async def releaseClient(self):
        """ release the client collection to Azure Blob Storage
           (Used for cleanup on application exit)
        """
        log.info("release AzureBlobClient")
        if 'azureBlobClient' in self._app:
            client = self._app['azureBlobClient']
            await client.close()
            del self._app['azureBlobClient']
