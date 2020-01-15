import datetime
import time
from azure.storage.blob.aio import BlobServiceClient

from aiohttp.web_exceptions import HTTPInternalServerError
# from aiohttp.web_exceptions import HTTPNotFound, HTTPForbidden
import hsds_logger as log
import config

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
                    self._client = app["s3"]
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

        self._client = BlobServiceClient.from_connection_string(conn_str=azure_connection_string)

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

    async def get_object(self, key, bucket=None, range=''):
        """ Return data for object at given key.
           If Range is set, return the given byte range.
        """
        if not bucket:
            log.error("get_object - bucket not set")
            raise HTTPInternalServerError()
        data = None

        start_time = time.time()
        log.debug(f"azureBlobClient.get_object({bucket}/{key} start: {start_time}")
        try:
            # TBD - read blob
            finish_time = time.time()
            log.info(f"azureBlobClient.get_object({key} bucket={bucket}) start={start_time:.4f} finish={finish_time:.4f} elapsed={finish_time-start_time:.4f} bytes={len(data)}")
        except Exception as e:
            # TBD: catch specific exceptions
            self._azure_stats_increment("error_count")
            msg = f"Unexpected Exception {type(e)} putting Azure obj {key}: {e}"
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
        log.debug(f"azureBlobClient.put_object({bucket}/{key} start: {start_time}")
        try:
            # TBD - write data to given blob
            finish_time = time.time()
            # TBD: get actual response values
            rsp = {"ETag": "fixme", "size": 999, "lastModified": finish_time }

            log.info(f"azureBlobClient.put_object({key} bucket={bucket}) start={start_time:.4f} finish={finish_time:.4f} elapsed={finish_time-start_time:.4f} bytes={len(data)}")
            azure_rsp = {"etag": rsp["ETag"], "size": len(data), "lastModified": int(finish_time)}

        except Exception as e:
            #s3_stats_increment(app, "error_count")
            msg = f"Unexpected Exception {type(e)} putting azure obj to {key}: {e}"
            log.error(msg)
            raise HTTPInternalServerError()
        if data and len(data) > 0:
            self._azure_stats_increment("bytes_out", inc=len(data))
        log.debug(f"azureBlobClient.put_object {key} complete, azure_rsp: {azure_rsp}")
        return azure_rsp

    async def delete_object(self, key, bucket=None):
        """ Deletes the object at the given key
        """
        if not bucket:
            log.error("delete_object - bucket not set")
            raise HTTPInternalServerError()

        start_time = time.time()
        log.debug(f"azureBlobClient.delete_object({bucket}/{key} start: {start_time}")
        try:
            # TBD: delete Azure blob
            pass

        except Exception as e:
            self._azure_stats_increment("error_count")
            msg = f"Unexpected Exception {type(e)} putting Azure obj {key}: {e}"
            log.error(msg)
            raise HTTPInternalServerError()


    async def list_keys(self, prefix='', deliminator='', suffix='', include_stats=False, callback=None, bucket=None, limit=None):
        """ return keys matching the arguments
        """
        if not bucket:
            log.error("putt_object - bucket not set")
            raise HTTPInternalServerError()
        log.info(f"list_keys('{prefix}','{deliminator}','{suffix}', include_stats={include_stats}")
        if include_stats:
            # use a dictionary to hold return values
            key_names = {}
        else:
            # just use a list
            key_names = []

        try:
            # TBD: get keys
            pass

        except Exception as e:
             log.error(f"azureBlobClient paginate got exception {type(e)}: {e}")
             raise HTTPInternalServerError()

        log.info(f"list_keys done, got {len(key_names)} keys")

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
