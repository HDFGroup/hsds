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
import sys
import os
from collections import namedtuple
from aiobotocore import get_session
from aiohttp.client_exceptions import ClientOSError
from aiohttp.web import StreamResponse
from aiohttp.http_writer import StreamWriter

if "CONFIG_DIR" not in os.environ:
    os.environ["CONFIG_DIR"] = "../admin/config/"
from hsds.util.storUtil import releaseStorageClient
from hsds.util.idUtil import isValidChunkId
from hsds.util.lruCache import LruCache
from hsds.chunk_dn import GET_Chunk
from hsds import config

# This is a utility to dump a JSON obj (group, dataset, ctype) given the
# the objects UUID

# Note - this is not quite working for streaming responses

HttpVersion = namedtuple('HttpVersion', ['major', 'minor'])
HttpVersion11 = HttpVersion(1, 1)
 
class Request:
    # Shim class to replace aiohttp request
    def __init__(self, app, path, method='GET', params={}):
        RelURL = namedtuple('RelURL', 'query')
        self._app = app
        self._method = method
        self._headers = {}
        self._path = path
        self._match_info = params
        self._rel_url = RelURL(params)
        self._payload_writer = None

    @property
    def app(self):
        return self._app

    @property
    def method(self):
        return self._method

    @property
    def headers(self):
        return self._headers

    @property
    def path(self):
        return self._path

    @property
    def match_info(self):
        return self._match_info

    @property
    def rel_url(self):
        return self._rel_url

    @property
    def keep_alive(self):
        """Is keepalive enabled by client?"""
        return False

    @property
    def version(self):
        # HTTP Version
        return HttpVersion11

    async def _prepare_hook(self, response: StreamResponse) -> None:
        return




#
# Print usage and exit
#
def printUsage():
    print("usage: python get_s3json [--bucket_name=<bucket>] [--aws_s3_gateway=<s3_endpoint>] objid ")
    print("  objid: s3 JSON obj to fetch")
    print("  Example: python get_chunk_values.py --aws_s3_gateway=http://192.168.99.100:9000 --bucket_name=hsdsdev --domain=/shared/tall.h5 c-75b88d05-46db146e-a19a-7a403e-f6d7a8_0_0")
    sys.exit()

async def printChunkValues(app, domain, chunk_id):

    params = {"domain": domain, "id": chunk_id}
    path=f"/chunks/{chunk_id}"
    request = Request(app, path, params=params)
    try:
        resp = await GET_Chunk(request)
        print("got resp:", resp)
        # print(json.dumps(json_obj, sort_keys=True, indent=4))
    except ValueError as ve:
        print(f"Got ValueError exception: {ve}")
    except ClientOSError as coe:
        print(f"Got error: {coe}")
    await releaseStorageClient(app)


def main():
    if len(sys.argv) == 1 or sys.argv[1] == "-h" or sys.argv[1] == "--help":
        printUsage()
        sys.exit(1)

    for arg in sys.argv:
        if arg.startswith("--domain="):
            domain = arg[len("--domain="):]
            print("got domain:", domain)

    if not domain:
        printUsage()
        sys.exit(-1)

    chunk_id = sys.argv[-1]
    if not isValidChunkId(chunk_id):
        print("Invalid chunk id")
        sys.exit(1)

    # we need to setup a asyncio loop to query s3
    loop = asyncio.get_event_loop()
    session = get_session()

    app = {}
    app["session"] = session
    app['bucket_name'] = config.get("bucket_name")
    app['node_count'] = 1
    app['node_number'] = 0  
    app['deleted_ids'] = set()
    app['meta_cache'] = {}
    app['pending_s3_read'] = {}
    app['meta_cache'] = LruCache(mem_target=1024*1024, chunk_cache=False)
    app['chunk_cache'] = LruCache(mem_target=64*1024*1024, chunk_cache=True)

   
    print("got domain:", domain)

    loop.run_until_complete(printChunkValues(app, domain, chunk_id))

    loop.close()


main()
