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
# data node of hsds cluster
# 
import asyncio

from aiohttp.web import run_app
import config
from util.lruCache import LruCache
from basenode import healthCheck, baseInit
import hsds_logger as log
from domain_dn import GET_Domain, PUT_Domain, DELETE_Domain, PUT_ACL
from group_dn import GET_Group, POST_Group, DELETE_Group, PUT_Group
from link_dn import GET_Links, GET_Link, PUT_Link, DELETE_Link
from attr_dn import GET_Attributes, GET_Attribute, PUT_Attribute, DELETE_Attribute
from ctype_dn import GET_Datatype, POST_Datatype, DELETE_Datatype
from dset_dn import GET_Dataset, POST_Dataset, DELETE_Dataset, PUT_DatasetShape
from chunk_dn import PUT_Chunk, GET_Chunk, POST_Chunk, DELETE_Chunk
from datanode_lib import s3syncCheck 

               

async def init(loop):
    """Intitialize application and return app object"""
    app = baseInit(loop, 'dn')

    #
    # call app.router.add_get() here to add node-specific routes
    #
    app.router.add_route('GET', '/domains', GET_Domain)
    app.router.add_route('PUT', '/domains', PUT_Domain)
    app.router.add_route('DELETE', '/domains', DELETE_Domain)
    app.router.add_route('PUT', '/acls/{username}', PUT_ACL)
    app.router.add_route('GET', '/groups/{id}', GET_Group)
    app.router.add_route('DELETE', '/groups/{id}', DELETE_Group)
    app.router.add_route('PUT', '/groups/{id}', PUT_Group)
    app.router.add_route('POST', '/groups', POST_Group)
    app.router.add_route('GET', '/groups/{id}/links', GET_Links)
    app.router.add_route('GET', '/groups/{id}/links/{title}', GET_Link)
    app.router.add_route('DELETE', '/groups/{id}/links/{title}', DELETE_Link)
    app.router.add_route('PUT', '/groups/{id}/links/{title}', PUT_Link)
    app.router.add_route('GET', '/groups/{id}/attributes', GET_Attributes)
    app.router.add_route('GET', '/groups/{id}/attributes/{name}', GET_Attribute)
    app.router.add_route('DELETE', '/groups/{id}/attributes/{name}', DELETE_Attribute)
    app.router.add_route('PUT', '/groups/{id}/attributes/{name}', PUT_Attribute)
    app.router.add_route('GET', '/datatypes/{id}', GET_Datatype)
    app.router.add_route('DELETE', '/datatypes/{id}', DELETE_Datatype)
    app.router.add_route('POST', '/datatypes', POST_Datatype)
    app.router.add_route('GET', '/datatypes/{id}/attributes', GET_Attributes)
    app.router.add_route('GET', '/datatypes/{id}/attributes/{name}', GET_Attribute)
    app.router.add_route('DELETE', '/datatypes/{id}/attributes/{name}', DELETE_Attribute)
    app.router.add_route('PUT', '/datatypes/{id}/attributes/{name}', PUT_Attribute)
    app.router.add_route('GET', '/datasets/{id}', GET_Dataset)
    app.router.add_route('DELETE', '/datasets/{id}', DELETE_Dataset)
    app.router.add_route('POST', '/datasets', POST_Dataset)
    app.router.add_route('PUT', '/datasets/{id}/shape', PUT_DatasetShape)
    app.router.add_route('GET', '/datasets/{id}/attributes', GET_Attributes)
    app.router.add_route('GET', '/datasets/{id}/attributes/{name}', GET_Attribute)
    app.router.add_route('DELETE', '/datasets/{id}/attributes/{name}', DELETE_Attribute)
    app.router.add_route('PUT', '/datasets/{id}/attributes/{name}', PUT_Attribute)
    app.router.add_route('PUT', '/chunks/{id}', PUT_Chunk)
    app.router.add_route('GET', '/chunks/{id}', GET_Chunk)
    app.router.add_route('POST', '/chunks/{id}', POST_Chunk)
    app.router.add_route('DELETE', '/chunks/{id}', DELETE_Chunk)
    
      
    return app

#
# Main
#

if __name__ == '__main__':
    log.info("datanode start")
    loop = asyncio.get_event_loop()

    metadata_mem_cache_size = int(config.get("metadata_mem_cache_size"))
    log.info("Using metadata memory cache size of: {}".format(metadata_mem_cache_size))
    chunk_mem_cache_size = int(config.get("chunk_mem_cache_size"))
    log.info("Using chunk memory cache size of: {}".format(chunk_mem_cache_size))

    #create the app object
    app = loop.run_until_complete(init(loop))
    app['meta_cache'] = LruCache(mem_target=metadata_mem_cache_size, chunk_cache=False)
    app['chunk_cache'] = LruCache(mem_target=chunk_mem_cache_size, chunk_cache=True)
    app['deleted_ids'] = set()
    app['dirty_ids'] = {}  # map of objids to timestamp of which they were last updated
    app['deflate_map'] = {} # map of dataset ids to deflate levels (if compressed)
    app["pending_s3_read"] = {} # map of s3key to timestamp for in-flight read requests
    app["pending_s3_write"] = {} # map of s3key to timestamp for in-flight write requests
    app["pending_s3_write_tasks"] = {} # map of objid to asyncio Task objects for writes
    app["an_notify_objs"] = set()   # set of objids to tell the AN about
    # TODO - there's nothing to prevent the deflate_map from getting ever larger 
    # (though it is only one int per dataset id)
    # add a timestamp and remove at a certain time?
    # delete entire map whenver the synch queue is empty?
    
    # run background tasks
    asyncio.ensure_future(healthCheck(app), loop=loop)

    # run data sync tasks
    asyncio.ensure_future(s3syncCheck(app), loop=loop)
   
    # run the app
    port = int(config.get("dn_port"))
    run_app(app, port=port)
