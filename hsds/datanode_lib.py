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
 
from aiohttp import HttpProcessingError 
from aiohttp.errors import HttpBadRequest
  
from util.idUtil import getObjPartition, getS3Key, isValidUuid
from util.s3Util import getS3JSONObj, putS3JSONObj
import hsds_logger as log
    

async def get_metadata_obj(app, obj_id):
    """ Get object from metadata cache (if present).
        Otherwise fetch from S3 and add to cache
    """
    if not isValidUuid:
        msg = "Invalid obj id: {}".format(obj_id)
        log.error(msg)
        raise HttpBadRequest(message=msg)
    if getObjPartition(obj_id, app['node_count']) != app['node_number']:
        # The request shouldn't have come to this node'
        log.error("wrong node for 'id':{}".format(obj_id))
        raise HttpProcessingError(code=500, message="Unexpected Error") 
    deleted_ids = app['deleted_ids']
    if obj_id in deleted_ids:
        msg = "{} has been deleted".format(obj_id)
        log.warn(msg)
        raise HttpProcessingError(code=410, message="Object has been deleted") 
    
    meta_cache = app['meta_cache'] 
    obj_json = None 
    if obj_id in meta_cache:
        log.info("{} found in meta cache".format(obj_id))
        obj_json = meta_cache[obj_id]
    else:
       
        s3_key = getS3Key(obj_id)
        log.info("getS3JSONObj({})".format(s3_key))
        # read S3 object as JSON
        obj_json = await getS3JSONObj(app, s3_key)
         
        meta_cache[obj_id] = obj_json  # add to cache
    return obj_json

async def save_metadata_obj(app, obj_json):
    if not isinstance(obj_json, dict):
        log.error("Passed non-dict obj to save_metadata_obj")
        raise HttpProcessingError(code=500, message="Unexpected Error") 
    if "id" not in obj_json:
        log.error("No id key found for json object")
        raise HttpProcessingError(code=500, message="Unexpected Error")
    obj_id = obj_json["id"]
    if not isValidUuid(obj_id):
        log.error("Invalid obj id: {}".format(obj_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")
    
    if getObjPartition(obj_id, app['node_count']) != app['node_number']:
        # The request shouldn't have come to this node'
        log.error("wrong node for 'id':{}".format(obj_id))
        raise HttpProcessingError(code=500, message="Unexpected Error") 
    deleted_ids = app['deleted_ids']
    if obj_id in deleted_ids:
        log.warn("{} has been deleted".format(obj_id))
        raise HttpProcessingError(code=500, message="Unexpected Error") 
    s3_key = getS3Key(obj_id)
    
    # write back to S3    
    await putS3JSONObj(app, s3_key, obj_json) 
    
    # update meta cache
    meta_cache = app['meta_cache'] 
    log.info("save: {} to cache: {}".format(obj_id, str(obj_json)))
    meta_cache[obj_id] = obj_json
     
    

    
 
   