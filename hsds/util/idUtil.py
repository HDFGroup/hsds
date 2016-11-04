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
# idUtil:
# id (uuid) related functions
# 
import hashlib
import uuid
from aiohttp import HttpProcessingError
import hsds_logger as log

def getIdHash(id):
    """  Return md5 prefix based on id value"""
    m = hashlib.new('md5')
    m.update(id.encode('utf8'))
    hexdigest = m.hexdigest()
    return hexdigest[:5]

def getS3Key(id):
    """ Return s3 key based on uuid and class char.
    Add a md5 prefix in front of the returned key to better 
    distribute S3 objects"""
    idhash = getIdHash(id)
    key = "{}-{}".format(idhash, id)
    return key

def createNodeId(prefix):
    """ Create a random id used to identify nodes"""
    node_uuid = str(uuid.uuid1())
    idhash = getIdHash(node_uuid)
    key = prefix + "-" + idhash
    return key

def createObjId(obj_type):
    if obj_type not in ('groups', 'datasets', 'datatypes', 'chunks'):
        raise ValueError("unexpected obj_type")
    prefix = None
    if obj_type == 'datatypes':
        prefix = 't'
    else:
        prefix = obj_type[0]
    id = prefix + '-' + str(uuid.uuid1())
    return id

def getCollectionForId(obj_id):
    if not isinstance(obj_id, str):
        raise ValueError("invalid object id")
    collection = None
    if obj_id.startswith("g-"):
        collection = "groups"
    elif obj_id.startswith("d-"):
        collection = "datasets"
    elif obj_id.startswith("t-"):
        collection = "datatypes"
    else:
        raise ValueError("not a collection id")
    return collection

    
def getHeadNodeS3Key():
    return "headnode"

def validateUuid(id, obj_class=None):
    if not isinstance(id, str):
        raise ValueError("Expected string type")
    if len(id) < 38:  
        # id should be prefix (e.g. "g-") and uuid value
        raise ValueError("Unexpected id length")
    if id[0] not in ('g', 'd', 't', 'c'):
        raise ValueError("Unexpected prefix")
    if id[1] != '-':
        raise ValueError("Unexpected prefix")
    if obj_class is not None:
        obj_class = obj_class.lower()
        prefix = obj_class[0]
        if obj_class.startswith("datatype"):
            prefix = 't'
        if id[0] != prefix:
            raise ValueError("Unexpected prefix for class: " + obj_class)
    if id[0] == 'c':
        # trim the chunk index for chunk ids
        index = id.find('_')
        if index == -1:
            raise ValueError("Invalid chunk id")
        id = id[:index]
    if len(id) != 38:
        # id should be 38 now
        raise ValueError("Unexpected id length")

    for ch in id:
        if ch.isalnum():
            continue
        if ch == '-':
            continue
        raise ValueError("Unexpected character in uuid: " + ch)

def isValidUuid(id, obj_class=None):
    try:
        validateUuid(id, obj_class)
        return True
    except ValueError:
        return False

def getUuidFromId(id):
    """ strip off the type prefix ('g-' or 'd-', or 't-')
    and return the uuid part """
    return id[2:]

def getObjPartition(id, count):
    """ Get the id of the dn node that should be handling the given obj id
    """
    hash_code = getIdHash(id)
    hash_value = int(hash_code, 16)
    number = hash_value % count
    return number

def validateInPartition(app, obj_id):
    if getObjPartition(obj_id, app['node_count']) != app['node_number']:
        # The request shouldn't have come to this node'
        expected_node = getObjPartition(obj_id, app['node_count'])
        msg = "wrong node for 'id':{}, expected node {} got {}".format(obj_id, expected_node, app['node_number'])
        log.error(msg)
        raise HttpProcessingError(message=msg, code=500)

def getDataNodeUrl(app, obj_id):
    """ Return host/port for datanode for given obj_id.
    Throw exception if service is not ready"""
    dn_urls = app["dn_urls"]
    node_number = app["node_number"]
    if app["node_state"] != "READY" or node_number not in dn_urls:
        log.info("Node_state:".format(app["node_state"]))
        log.info("node_number:".format(node_number))
        msg="Service not ready"
        log.warn(msg)
        raise HttpProcessingError(message=msg, code=503)
    dn_number = getObjPartition(obj_id, app['node_count'])
      
    url = dn_urls[dn_number]
    log.info("got dn url: {}".format(url))
    return url


  



 
