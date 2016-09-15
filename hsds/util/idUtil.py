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
import json
import hashlib
import uuid
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
    if obj_type not in ('group', 'dataset', 'namedtype', 'chunk'):
        raise ValueError("unexpected obj_type")
    id = obj_type[0] + '-' + str(uuid.uuid1())
    return id
    
def getHeadNodeS3Key():
    return "headnode"

def validateUuid(id, obj_class=None):
    if not isinstance(id, str):
        raise ValueError("Expected string type")
    if len(id) != 38:  
        # id should be prefix (e.g. "g-") and uuid value
        raise ValueError("Unexpected id length")
    if id[0] not in ('g', 'd', 't', 'c'):
        raise ValueError("Unexpected prefix")
    if id[1] != '-':
        raise ValueError("Unexpected prefix")
    if obj_class is not None:
        if id[0] != obj_class[0].lower():
            raise ValueError("Unexpected prefix for class: " + obj_class)
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
    return id[2:]

def getObjPartition(id, count):
    hash_code = getIdHash(id)
    hash_value = int(hash_code, 16)
    number = hash_value % count
    return number

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
      
    url = dn_urls[node_number]
    log.info("got dn url: {}".format(url))
    return url
  



 
