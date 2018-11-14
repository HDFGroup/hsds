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
from aiohttp.web_exceptions import HTTPServiceUnavailable

import hsds_logger as log

def getIdHash(id):
    """  Return md5 prefix based on id value"""
    m = hashlib.new('md5')
    m.update(id.encode('utf8'))
    hexdigest = m.hexdigest()
    return hexdigest[:5]

def isSchema2Id(id):
    """ return true if this is a v2 id """
    # v1 ids are in the standard UUID format: 8-4-4-4-12
    # v2 ids are in the non-standard: 8-8-4-6-6
    parts = id.split('-')
    if len(parts) != 6:
        raise ValueError(f"Unexpected id formation for uuid: {id}")
    if len(parts[2]) == 8:
        return True
    else:
        return False

def getIdHexChars(id):
    """ get the hex chars of the given id """
    if id[0] == 'c':
        # don't include chunk index
        index = id.index('_')
        parts = id[0:index].split('-')
    else:
        parts = id.split('-')
    if len(parts) != 6:
        raise ValueError(f"Unexpected id format for uuid: {id}")
    return "".join(parts[1:]) 

def hexRot(ch):
    """ rotate hex character by 8 """
    return format((int(ch, base=16) + 8) % 16, 'x')

def isRootObjId(id):
    """ returns true if this is a root id (only for v2 schema) """
    if not isSchema2Id(id):
        raise ValueError("isRootObjId can only be used with v2 ids")
    validateUuid(id) # will throw ValueError exception if not a objid
    if id[0] != 'g':
        return False  # not a group
    token = getIdHexChars(id)
    # root ids will have last 16 chars rotated version of the first 16
    is_root = True
    for i in range(16):
        if token[i] != hexRot(token[i+16]):
            is_root = False
            break
    return is_root

def getRootObjId(id):
    """ returns root id for this objid if this is a root id (only for v2 schema) """
    if isRootObjId(id):
        return id  # this is the root id
    token = list(getIdHexChars(id))
    # root ids will have last 16 chars rotated version of the first 16
    for i in range(16):
        token[i+16] = hexRot(token[i])
    token = "".join(token)
    root_id = 'g-' + token[0:8] + '-' + token[8:16] + '-' + token[16:20] + '-' + token[20:26] + '-' + token[26:32]

    return root_id

def createObjId(obj_type, rootid=None):
    if obj_type not in ('groups', 'datasets', 'datatypes', 'chunks', "roots"):
        raise ValueError("unexpected obj_type")
    
    prefix = None
    if obj_type == 'datatypes':
        prefix = 't'  # don't collide with datasets
    elif obj_type == "roots":
        prefix = 'g'  # root obj is a group
    else:
        prefix = obj_type[0]
    if not rootid and obj_type != "roots":
        # v1 schema
        objid = prefix + '-' + str(uuid.uuid1())
    else:
        # schema v2
        salt = uuid.uuid4().hex
        # take a hash to randomize the uuid
        token = list(hashlib.sha256(salt.encode()).hexdigest())
        
        if rootid:
            # replace first 16 chars of token with first 16 chars of rootid
            root_hex = getIdHexChars(rootid)
            token[0:16] = root_hex[0:16]
        else:
            # obj_type == "roots"
            # use only 16 chars, but make it look a 32 char id
            for i in range(16):
                token[16+i] = hexRot(token[i])
        # format as a string
        token = "".join(token)        
        objid = prefix + '-' + token[0:8] + '-' + token[8:16] + '-' + token[16:20] + '-' + token[20:26] + '-' + token[26:32]

    return objid


def getS3Key(id):
    """ Return s3 key for given id. 

    For schema v1:
        A md5 prefix is added to the front of the returned key to better 
        distribute S3 objects.
    For schema v2:
        The id is converted to the pattern: "db/{rootid[0:16]}" for rootids and
        "db/id[0:16]/{prefix}/id[16-32]" for other ids
        Chunk ids have the chunk index added after the slash: "db/id[0:16]/d/id[16:32]/x_y_z
        
    For domain id's return a key with the .domain suffix and no preceeding slash
    """
    if id[0] == '/':
        # a domain id
        domain_suffix = ".domain.json"
        key = id[1:]
        if not key.endswith(domain_suffix):
            if key[-1] != '/':
                key += '/'
            key += domain_suffix
    else:
        if isSchema2Id(id):
            # schema v2 id
            hexid = getIdHexChars(id)
            prefix = id[0]  # one of g, d, t, c

            if isRootObjId(id):
                key = f"db/{hexid[0:8]}-{hexid[8:16]}"
            else:
                if prefix == 'c':
                    s3col = 'd'  # so that chunks will show up under their dataset
                else:
                    s3col = prefix
                key = f"db/{hexid[0:8]}-{hexid[8:16]}/{s3col}/{hexid[16:20]}-{hexid[20:26]}-{hexid[26:32]}"
            if prefix == 'c':
                # add the chunk coordinate
                index = id.index('_')  # will raise ValueError if not found
                coord = id[index+1:]
                key += '/'
                key += coord  
        else:
            # v1 id
            # schema v1 id
            idhash = getIdHash(id)
            key = f"{idhash}-{id}"

    return key

def getObjId(s3key):
    """ Return object id given valid s3key """
    if len(s3key) >= 44 and s3key[0:5].isalnum() and s3key[5] == '-' and s3key[6] in ('g', 'd', 'c', 't'):
        # v1 obj keys
        objid = s3key[6:]
    elif s3key.endswith("/.domain.json"):
        objid = '/' + s3key[:-(len("/.domain.json"))]
    elif s3key.startswith("db/"):
        # schema v2 object key
        parts = s3key.split('/')
        chunk_coord = ""  # used only for chunk ids
        token = []
        for ch in parts[1]:
            if ch != '-':
                token.append(ch)
        if len(parts) == 2:
            # root id
            # add 16 more chars using rotated version of first 16
            for i in range(16):
                token.append(hexRot(token[i]))
            prefix = 'g'
        else:  
            # group, dataset, or datatype if len(parts) == 4
            # chunk if len(parts) == 5
            for ch in parts[3]:
                if ch != '-':
                    token.append(ch)
            prefix = parts[2]
            
        if len(parts) == 5:
            # chunk id
            prefix = 'c'
            chunk_coord = "_" + parts[4]
        
        token = "".join(token)
        objid = prefix + '-' + token[0:8] + '-' + token[8:16] + '-' + token[16:20] + '-' + token[20:26] + '-' + token[26:32] + chunk_coord
    else:
        raise ValueError(f"unexpected S3Key: {s3key}")

    return objid


def isS3ObjKey(s3key):
    valid = False
    try:
        objid = getObjId(s3key)
        if objid:
            valid = True
    except KeyError:
        pass # ignore
    except ValueError:
        pass # ignore
    return valid

def createNodeId(prefix):
    """ Create a random id used to identify nodes"""
    node_uuid = str(uuid.uuid1())
    idhash = getIdHash(node_uuid)
    key = prefix + "-" + idhash
    return key



def getCollectionForId(obj_id):
    """ return groups/datasets/datatypes based on id """
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
        raise ValueError(f"Unexpected character in uuid: {ch}")

def isValidUuid(id, obj_class=None):
    try:
        validateUuid(id, obj_class)
        return True
    except ValueError:
        return False

def isValidChunkId(id):
    if not isValidUuid(id):
        return False
    if id[0] != 'c':
        return False
    return True



def getClassForObjId(id):
    """ return domains/chunks/groups/datasets/datatypes based on id """
    if not isinstance(id, str):
        raise ValueError("Expected string type")
    if len(id) == 0:
        raise ValueError("Empty string")
    if id[0] == '/':
        return "domains"
    if isValidChunkId(id):
        return "chunks"
    else:
        return getCollectionForId(id) 

def isObjId(id):
    """ return true if uuid or domain """
    if not isinstance(id, str) or len(id) == 0:
        return False
    if id[0] == '/':
        return True  # domain id is any string that starts with '/'
    return isValidUuid(id)


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
        partition = getObjPartition(obj_id, app['node_count'])
        msg = "wrong node for 'id':{}, expected node {} got {}".format(obj_id, app['node_number'], partition)
        log.error(msg)
        raise KeyError(msg)

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
        raise HTTPServiceUnavailable()
    dn_number = getObjPartition(obj_id, app['node_count'])
      
    url = dn_urls[dn_number]
    return url
