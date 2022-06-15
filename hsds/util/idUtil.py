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

import os.path
import hashlib
import uuid
from aiohttp.web_exceptions import HTTPServiceUnavailable
from .. import hsds_logger as log


def getIdHash(id):
    """Return md5 prefix based on id value"""
    m = hashlib.new("md5")
    m.update(id.encode("utf8"))
    hexdigest = m.hexdigest()
    return hexdigest[:5]


def isSchema2Id(id):
    """return true if this is a v2 id"""
    # v1 ids are in the standard UUID format: 8-4-4-4-12
    # v2 ids are in the non-standard: 8-8-4-6-6
    parts = id.split("-")
    if len(parts) != 6:
        raise ValueError(f"Unexpected id formation for uuid: {id}")
    if len(parts[2]) == 8:
        return True
    else:
        return False


def getIdHexChars(id):
    """get the hex chars of the given id"""
    if id[0] == "c":
        # don't include chunk index
        index = id.index("_")
        parts = id[0:index].split("-")
    else:
        parts = id.split("-")
    if len(parts) != 6:
        raise ValueError(f"Unexpected id format for uuid: {id}")
    return "".join(parts[1:])


def hexRot(ch):
    """rotate hex character by 8"""
    return format((int(ch, base=16) + 8) % 16, "x")


def isRootObjId(id):
    """returns true if this is a root id (only for v2 schema)"""
    if not isSchema2Id(id):
        raise ValueError("isRootObjId can only be used with v2 ids")
    validateUuid(id)  # will throw ValueError exception if not a objid
    if id[0] != "g":
        return False  # not a group
    token = getIdHexChars(id)
    # root ids will have last 16 chars rotated version of the first 16
    is_root = True
    for i in range(16):
        if token[i] != hexRot(token[i + 16]):
            is_root = False
            break
    return is_root


def getRootObjId(id):
    """returns root id for this objid if this is a root id
    (only for v2 schema)
    """
    if isRootObjId(id):
        return id  # this is the root id
    token = list(getIdHexChars(id))
    # root ids will have last 16 chars rotated version of the first 16
    for i in range(16):
        token[i + 16] = hexRot(token[i])
    token = "".join(token)
    root_id = "g-" + token[0:8] + "-" + token[8:16] + "-" + token[16:20]
    root_id += "-" + token[20:26] + "-" + token[26:32]

    return root_id


def createObjId(obj_type, rootid=None):
    if obj_type not in ("groups", "datasets", "datatypes", "chunks", "roots"):
        raise ValueError("unexpected obj_type")

    prefix = None
    if obj_type == "datatypes":
        prefix = "t"  # don't collide with datasets
    elif obj_type == "roots":
        prefix = "g"  # root obj is a group
    else:
        prefix = obj_type[0]
    if not rootid and obj_type != "roots":
        # v1 schema - folder
        objid = prefix + "-" + str(uuid.uuid1())
    elif rootid and not isSchema2Id(rootid):
        # v1 schema - domain
        objid = prefix + "-" + str(uuid.uuid1())
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
                token[16 + i] = hexRot(token[i])
        # format as a string
        token = "".join(token)
        objid = prefix + "-" + token[0:8] + "-" + token[8:16] + "-"
        objid += token[16:20] + "-" + token[20:26] + "-" + token[26:32]

    return objid


def getS3Key(id):
    """Return s3 key for given id.

    For schema v1:
        A md5 prefix is added to the front of the returned key to better
        distribute S3 objects.
    For schema v2:
        The id is converted to the pattern: "db/{rootid[0:16]}" for rootids and
        "db/id[0:16]/{prefix}/id[16-32]" for other ids
        Chunk ids have the chunk index added after the slash:
        "db/id[0:16]/d/id[16:32]/x_y_z

    For domain id's return a key with the .domain suffix and no
    preceeding slash
    """
    if id.find("/") > 0:
        # a domain id
        domain_suffix = ".domain.json"
        index = id.find("/") + 1
        key = id[index:]
        if not key.endswith(domain_suffix):
            if key[-1] != "/":
                key += "/"
            key += domain_suffix
    else:
        if isSchema2Id(id):
            # schema v2 id
            hexid = getIdHexChars(id)
            prefix = id[0]  # one of g, d, t, c
            if prefix not in ("g", "d", "t", "c"):
                raise ValueError(f"Unexpected id: {id}")

            if isRootObjId(id):
                key = f"db/{hexid[0:8]}-{hexid[8:16]}"
            else:
                partition = ""
                if prefix == "c":
                    # use 'g' so that chunks will show up under their dataset
                    s3col = "d"
                    n = id.find("-")
                    if n > 1:
                        # extract the partition index if present
                        partition = "p" + id[1:n]
                else:
                    s3col = prefix
                key = f"db/{hexid[0:8]}-{hexid[8:16]}/{s3col}/{hexid[16:20]}"
                key += f"-{hexid[20:26]}-{hexid[26:32]}"
            if prefix == "c":
                if partition:
                    key += "/"
                    key += partition
                # add the chunk coordinate
                index = id.index("_")  # will raise ValueError if not found
                n = index + 1
                coord = id[n:]
                key += "/"
                key += coord
            elif prefix == "g":
                # add key suffix for group
                key += "/.group.json"
            elif prefix == "d":
                # add key suffix for dataset
                key += "/.dataset.json"
            else:
                # add key suffix for datatype
                key += "/.datatype.json"
        else:
            # v1 id
            # schema v1 id
            idhash = getIdHash(id)
            key = f"{idhash}-{id}"

    return key


def getObjId(s3key):
    """Return object id given valid s3key"""
    if all(
        (
            len(s3key) >= 44 and s3key[0:5].isalnum(),
            len(s3key) >= 44 and s3key[5] == "-",
            len(s3key) >= 44 and s3key[6] in ("g", "d", "c", "t"),
        )
    ):
        # v1 obj keys
        objid = s3key[6:]
    elif s3key.endswith("/.domain.json"):
        objid = "/" + s3key[: -(len("/.domain.json"))]
    elif s3key.startswith("db/"):
        # schema v2 object key
        parts = s3key.split("/")
        chunk_coord = ""  # used only for chunk ids
        partition = ""  # likewise
        token = []
        for ch in parts[1]:
            if ch != "-":
                token.append(ch)

        if len(parts) == 3:
            # root id
            # last part should be ".group.json"
            if parts[2] != ".group.json":
                raise ValueError(f"unexpected S3Key: {s3key}")
            # add 16 more chars using rotated version of first 16
            for i in range(16):
                token.append(hexRot(token[i]))
            prefix = "g"
        elif len(parts) == 5:
            # group, dataset, or datatype or chunk
            for ch in parts[3]:
                if ch != "-":
                    token.append(ch)

            if parts[2] == "g" and parts[4] == ".group.json":
                prefix = "g"  # group json
            elif parts[2] == "t" and parts[4] == ".datatype.json":
                prefix = "t"  # datatype json
            elif parts[2] == "d":
                if parts[4] == ".dataset.json":
                    prefix = "d"  # dataset json
                else:
                    # chunk object
                    prefix = "c"
                    chunk_coord = "_" + parts[4]
            else:
                raise ValueError(f"unexpected S3Key: {s3key}")
        elif len(parts) == 6:
            # chunk key with partitioning
            for ch in parts[3]:
                if ch != "-":
                    token.append(ch)
            if parts[2][0] != "d":
                raise ValueError(f"unexpected S3Key: {s3key}")
            prefix = "c"
            partition = parts[4]
            if partition[0] != "p":
                raise ValueError(f"unexpected S3Key: {s3key}")
            partition = partition[1:]  # strip off the p
            chunk_coord = "_" + parts[5]
        else:
            raise ValueError(f"unexpected S3Key: {s3key}")

        token = "".join(token)
        objid = prefix + partition + "-" + token[0:8] + "-" + token[8:16]
        objid += "-" + token[16:20] + "-" + token[20:26] + "-"
        objid += token[26:32] + chunk_coord
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
        pass  # ignore
    except ValueError:
        pass  # ignore
    return valid


def createNodeId(prefix, node_number=None):
    """Create a random id used to identify nodes"""
    node_id = ""  # nothing too bad happens if this doesn't get set
    if node_number is not None:
        # just make an id based on the node_number
        hash_key = f"{node_number+1:03d}"
    else:
        # use the container id if we are running inside docker
        hash_key = getIdHash(str(uuid.uuid1()))
        proc_file = "/proc/self/cgroup"
        if os.path.isfile(proc_file):
            with open(proc_file) as f:
                first_line = f.readline()
                if first_line:
                    fields = first_line.split(":")
                    if len(fields) >= 3:
                        field = fields[2]
                        if field.startswith("/docker/"):
                            docker_len = len("/docker/")

                            if len(field) > docker_len + 12:
                                n = docker_len
                                m = n + 12
                                node_id = field[n:m]

    if node_id:
        key = f"{prefix}-{node_id}-{hash_key}"
    else:
        key = f"{prefix}-{hash_key}"
    return key


def getCollectionForId(obj_id):
    """return groups/datasets/datatypes based on id"""
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


def validateUuid(id, obj_class=None):
    if not isinstance(id, str):
        raise ValueError("Expected string type")
    if len(id) < 38:
        # id should be prefix (e.g. "g-") and uuid value
        raise ValueError("Unexpected id length")
    if id[0] not in ("g", "d", "t", "c"):
        raise ValueError("Unexpected prefix")
    if id[0] != "c" and id[1] != "-":
        # chunk ids may have a partition index following the c
        raise ValueError("Unexpected prefix")
    if obj_class is not None:
        obj_class = obj_class.lower()
        prefix = obj_class[0]
        if obj_class.startswith("datatype"):
            prefix = "t"
        if id[0] != prefix:
            raise ValueError(f"Unexpected prefix for class: {obj_class}")
    if id[0] == "c":
        # trim the type char and any partition id
        n = id.find("-")
        if n == -1:
            raise ValueError("Invalid chunk id")

        # trim the chunk index for chunk ids
        m = id.find("_")
        if m == -1:
            raise ValueError("Invalid chunk id")
        n += 1
        id = "c-" + id[n:m]
    if len(id) != 38:
        # id should be 36 now
        raise ValueError("Unexpected id length")

    for ch in id:
        if ch.isalnum():
            continue
        if ch == "-":
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
    if id[0] != "c":
        return False
    return True


def getClassForObjId(id):
    """return domains/chunks/groups/datasets/datatypes based on id"""
    if not isinstance(id, str):
        raise ValueError("Expected string type")
    if len(id) == 0:
        raise ValueError("Empty string")
    if id[0] == "/":
        return "domains"
    if isValidChunkId(id):
        return "chunks"
    else:
        return getCollectionForId(id)


def isObjId(id):
    """return true if uuid or domain"""
    if not isinstance(id, str) or len(id) == 0:
        return False
    if id.find("/") > 0:
        # domain id is any string in the form <bucket_name>/<domain_path>
        return True
    return isValidUuid(id)


def getUuidFromId(id):
    """strip off the type prefix ('g-' or 'd-', or 't-')
    and return the uuid part"""
    return id[2:]


def getObjPartition(id, count):
    """Get the id of the dn node that should be handling the given obj id"""
    hash_code = getIdHash(id)
    hash_value = int(hash_code, 16)
    number = hash_value % count
    return number


def getNodeNumber(app):
    if app["node_type"] == "sn":
        log.error("node number if only for DN nodes")
        raise ValueError()

    dn_ids = app["dn_ids"]
    log.debug(f"getNodeNumber(from dn_ids: {dn_ids})")
    for i in range(len(dn_ids)):
        dn_id = dn_ids[i]
        if dn_id == app["id"]:
            log.debug(f"returning nodeNumber: {i}")
            return i
    log.error("getNodeNumber, no matching id")
    return -1


def getNodeCount(app):
    dn_urls = app["dn_urls"]
    log.debug(f"getNodeCount for dn_urls: {dn_urls}")
    dn_node_count = len(dn_urls)
    return dn_node_count


def validateInPartition(app, obj_id):
    node_number = getNodeNumber(app)
    node_count = getNodeCount(app)
    msg = f"obj_id: {obj_id}, node_count: {node_count}, "
    msg += f"node_number: {node_number}"
    log.debug(msg)
    partition_number = getObjPartition(obj_id, node_count)
    if partition_number != node_number:
        # The request shouldn't have come to this node'
        msg = f"wrong node for 'id':{obj_id}, expected node {node_number} "
        msg += f"got {partition_number}"
        log.error(msg)
        raise KeyError(msg)


def getDataNodeUrl(app, obj_id):
    """Return host/port for datanode for given obj_id.
    Throw exception if service is not ready"""
    dn_urls = app["dn_urls"]
    dn_node_count = getNodeCount(app)
    node_state = app["node_state"]
    if node_state != "READY" or dn_node_count <= 0:
        msg = "Service not ready"
        log.warn(msg)
        raise HTTPServiceUnavailable()
    dn_number = getObjPartition(obj_id, dn_node_count)
    url = dn_urls[dn_number]
    log.debug(f"got dn_url: {url} for obj_id: {obj_id}")
    return url
