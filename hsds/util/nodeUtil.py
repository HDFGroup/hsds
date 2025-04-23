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
# nodeUtil:
# node (SN/DN mapping) related functions
#
import hashlib
import os.path
import uuid

from aiohttp.web_exceptions import HTTPServiceUnavailable

from .. import hsds_logger as log


def _getIdHash(id):
    """Return md5 prefix based on id value"""
    m = hashlib.new("md5")
    m.update(id.encode("utf8"))
    hexdigest = m.hexdigest()
    return hexdigest[:5]


def createNodeId(prefix, node_number=None):
    """Create a random id used to identify nodes"""
    node_id = ""  # nothing too bad happens if this doesn't get set
    if node_number is not None:
        # just make an id based on the node_number
        hash_key = f"{node_number + 1:03d}"
    else:
        # use the container id if we are running inside docker
        hash_key = _getIdHash(str(uuid.uuid1()))
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


def getObjPartition(id, count):
    """Get the id of the dn node that should be handling the given obj id"""
    hash_code = _getIdHash(id)
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
