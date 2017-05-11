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
# Head node of hsds cluster
# 

import time
from aiohttp.errors import HttpProcessingError

from util.s3Util import  getS3Keys, getS3JSONObj
from util.idUtil import getCollectionForId, getS3Key, getDataNodeUrl
from util.httpUtil import http_delete
from util.chunkUtil import getDatasetId 
import hsds_logger as log

async def listKeys(app):
    """ Get all s3 keys in the bucket and create list of objkeys and domain keys """
    log.info("listKeys start")
    # Get all the keys for the bucket
    # request include_stats, so that for each key we get the ETag, LastModified, and Size values.
    s3keys = await getS3Keys(app, include_stats=True)
    log.info("got: {} keys".format(len(s3keys)))
    domains = {}
    groups = {}
    datasets = {}
    datatypes = {}
    chunks = {}
    top_level_domains = {}
    group_cnt = 0
    dset_cnt = 0
    datatype_cnt = 0
    chunk_cnt = 0
    domain_cnt = 0
    other_cnt = 0
    bytes_in_bucket = 0
    for s3key in s3keys:
        item = s3keys[s3key]
        item["used"] = False   # Mixin "Used" flag of false
        bytes_in_bucket += item["Size"]
        if len(s3key) >= 44 and s3key[0:5].isalnum() and s3key[5] == '-' and s3key[6] in ('g', 'd', 'c', 't'):
            objid = s3key[6:]
            
            if objid[0] == 'g':
                groups[objid] = item
                group_cnt += 1
            elif objid[0] == 'd':
                # add a cunks dictionary that we'll use to store chunk keys later
                item["chunks"] = {}
                datasets[objid] = item
                dset_cnt += 1
            elif objid[0] == 't':
                datatypes[objid] = item
                datatype_cnt += 1
            elif objid[0] == 'c':
                chunks[objid] = item
                chunk_cnt += 1
        elif s3key == "headnode":
            item["used"] = True   # Mark used
        elif s3key.endswith(".txt"):
            # ignore collection files
            item["used"] = True   # Mark used
        elif s3key.endswith("/.domain.json"):
            item["used"] = True   # Mark used
            n = s3key.index('/')
            if n == 0:
                log.warn("unexpected domain name (leading slash): {}".format(s3key))
            elif n == -1:
                log.warn("unexpected domain name (no slash): {}".format(s3key))
            else:
                tld = s3key[:n]
                if tld not in top_level_domains:
                    top_level_domains[tld] = {}
                domain_cnt += 1
                # TBD - add a domainUtil func for this
                domain = '/' + s3key[:-(len("/.domain.json"))]
                log.info("adding domain: {} to domains list".format(domain))
                domains[domain] = {}
                #domains[domain] = {"groups": {}, "datasets": {}, "datatypes": {}}
            
        else:
            log.warn("unknown object: {}".format(s3key))
    log.info("domain_cnt: {}".format(domain_cnt))
    log.info("group_cnt: {}".format(group_cnt))
    log.info("dset_cnt: {}".format(dset_cnt))
    log.info("datatype_cnt: {}".format(datatype_cnt))
    log.info("chunk_cnt: {}".format(chunk_cnt))
    log.info("other_cnt: {}".format(other_cnt))
    log.info("top_level_domains:")
    for tld in top_level_domains:
        log.info(tld)    
    
    app["s3keys"] = s3keys
    app["domains"] = domains
    app["groups"] = groups
    app["datasets"] = datasets
    app["datatypes"] = datatypes
    app["chunks"] = chunks
    app["bytes_in_bucket"] = bytes_in_bucket
    app["roots"] = {}  # will be filled in by markObj

    # iterate through s3keys again and add any chunks to the corresponding dataset
    for chunkid in chunks:
        dset_id = getDatasetId(chunkid)
        if dset_id not in datasets:
            log.info("dataset for chunk: {} not found".format(chunkid))
        else:
            item = chunks[chunkid]  # Dictionary of ETag, LastModified, and Size
            item["used"] = True
            dset = datasets[dset_id]
            dset_chunks = dset["chunks"]
            dset_chunks[chunkid] = item
     
    log.info("listKeys done")

async def removeLink(app, grpid, link_name):
    """ Remove the given link """
    log.info("removeLink {} from group: {}".format(link_name, grpid))
    if getCollectionForId(grpid) != "groups":
        log.error("Expected groups id for removeLink call")
        return False
    req = getDataNodeUrl(app, grpid)

    req += "/groups/" + grpid + "/links/" + link_name
    log.info("Delete request: {}".format(req))
    params = {"Notify": 0}  # Let the DN not to notify the AN node about this action
    try:
        await http_delete(app, req, params=params)
        success = True
    except HttpProcessingError as hpe:
        log.warn("Error deleting link {}: {}".format(grpid, hpe.code))
        success = False
    return success

async def markObj(app, domain, objid=None, rootid=None, updateLinks=False):
    """ Mark obj as in-use and for group objs, recursively call for hardlink objects 
    """
    log.info("markObj domain: {} objid: {}".format(domain, objid))
    domains = app["domains"]
    if domain not in domains:
        log.error("Expected to find domain: {} in domains collection".format(domain))
        return
    roots = app["roots"]
    domain_obj = domains[domain]
    
    # if no objid, start with the root
    if objid is None:        
        s3key = getS3Key(domain)
        # TBD - use LRU Cache to avoid repeated lookups?
        obj_json = await getS3JSONObj(app, s3key)
        if "root" not in obj_json:
            # Skip folder domains
            log.info("no root for {} (domain folder)".format(domain))
            return
        rootid = obj_json["root"]
        domain_obj["root"] = rootid
        # create new root collection
        root_obj = {}
        root_obj["groups"] = {}
        root_obj["datasets"] = {}
        root_obj["datatypes"] = {}
        
        roots[rootid] = root_obj
        objid = rootid
        log.info("{} root: {}".format(domain, objid))
    else:
        if rootid is None:
            log.error("expected root id for markobj")
            return
        if rootid not in roots:
            log.error("expected to find root id: {} in roots collection".format(rootid))
            return
        root_obj = roots[rootid]

    log.info("markObj: {} root: {}".format(objid, rootid))
    collection = getCollectionForId(objid)
    domain_ids = root_obj[collection]
    if objid in domain_ids:
        log.warn("already visited obj {}".format(objid))
        return
    bucket_ids = app[collection]
     
    if objid not in bucket_ids:
        log.warn("Expected to find id: {} in bucket (s3key: {}) for domain: {}".format(objid, getS3Key(objid), domain))
        return
    obj = bucket_ids[objid]
    if obj["used"]:
        # we must have already visited this object and its children before
        # i.e. through a loop in the graph, so just return here
        log.warn("Expected used state to be False for obj: {}".format(objid))
        return
    domain_ids[objid] = obj  # add to domain collection
    obj["used"] = True  # in use
    if collection == "groups":
        # add the objid to our domain list by collection type
        s3key = getS3Key(objid)
        try:
            group_json = await getS3JSONObj(app, s3key)
        except HttpProcessingError as hpe:
            log.warn("Got error retrieving key {}: {}".format(s3key, hpe))
            return
        if "domain" not in group_json:
            log.warn("Expected to find domain key for obj: {} (s3key: {})".format(objid, getS3Key(objid)))
            return
        if group_json["domain"] != domain:
            log.warn("Unexpected domain for obj {}: {}".format(objid, group_json["domain"]))
            return
        # For group objects, iteratore through all the hard links and mark those objects
        if "links"  not in group_json:
            log.warn("Expected to find links key in groupjson for obj: {} (s3key: {})".formt(objid, getS3Key(objid)))
            return
        links = group_json["links"]
        for link_name in links:
            link_json = links[link_name]
            if "class" not in link_json:
                log.warn("Expected to find class key for link: {} in obj: {}".format(link_name, objid))
                continue
            if link_json["class"] == "H5L_TYPE_HARD":
                linkid = link_json["id"]
                try:
                    linkee_collection = getCollectionForId(linkid)
                except ValueError:
                    log.warn("got unexpected collection type for link id: {}".format(linkid))
                    continue
                
                collection_ids = app[linkee_collection]
                if linkid not in collection_ids:
                    log.warn("linked obj: {} not found".format(linkid))
                    lastModified = obj["LastModified"]
                    now = time.time()
                    if updateLinks and now - lastModified > app["anonymous_ttl"]:
                        # the linkee should have been written to s3 by now, so this must be
                        # a bogus link
                        await removeLink(app, objid, link_name)
                    else:
                        log.warn("missing linkee: {} but group recently modified".format(linkid))
                else:
                    await markObj(app, domain, objid=linkid, rootid=rootid)  

 