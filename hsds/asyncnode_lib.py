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

from util.s3Util import  getS3Keys, getS3JSONObj, getS3ObjStats
from util.idUtil import getCollectionForId, getS3Key, getDataNodeUrl, isValidChunkId, isS3ObjKey, getObjId, isValidUuid, getClassForObjId
from util.httpUtil import http_delete
from util.chunkUtil import getDatasetId
from util.domainUtil import isValidDomain
import hsds_logger as log

def getCollection(objid):
    """ Return domains, groups, datasets, datatypes, or chunks based on id """
    obj_type = None
    if isValidDomain(objid):
        obj_type = "domains"
    elif isValidChunkId(objid):
        obj_type = "chunks"
    else:
        obj_type = getCollectionForId(objid)
    return obj_type

class S3Obj():
    def __init__(self, id, *args, **kwds):
        self._id = id
        #log.info("S3Obj({}) Keys: {}".format(id, list(kwds.keys())))
        if "ETag" in kwds:
            self._etag = kwds["ETag"]
        else:
            self._etag = None
        if "Size" in kwds:
            self._size = kwds["Size"]
        else:
            self._size = None
        if "LastModified" in kwds:
            self._lastModified = kwds["LastModified"]
        else:
            self._lastModified = None
        if "root" in kwds:
            self._root = kwds["root"]
         
    @property
    def id(self):
        return self._id

    @property
    def s3key(self):
        return getS3Key(self._id)

    @property
    def etag(self):
        return self._etag

    def setETag(self, etag):
        self._etag = etag

    @property
    def size(self):
        return self._size

    def setSize(self, size):
        self._size = size

    @property
    def lastModified(self):
        return self._lastModified

    def setLastModified(self, lastModified):
        self._lastModified = lastModified

    @property
    def root(self):
        if isValidChunkId(self._id):
            raise TypeError("root only valid for domains, groups, datasets, and datatypes")

        rootid = None
        try:
            rootid = self._rootid
        except AttributeError:
            pass # never defined
        
        return rootid

    @property
    def collection(self):
        return getCollection(self._id)

    @property
    def isRoot(self):
        if self.collection != "groups":
            return False
        is_root = False
        try:
            if self._rootid == self._id:
                is_root = True
        except AttributeError:
            pass # root not defined

        return is_root

    def setRoot(self, root_id):
        if self.root is not None:
            if self.root != root_id:
                raise KeyError("Root property was: {} but attempting to set to: {}".format(self.root, root_id))
            else:
                return # already set
        self._rootid = root_id

    def update(self,  **kwds):
        """ Update properties from kwds list """
        if "ETag" in kwds:
            self._etag = kwds["ETag"]
        if "Size" in kwds:
            self._size = kwds["Size"]
        if "LastModified" in kwds:
            self._lastModified = kwds["LastModified"]
        if "root" in kwds:
            self._root = kwds["root"]
        
    @property
    def used(self):
        if not isValidUuid(self._id) or isValidChunkId(self._id):
            raise TypeError("Only groups/datasets/datatypes have used flag")
        used = None
        try:
            used = self._used
        except AttributeError:
            pass # flag never set
        return used

    def setUsed(self, used):
        if not isValidUuid(self._id) or isValidChunkId(self._id):
            raise TypeError("Only groups/datasets/datatypes have used flag")
        if used not in (True, False):
            raise TypeError("Invalid argument")
        self._used = used

    @property
    def chunks(self):
        if self.collection != "datasets":
            raise TypeError("Only datasets can have a chunks colletion")
        chunks = None
        try:
            chunks = self._chunks
        except AttributeError:
            # chunks hasn't been initialized
            self._chunks = set()
            chunks = self._chunks
        return chunks

    @property
    def datasets(self):
        if not self.isRoot:
            raise KeyError("not a root object")

        datasets = None
        try:
            datasets = self._datasets
        except AttributeError:
            # datasets hasn't been initialized
            self._datasets = set()
            datasets = self._datasets
        return datasets
    
    @property
    def datatypes(self):
        if not self.isRoot:
            raise KeyError("not a root object")

        datatypes = None
        try:
            datatypes = self._datatypes
        except AttributeError:
            # datasets hasn't been initialized
            self._datatypes = set()
            datatypes = self._datatypes
        return datatypes

    @property
    def groups(self):
        if not self.isRoot:
            raise KeyError("not a root object")

        groups = None
        try:
            groups = self._groups
        except AttributeError:
            # datasets hasn't been initialized
            self._groups = set()
            groups = self._groups
        return groups

    def get_collection(self, collection):
        if collection not in ("groups", "datatypes", "datasets"):
            raise KeyError("unexpected argument")
        if collection == "groups":
            return self.groups
        elif collection == "datatypes":
            return self.datatypes
        return self.datasets

# End S3Obj class
    

   
def isGone(app, id):
    """ return True if this id is in the deleted_ids set """
    deleted_ids = app["deleted_ids"]
    if id in deleted_ids:
        return True
    else:
        return False
    
def isS3Obj(app, id):
    """ See if this object has already been loaded """
    deleted_ids = app["deleted_ids"]
    if id in deleted_ids:
        return False
    s3objs = app["s3objs"]
    if id in s3objs:
        return True
    else:
        return False
    
async def getRootProperty(app, s3obj):
    """ Get the root property if not already set """
    if s3obj.root is not None:
        return s3obj.root # root already set
    if isValidChunkId(s3obj.id):
        return None  # no root for chunk objects
    s3key = getS3Key(s3obj.id)
    obj_json = await getS3JSONObj(app, s3key)
    rootid = None
    if "root" not in obj_json:
        if isValidDomain(s3obj.id):
            log.info("No root for folder domain: {}".format(s3obj.id))
        else:
            log.warn("no root for {}".format(s3obj.id))
    else:
        rootid = obj_json["root"]
        log.info("got rootid {} for obj: {}".format(rootid, s3obj.id))
        s3obj.setRoot(obj_json["root"])
        if s3obj.root is None:
            log.error("unable to set root property")
    return rootid

     

async def getS3Obj(app, id, *args, **kwds):
    """ return object if found, otherwise create.
        kwd args will be used to initialize or update given properties """
    if not isValidDomain(id) and not isValidUuid(id):
        raise KeyError("Invalid id for getS3Obj")

    if "deleted_ids" in app:
        deleted_ids = app["deleted_ids"]
        if isValidUuid(id) and id in deleted_ids:
            raise HttpProcessingError(code=410, message="Object removed")
    log.info("getS3Obj {}".format(id))
    s3objs = app["s3objs"]
    s3obj = None
    s3stats = None
    eTag = None
    lastModified = None
    s3size = 0
    if "ETag" in kwds and "LastModified" in kwds and "Size" in kwds:
        # don't need to call getS3ObjStats
        eTag = kwds["ETag"]
        lastModified = kwds["LastModified"]
        s3size = kwds["Size"]
    else:
        try:
            s3key = getS3Key(id)
            s3stats = await getS3ObjStats(app, s3key) # will throw 404 if not found
            if "ETag" in s3stats:
                eTag = s3stats["ETag"]
            if "LastModified" in s3stats:
                lastModified = s3stats["LastModified"]
            if "Size" in s3stats:
                s3size = s3stats["Size"]
        except HttpProcessingError as hpe:
            # TBD - should we not add object if not in bucket?
            log.warn("getS3Obj - error getting s3stats for key {}: {}".format(s3key, hpe.code))

    old_size = 0
    if id in s3objs:
        s3obj = s3objs[id]
        old_size = s3obj.size
    else:
        s3obj = S3Obj(id, **kwds)

    if eTag is not None:
        log.info("s3obj {} set etag: {}".format(id, eTag))
        s3obj.setETag(eTag)

    if lastModified is not None:
        log.info("s3obj {} set lastModified: {}".format(id, lastModified))
        s3obj.setLastModified(lastModified)

    if s3size != 0:
        log.info("s3obj {} set size: {}".format(id, s3size))
        s3obj.setSize(s3size)
        app["bytes_in_bucket"] += (s3size - old_size)

    if "root" in kwds:
        s3obj.setRoot(kwds["root"])

    if id not in s3objs: 
        log.info("adding {} to s3objs - size: {}".format(id, s3obj.size))        
        s3objs[id] = s3obj

    if isValidDomain(id):
        if id in deleted_ids:
            # domain objects can be re-created
            deleted_ids.remove(id)
        # add to domain set
        domains = app["domains"]
        domains.add(id)
    elif s3obj.isRoot:
        roots = app["roots"]
        roots.add(id)
    
     
    return s3obj

def getRootForObjId(app, objid):
    """ get root id for the object """
    if isValidChunkId(objid):
        objid = getDatasetId(objid)
    s3objs = app["s3objs"]
    if objid not in s3objs:
        log.warn("getRootForObjId - {} not found in s3objs".format(objid))
        return None

    s3obj = s3objs[objid]

    if s3obj.root is None:
        log.warn("getRootForObjId - root not set objid: {}".format(objid))
        return None
 
    return s3obj.root

def getDomainForObjId(app, objid):
    """ Return the domain collection for the given objid """

    rootid = getRootForObjId(app, objid)
    if rootid is None:
        return None

    s3objs = app["s3objs"]
    
    domains = app["domains"]
    # Do a linear search for the given root 
    # TBD: create a reverse lookup of root -> domain
    for domain in domains:
        if domain not in s3objs:
            log.warn("domain in domains set not found in s3objs: {}".format(domain))
            continue
        domainS3Obj = s3objs[domain]
        if domainS3Obj.root == rootid:
            # found it
            return domain  
    log.info("getDomainForObjId - domain not found objid: {}".format(objid))
    return None

async def deleteObj(app, objid, notify=True):
    """ delete the object from S3 
    If notify is true, send a delete request to the appropriate DN node.
    Otherwise, (e.g. we're responding to a DN delete notification) 
    just remove the tracking obj from the DN global collection. """

    log.info("deleteObj: {}".format(objid))  
    s3objs = app["s3objs"]
    if "deleted_ids" not in app:
        # app should only create this key if the intent is to allow object deletion
        raise KeyError("deleted_ids key not found")

    deleted_ids = app["deleted_ids"]
    roots = app["roots"]

    if objid not in s3objs:
        log.error("deleteObj - {} not found in s3objs".format(objid))
        return False
    
    s3obj = s3objs[objid]
    num_bytes = s3obj.size   

    if notify:
        req = getDataNodeUrl(app, objid)
        collection = getClassForObjId(objid)

        req += '/' + collection + '/' + objid
        log.info("Delete object {}, [{} bytes]".format(objid, num_bytes))
     
        params = {"Notify": 0}  # Let the DN not to notify the AN node about this action
        try:
            await http_delete(app, req, params=params)
            success = True
        except HttpProcessingError as hpe:
        
            if hpe.code == 410:
                log.info("http_delete {} - object is already GONE".format(objid))
                success = True
            else:
                log.warn("Error deleting obj {}: {}".format(objid, hpe.code))
                success = False
                # TBD: add back to s3keys?
    else:
        success = True   # no notify, so we can always remove the s3obj

    if success:     
        del s3objs[objid]
        deleted_ids.add(objid)
        if objid in roots:
            roots.remove(objid)
        elif isValidChunkId(objid):
            log.info("delete for chunk: {}".format(objid))
        elif isValidUuid(objid) and s3obj.root is not None and s3obj.root in s3objs:
            # remove groups/datasets/datatypes from their domain collection
            s3root = s3objs[s3obj.root]
            obj_collection = getCollectionForId(objid)
            root_collection = s3root.get_collection(obj_collection)
            if objid in root_collection:
                log.info("removing {} from collection {} of root {}".format(objid, obj_collection, s3obj.root))
                root_collection.remove(objid)  

        app["bytes_in_bucket"] -= num_bytes

    return success


async def listKeys(app):
    """ Get all s3 keys in the bucket and create list of objkeys and domain keys """
    log.info("listKeys start")
    # Get all the keys for the bucket
    # request include_stats, so that for each key we get the ETag, LastModified, and Size values.
    s3keys = await getS3Keys(app, include_stats=True)
    log.info("got: {} keys".format(len(s3keys)))    
    for s3key in s3keys:
        log.info("got s3key: {}".format(s3key))
        if not isS3ObjKey(s3key):
            log.info("ignoring: {}".format(s3key))
            continue
        item = s3keys[s3key]   
        objid = getObjId(s3key)
        try:
            s3obj = await getS3Obj(app, objid, **item) 
        except HttpProcessingError as hpe:
            log.warn("got error retreiving obj {}: {}".format(objid, hpe.code))
            continue
        log.info("got s3obj({})".format(objid))

    s3objs = app["s3objs"]  
    domains = app["domains"] 
    roots = app["roots"]
    bytes_in_bucket = app["bytes_in_bucket"]  

    log.info("listKeys: s3objs...")
    for id in s3objs:
        log.info("{}".format(id))

    log.info("listKeys: add chunks")
    # iterate through s3keys again and add any chunks to the corresponding dataset
    for objid in s3objs:
        if isValidChunkId(objid):
            chunkid = objid
            dsetid = getDatasetId(chunkid)
            if dsetid not in s3objs:
                log.info("dataset for chunk: {} not found".format(chunkid))
            else:
                item = s3objs[chunkid]  # Dictionary of ETag, LastModified, and Size
                dset = s3objs[dsetid]
                log.info("listKeys: adding chunk {} to dset {} chunks".format(chunkid, dsetid))
                dset.chunks.add(chunkid)  

    # get root properties
    log.info("listKeys: get root properties")
    for objid in s3objs:
        log.info("listKeys getRootProperty: {}".format(objid))
        if isValidChunkId(objid):
            continue  # no root for chunks
        if isValidDomain(objid):
            if objid not in domains:
                log.error("Expected to find domain: {} in the domains set".format(objid))
        s3obj = s3objs[objid]
        if s3obj.root is None:
            try:
                log.info("listKeys: getRootProperty for {}".format(objid))
                await getRootProperty(app, s3obj)
                
                s3obj_check = s3objs[objid]
                log.info("root property for {} set to {}".format(objid, s3obj_check.root))
            except HttpProcessingError as hpe:
                log.warn("Got error getting root property of {}: {}".format(objid, hpe.code))
                continue
            
        if s3obj.isRoot:
            # add to roots set
            roots.add(objid)

    # iterate again and add non-root objects the root collection
    log.info("listKeys: add objects to root collections")
    for objid in s3objs:
        if isValidDomain(objid) or isValidChunkId(objid):
            continue
        # group/dataset or chunk
        s3obj = s3objs[objid]
        if s3obj.root is None:
            log.info("no rootid for id: {}".objid)
            continue
        if s3obj.isRoot:
            continue  # a root group doesn't belong to its own collection
        if s3obj.root not in s3objs:
            log.info("root obj {} for id: {} not found".format(objid, s3obj.root))
            continue
        s3root = s3objs[s3obj.root]
        root_collection = s3root.get_collection(getCollectionForId(objid))
        root_collection.add(objid)
        
        
    log.info("list keys done")
    log.info("s3object_cnt: {}".format(len(s3objs)))
    log.info("domain_cnt: {}".format(len(domains)))
    log.info("root_cnt: {}".format(len(roots)))
    log.info("bytes_in_bucket: {}".format(bytes_in_bucket))
     
    
 
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

async def safeRemoveLink(app, grpid, link_name, linkid):
    """ Remove the given link only if we're confident the linked object doesn't really exist """
    can_remove = False
    if isGone(app, linkid):
        log.info("Linked object has been deleted")
        can_remove = True
    else:
        # remove the link only if there should have been time for the object to show up
        grpobj = await getS3Obj(app, grpid) 
        lastModified = grpobj.lastModified
        now = time.time()
        if lastModified is not None and now - lastModified > app["anonymous_ttl"]:
            # the linkee should have been written to s3 by now, so this must be
            # a bogus link
            can_remove = True
        else:
            log.warn("missing linkee: {} for link: {} but group recently modified".format(linkid, link_name))

    if can_remove:
        await removeLink(app, grpid, link_name)
                         
                            
def clearUsedFlags(app):
    """ Reset all used flags """
    s3objs = app["s3objs"]
    for objid in s3objs:
        if isValidUuid(objid) and not isValidChunkId(objid):
            s3obj = s3objs[objid]
            s3obj.setUsed(False)
            

async def markObj(app, objid, updateLinks=False):
    """ Mark obj as in-use and for group objs, recursively call for hardlink objects 
    """
    log.info("markObj objid: {}".format(objid))
    s3objs = app["s3objs"]
    if objid not in s3objs:
        log.warn("markObj: expected to find {} in s3objs".format(objid))
        return

    s3obj = s3objs[objid]
    if s3obj.used:
        log.info("markObj: {} already in use".format(objid))
        return

    log.info("markObj: setUsed {}".format(objid))
    s3obj.setUsed(True)  # mark as inuse

    if s3obj.root is None:
        log.warn("markObj: root property not set for obj: {}".format(objid))
        return

    if s3obj.collection != "groups":
        log.info("markObj: not a group so no iterating for linked objects")
        return  # no linked objects to mark

    # for group object recurse through all hard links
    try:
        s3key = getS3Key(objid)
        group_json = await getS3JSONObj(app, s3key)
    except HttpProcessingError as hpe:
        log.warn("markObj: getS3JSONObj for {} raised hpe.{}".format(objid, hpe.code))
        return
        
    if "root" not in group_json:
        log.warn("Expected to find root key in groupjson for obj: {} (s3key: {})".formt(objid, getS3Key(objid)))
        return
    if s3obj.root is None:
        s3obj.setRoot(group_json["root"])
    elif s3obj.root != group_json["root"]:
        log.warn("markObj: root ids inconsistent - s3json: {} s3obj: {}".format(group_json["root"], s3obj.root))

    if "links" not in group_json:
        log.warn("makrObj: expected to find links key in groupjson for obj: {} (s3key: {})".formt(objid, getS3Key(objid)))
        return

    links = group_json["links"]
    
    for link_name in links:
        link_json = links[link_name]
        if "class" not in link_json:
            log.warn("Expected to find class key for link: {} in obj: {}".format(link_name, objid))
            continue
        if link_json["class"] == "H5L_TYPE_HARD":
            linkid = link_json["id"]
            if not isS3Obj(app, linkid):
                log.info("No linked id {} from group obj: {} with link name: {} found".format(linkid, objid, link_name))
                if updateLinks:
                    await safeRemoveLink(app, objid, link_name)
            else:
                log.info("markObj: recursive call from group: {} to link id: {}".format(objid, linkid))
                await markObj(app, linkid, updateLinks=updateLinks)

async def markObjs(app):
    """ Set Used flag for all objects that are linked from a domain """
    log.info("Mark objects")
    domains = app["domains"]
    roots = app["roots"]
    log.info("domains..")
    for domain in domains:
        log.info("domain: {}".format(domain))
    s3objs = app["s3objs"]
    for domain in domains:
        if domain not in s3objs:
            log.warn("markObjs: expected to find domain {} in s3objs".format(domain))
            continue
        s3obj = s3objs[domain]
        if s3obj.root is None:
            log.info("markObjs: skipping folder domain: {}".format(domain))
            continue
        if s3obj.root not in s3objs:
            log.warn("markObjs: root {} for domain not found in s3objs".format(s3obj.root, s3obj.id))
            continue
        s3root = s3objs[s3obj.root]
        if not s3root.isRoot:
            log.warn("markObjs: Expected to find {} to be root for domain: {}".format(s3root.id, domain))
            continue
        if s3root.root not in roots:
            log.warn("markObjs: Expected to find {} in roots set for domain: {}".format())
            continue
        # this will recurse into each linked object
        log.info("markObjs domain: {} root: {}".format(domain, s3obj.root))
        await markObj(app, s3obj.root)
       

 