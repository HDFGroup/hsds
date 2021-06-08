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
# service node of hsds cluster
#
from asyncio import CancelledError
import asyncio
import json
import numcodecs as codecs
import os.path as op
import re
import time

from aiohttp.web_exceptions import HTTPBadRequest, HTTPForbidden, HTTPNotFound, HTTPGone, HTTPInternalServerError, HTTPConflict, HTTPServiceUnavailable
from aiohttp import ClientResponseError
from aiohttp.client_exceptions import ClientError
from aiohttp.web import json_response

from .util.httpUtil import  http_post, http_put, http_get, http_delete, getHref, get_http_client, jsonResponse
from .util.idUtil import  getDataNodeUrl, createObjId, getCollectionForId, isValidUuid, isSchema2Id, getNodeCount
from .util.authUtil import getUserPasswordFromRequest, aclCheck, isAdminUser
from .util.authUtil import validateUserPassword, getAclKeys
from .util.domainUtil import getParentDomain, getDomainFromRequest, isValidDomain, getBucketForDomain, getPathForDomain
from .util.storUtil import getStorKeys
from .util.boolparser import BooleanParser
from .servicenode_lib import getDomainJson, getObjectJson, getObjectIdByPath, getRootInfo
from .basenode import getVersion
from . import hsds_logger as log
from . import config

class DomainCrawler:
    def __init__(self, app, root_id, bucket=None, include_attrs=True, max_tasks=40, max_objects_limit=0):
        log.info(f"DomainCrawler.__init__  root_id: {root_id}")
        self._app = app
        self._max_objects_limit = max_objects_limit
        self._include_attrs = include_attrs
        self._max_tasks = max_tasks
        self._q = asyncio.Queue()
        self._obj_dict = {}
        self.seen_ids = set()
        self._q.put_nowait(root_id)
        self._bucket = bucket

    async def crawl(self):
        workers = [asyncio.Task(self.work())
                   for _ in range(self._max_tasks)]
        # When all work is done, exit.
        log.info(f"DomainCrawler - await queue.join - count: {len(self._obj_dict)}")
        await self._q.join()
        log.info(f"DomainCrawler - join complete - count: {len(self._obj_dict)}")

        for w in workers:
            w.cancel()
        log.debug("DomainCrawler - workers canceled")

    async def work(self):
        while True:
            obj_id = await self._q.get()
            await self.fetch(obj_id)
            self._q.task_done()

    async def fetch(self, obj_id):
        log.debug(f"DomainCrawler - fetch for obj_id: {obj_id}")
        obj_json = await getObjectJson(self._app, obj_id, include_links=True, include_attrs=self._include_attrs, bucket=self._bucket)
        log.debug(f"DomainCrawler - got json for {obj_id}")

        # including links, so don't need link count
        if "link_count" in obj_json:
            del obj_json["link_count"]
        self._obj_dict[obj_id] = obj_json
        if self._include_attrs:
            del obj_json["attributeCount"]
       
        # if this is a group, iterate through all the hard links and
        # add to the lookup ids set
        if getCollectionForId(obj_id) == "groups":
            links = obj_json["links"]
            log.debug(f"DomainCrawler links: {links}")
            for title in links:
                log.debug(f"DomainCrawler - got link: {title}")
                link_obj = links[title]
                num_objects = len(self._obj_dict)
                if self._max_objects_limit > 0 and num_objects >= self._max_objects_limit:
                    log.info(f"DomainCrawler reached limit of {self._max_objects_limit}")
                    break
                if link_obj["class"] != 'H5L_TYPE_HARD':
                    continue
                link_id = link_obj["id"]
                if link_id not in self._obj_dict:
                    # haven't seen this object yet, get obj json
                    log.debug(f"DomainCrawler - adding link_id: {link_id}")
                    self._obj_dict[link_id] = {} # placeholder for obj id
                    self._q.put_nowait(link_id)
        log.debug(f"DomainCrawler - fetch complete obj_id: {obj_id}, {len(self._obj_dict)} objects found")

class FolderCrawler:
    def __init__(self, app, domains, bucket=None, get_root=False, verbose=False, max_tasks_per_node=100):
        log.info(f"FolderCrawler.__init__  {len(domains)} domain names")
        self._app = app
        self._get_root = get_root
        self._verbose = verbose
        self._q = asyncio.Queue()
        self._domain_dict = {}
        self._group_dict = {}
        for domain in domains:
            self._q.put_nowait(domain)
        self._bucket = bucket
        max_tasks = max_tasks_per_node * getNodeCount(app)
        if len(domains) > max_tasks:
            self._max_tasks = max_tasks
        else:
            self._max_tasks = len(domains)

    async def crawl(self):
        workers = [asyncio.Task(self.work())
                   for _ in range(self._max_tasks)]
        # When all work is done, exit.
        log.info(f"FolderCrawler max_tasks {self._max_tasks} = await queue.join - count: {len(self._domain_dict)}")
        await self._q.join()
        log.info(f"FolderCrawler - join complete - count: {len(self._domain_dict)}")

        for w in workers:
            w.cancel()
        log.debug("FolderCrawler - workers canceled")

    async def work(self):
        while True:
            start = time.time()
            domain = await self._q.get()
            await self.fetch(domain)
            self._q.task_done()
            elapsed = time.time() - start
            log.debug(f"FolderCrawler - task {domain} start: {start:.3f} elapsed: {elapsed:.3f}")

    async def fetch(self, domain):
        log.debug(f"FolderCrawler - fetch for domain: {domain} bucket: {self._bucket}")
        domain_key = self._bucket + domain
        try:
            domain_json = await getDomainJson(self._app, domain_key, reload=True)
            log.debug(f"FolderCrawler - {domain} got domain_json: {domain_json}")
            if domain_json:
                domain_rsp = await get_domain_response(self._app, domain_json, verbose=self._verbose, bucket=self._bucket)
                if "limits" in domain_rsp:
                    # don't return limits for multi-domain responses
                    del domain_rsp["limits"]
                if "version" in domain_rsp:
                    del domain_rsp["version"]
                log.debug(f"FolderCrawler - {domain} get domain_rsp: {domain_rsp}")
                # mixin domain name
                self._domain_dict[domain] = domain_rsp
                if self._get_root and "root" in domain_json:
                    root_id = domain_json["root"]
                    log.debug(f"fetching root json for {root_id}")
                    root_json = await getObjectJson(self._app, root_id, include_links=False, include_attrs=True, bucket=self._bucket)
                    log.debug(f"got root_json: {root_json}")
                    self._group_dict[root_id] = root_json
            else:
                log.warn(f"FolderCrawler - no domain found for {domain}")
        except HTTPNotFound:
            # One of the dmains not found, but continue through the list
            log.warn(f"fetch result - not found error for: {domain}")
        except HTTPGone:
            log.warn(f"fetch result - domain: {domain} has been deleted")
        except HTTPInternalServerError:
            log.error(f"fetch result - internal error fetching: {domain}")
        except HTTPForbidden:
            log.warn(f"fetch result - access not allowed for: {domain}")
        except HTTPBadRequest:
            log.error(f"fetch result - bad request for: {domain}")
        except HTTPServiceUnavailable:
            log.warn(f"fetch result - service unavailable for domain: {domain}")
        except Exception as e:
            log.error(f"fetch result - unexpected exception for domain {domain}: exception of type {type(e)}, {e}")

async def get_collections(app, root_id):
    """ Return the object ids for given root.
    """

    log.info(f"get_collections for {root_id}")
    groups = {}
    datasets = {}
    datatypes = {}
    lookup_ids = set()
    lookup_ids.add(root_id)

    while lookup_ids:
        grp_id = lookup_ids.pop()
        req = getDataNodeUrl(app, grp_id)
        req += '/groups/' + grp_id + "/links"
        log.debug("collection get LINKS: " + req)
        try:
            links_json = await http_get(app, req)  # throws 404 if doesn't exist
        except HTTPNotFound:
            log.warn(f"get_collection, group {grp_id} not found")
            continue

        log.debug(f"got links json from dn for group_id: {grp_id}")
        links = links_json["links"]
        log.debug(f"get_collection: got links: {links}")
        for link in links:
            if link["class"] != 'H5L_TYPE_HARD':
                continue
            link_id = link["id"]
            obj_type = getCollectionForId(link_id)
            if obj_type == "groups":
                if link_id in groups:
                    continue  # been here before
                groups[link_id] = {}
                lookup_ids.add(link_id)
            elif obj_type == "datasets":
                if link_id in datasets:
                    continue
                datasets[link_id] = {}
            elif obj_type == "datatypes":
                if link_id in datatypes:
                    continue
                datatypes[link_id] = {}
            else:
                log.error(f"get_collection: unexpected link object type: {obj_type}")
                HTTPInternalServerError()

    result = {}
    result["groups"] = groups
    result["datasets"] = datasets
    result["datatypes"] = datatypes
    return result

async def getDomainObjects(app, root_id, include_attrs=False, bucket=None):
    """ Iterate through all objects in heirarchy and add to obj_dict keyed by obj id
    """

    log.info(f"getDomainObjects for root: {root_id}")
    max_objects_limit = int(config.get("domain_req_max_objects_limit", default=500)) 

    crawler = DomainCrawler(app, root_id, include_attrs=include_attrs, bucket=bucket, max_objects_limit=max_objects_limit)
    await crawler.crawl()
    if len(crawler._obj_dict) >= max_objects_limit:
        log.info(f"getDomainObjects - too many objects:  {len(crawler._obj_dict)}, returning None")
        return None
    else:
        log.info(f"getDomainObjects returning: {len(crawler._obj_dict)} objects")
        return crawler._obj_dict

def getIdList(objs, marker=None, limit=None):
    """ takes a map of ids to objs and returns ordered list
        of ids, optionally reduced by marker and limit """

    ids = []
    for k in objs:
        ids.append(k)
    ids.sort()
    if not marker and not limit:
        return ids  # just return ids
    ret_ids = []
    for id in ids:
        if marker:
            if id == marker:
                marker = None  # clear so we will start adding items
            continue
        ret_ids.append(id)
        if limit and len(ret_ids) == limit:
            break
    return ret_ids

def getLimits():
    """ return limits the client may need """
    limits = {}
    limits["min_chunk_size"] = int(config.get("min_chunk_size"))
    limits["max_chunk_size"] = int(config.get("max_chunk_size"))
    limits["max_request_size"] = int(config.get("max_request_size"))
    limits["max_chunks_per_request"] = int(config.get("max_chunks_per_request"))
    return limits

def getCompressors():
    """ return available compressors """
    compressors = codecs.blosc.list_compressors()
    # replace zlib with the equivalent gzip since that is the h5py name
    if "gzip" not in compressors and "zlib" in compressors:
        for i in range(len(compressors)):
            if compressors[i] == "zlib":
                compressors[i] = "gzip"
                break
    
    return compressors 

async def get_domain_response(app, domain_json, bucket=None, verbose=False):
    rsp_json = { }
    if "root" in domain_json:
        rsp_json["root"] = domain_json["root"]
        rsp_json["class"] = "domain"
    else:
        rsp_json["class"] = "folder"
    if "owner" in domain_json:
        rsp_json["owner"] = domain_json["owner"]
    if "created" in domain_json:
        rsp_json["created"] = domain_json["created"]

    lastModified = 0
    if "lastModified" in domain_json:
        lastModified = domain_json["lastModified"]
    totalSize = len(json.dumps(domain_json))
    metadata_bytes = 0
    allocated_bytes = 0
    linked_bytes = 0
    num_chunks = 0
    num_linked_chunks = 0
    md5_sum = ""

    if verbose and "root" in domain_json:
        root_id = domain_json["root"]
        root_info = await getRootInfo(app, domain_json["root"], bucket=bucket)
        if root_info:
            log.info(f"got root_info for root: {root_id}")
            allocated_bytes = root_info["allocated_bytes"]
            totalSize += allocated_bytes
            if "linked_bytes" in root_info:
                linked_bytes += root_info["linked_bytes"]
                totalSize += linked_bytes
            if "num_linked_chunks" in root_info:
                num_linked_chunks = root_info["num_linked_chunks"]
            if "metadata_bytes" in root_info:
                # this key was added for schema v2
                metadata_bytes = root_info["metadata_bytes"]
                totalSize += metadata_bytes
            if root_info["lastModified"] > lastModified:
                lastModified = root_info["lastModified"]
            if "md5_sum" in root_info:
                md5_sum = root_info["md5_sum"]

            num_groups = root_info["num_groups"]
            num_datatypes = root_info["num_datatypes"]
            num_datasets = len(root_info["datasets"])
            num_chunks = root_info["num_chunks"]
            rsp_json["scan_info"] = root_info  # return verbose info here

        else:
            # root info not available - just return 0 for these values
            log.info(f"root_info not available for root: {root_id}")
            allocated_bytes = 0
            totalSize = 0
            num_groups = 0
            num_datasets = 0
            num_datatypes = 0
            num_chunks = 0


        num_objects = num_groups + num_datasets + num_datatypes + num_chunks
        rsp_json["num_groups"] = num_groups
        rsp_json["num_datasets"] = num_datasets
        rsp_json["num_datatypes"] = num_datatypes
        rsp_json["num_objects"] = num_objects
        rsp_json["total_size"] = totalSize
        rsp_json["allocated_bytes"] = allocated_bytes
        rsp_json["num_objects"] =  num_objects
        rsp_json["metadata_bytes"] = metadata_bytes
        rsp_json["linked_bytes"] = linked_bytes
        rsp_json["num_chunks"] = num_chunks
        rsp_json["num_linked_chunks"] = num_linked_chunks
        rsp_json["md5_sum"] = md5_sum

    # pass back config parameters the client may care about

    rsp_json["limits"] = getLimits()
    rsp_json["compressors"] = getCompressors()
    rsp_json["version"] = getVersion()
    rsp_json["lastModified"] = lastModified
    return rsp_json


async def get_domains(request):
    """ This method is called by GET_Domains and GET_Domain """
    app = request.app
    params = request.rel_url.query

    node_count = getNodeCount(app)  # DomainCrawler will expect this to be larger than zero
    if node_count == 0:
        log.warn("get_domains called with no active DN nodes")
        raise HTTPServiceUnavailable()

    # if there is no domain passed in, get a list of top level domains
    if "domain" not in request.rel_url.query:
        prefix = '/'
    else:
        prefix = request.rel_url.query["domain"]

    if "pattern" not in request.rel_url.query:
        pattern = None
        regex = None
    else:
        pattern = request.rel_url.query["pattern"]
        log.info(f"get_domains - using regex pattern: {pattern}")
        regex = re.compile(pattern)

    if "query" not in request.rel_url.query:
        query = None
    else:
        query = request.rel_url.query["query"]
        log.info(f"get_domains - using query: {query}")

    # use "verbose" to pull extra info
    if "verbose" in request.rel_url.query and request.rel_url.query["verbose"]:
        verbose = True
    else:
        verbose = False

    log.info(f"get_domains for: {prefix} verbose: {verbose}")

    if not prefix.startswith('/'):
        msg = "Prefix must start with '/'"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    limit = None
    if "Limit" in request.rel_url.query:
        try:
            limit = int(request.rel_url.query["Limit"])
            log.debug(f"get_domains - using Limit: {limit}")
        except ValueError:
            msg = "Bad Request: Expected int type for limit"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        log.debug("get_domains - no limit")


    marker = None
    if "Marker" in request.rel_url.query:
        marker = request.rel_url.query["Marker"]
        log.debug(f"get_domains - got marker request param: {marker}")

    if "bucket" in params:
        bucket = params["bucket"]
    elif "X-Hdf-bucket" in request.headers:
        bucket = request.headers["X-Hdf-bucket"]
    elif "bucket_name" in app and app["bucket_name"]:
        bucket = app["bucket_name"]
    else:
        bucket = None
    if not bucket:
        msg = "no bucket specified for request"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    log.info(f"get_domains - prefix: {prefix} bucket: {bucket}")

    # list the S3 keys for this prefix
    domainNames = []
    if prefix == "/" and config.get("top_level_domains"):
        domainNames = config.get("top_level_domains")
        if isinstance(domainNames, str):
            # split multiple domains by comma char
            domainNames = domainNames.split(',')
    else:
        s3prefix = prefix[1:]
        log.debug(f"get_domains - listing S3 keys for {s3prefix}")
        s3keys = await getStorKeys(app, include_stats=False, prefix=s3prefix, deliminator='/', bucket=bucket)
        log.debug(f"get_domains - getS3Keys returned: {len(s3keys)} keys")

        for s3key in s3keys:
            if s3key[-1] != '/':
                log.debug(f"get_domains - ignoring key: {s3key}")
                continue
            if len(s3key) > 1 and s3key[-2] == '/':
                # trim off double slash
                s3key = s3key[:-1]
            log.debug(f"get_domains - got s3key: {s3key}")
            domain = "/" + s3key[:-1]
            if regex:
                # do a pattern match on the basename
                basename = op.basename(domain)
                if not regex.match(basename):
                    log.debug(f"get_domains - {basename} did not match regex: {pattern}")
                    continue

            if marker:
                log.debug(f"get_domains - compare marker {marker} and {domain}")
                if marker == domain:
                    log.debug("get_domains - clearing marker")
                    marker = None
                continue

            log.debug(f"get_domains - adding domain: {domain} to domain list")
            domainNames.append(domain)

            if limit and len(domainNames) == limit:
                # got to requested limit
                break


    # get domain info for each domain
    domains = []
    if query:
        get_root = True
    else:
        get_root = False
    crawler = FolderCrawler(app, domainNames, bucket=bucket, get_root=get_root, verbose=verbose)
    await crawler.crawl()

    if query:
        log.info(f"get_domains - proccessing query: {query}")
        try:
            parser = BooleanParser(query)
        except IndexError as ie:
            log.warn(f"get_domains - domaing query syntax error: {ie}")
            raise HTTPBadRequest(reason="Invalid query expression")
        attr_names = parser.getVariables()
        log.info(f"get_domains - query variables: {attr_names}")
        # remove any domains from dict for which the attribute query is false
        domain_keys = list(crawler._domain_dict.keys())
        log.debug(f"get_domains - querying through {len(domain_keys)}")

        for domain in domain_keys:
            log.debug(f"get_domains - query search for: {domain}")
            domain_json = crawler._domain_dict[domain]
            if "root" not in domain_json:
                log.debug(f"get_domains - skipping folder: {domain} for attribute query search")
                del domain_keys[domain]
                continue

            root_id = domain_json["root"]
            if root_id not in crawler._group_dict:
                log.warn(f"Expected to find {root_id} in crawler group dict")
                continue
            root_json = crawler._group_dict[root_id]
            attributes = root_json["attributes"]
            variable_dict = {}
            for attr_name in attr_names:
                if attr_name not in attributes:
                    log.debug(f"{attr_name} not found")
                    del crawler._domain_dict[domain]
                    continue
                attr_json = attributes[attr_name]
                log.debug(f"{attr_name}: {attr_json}")
                attr_type = attr_json["type"]
                attr_type_class = attr_type["class"]
                if attr_type_class not in ('H5T_INTEGER', 'H5T_FLOAT', 'H5T_STRING'):
                    log.debug("unable to query non-primitive attribute class: {attr_type_class}")
                    del crawler._domain_dict[domain]
                    continue
                attr_shape = attr_json["shape"]
                attr_shape_class = attr_shape["class"]
                if attr_shape_class == 'H5S_SCALAR':
                    variable_dict[attr_name] = attr_json["value"]
                else:
                    log.debug("get_domains - unable to query non-scalar attributes")
                    del crawler._domain_dict[domain]
                    continue
            # evaluate the boolean expression
            if len(variable_dict) == len(attr_names):
                # we have all the variables, evaluate
                parser_value = False
                try:
                    parser_value = parser.evaluate(variable_dict)
                except TypeError as te:
                    log.warn(f"get_domains - evaluate {query} for {domain} but got error: {te}")
                if parser_value:
                    log.info(f"get_domains - {domain} passed query test")
                else:
                    log.debug(f"get_domains - {domain} failed query test")
                    del crawler._domain_dict[domain]

    for domain in domainNames:
        if domain in crawler._domain_dict:
            domain_json = crawler._domain_dict[domain]
            # mixin domain name
            domain_json["name"] = domain
            domains.append(domain_json)
        else:
            if not query:
                log.warn(f"get_domains - domain: {domain} not found in crawler dict")

    return domains


async def GET_Domains(request):
    """HTTP method to return JSON for child domains of given domain"""
    log.request(request)
    app = request.app

    (username, pswd) = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domains = await get_domains(request)

    rsp_json = {"domains": domains}
    rsp_json["hrefs"] = []
    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

async def GET_Domain(request):
    """HTTP method to return JSON for given domain"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

    (username, pswd) = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domain = None
    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        log.warn(f"Invalid domain: {domain}")
        raise HTTPBadRequest(reason="Invalid domain name")
    bucket = getBucketForDomain(domain)
    log.debug(f"GET_Domain domain: {domain} bucket: {bucket}")

    if not bucket and not config.get("bucket_name"):
        # no bucket defined, raise 400
        msg = "Bucket not provided"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    verbose = False
    if "verbose" in params and params["verbose"]:
        verbose = True

    if not domain:
        log.info("no domain passed in, returning all top-level domains")
        # no domain passed in, return top-level domains for this request
        domains = await get_domains(request)
        rsp_json = {"domains": domains}
        rsp_json["hrefs"] = []
        resp = await jsonResponse(request, rsp_json)
        log.response(request, resp=resp)
        return resp

    log.info(f"got domain: {domain}")

    domain_json = await getDomainJson(app, domain, reload=True)

    if domain_json is None:
        log.warn(f"domain: {domain} not found")
        raise HTTPNotFound()

    if 'owner' not in domain_json:
        log.error("No owner key found in domain")
        raise HTTPInternalServerError()

    if 'acls' not in domain_json:
        log.error("No acls key found in domain")
        raise HTTPInternalServerError()

    log.debug(f"got domain_json: {domain_json}")
    # validate that the requesting user has permission to read this domain
    aclCheck(app, domain_json, "read", username)  # throws exception if not authorized

    if "h5path" in params:
        # if h5path is passed in, return object info for that path
        #   (if exists)
        h5path = params["h5path"]
        root_id = domain_json["root"]
        obj_id = await getObjectIdByPath(app, root_id, h5path, bucket=bucket)  # throws 404 if not found
        log.info(f"get obj_id: {obj_id} from h5path: {h5path}")
        # get authoritative state for object from DN (even if it's in the meta_cache).
        obj_json = await getObjectJson(app, obj_id, refresh=True, bucket=bucket)
        obj_json["domain"] = domain
        # Not bothering with hrefs for h5path lookups...
        resp = await jsonResponse(request, obj_json)
        log.response(request, resp=resp)
        return resp

    # return just the keys as per the REST API
    rsp_json = await get_domain_response(app, domain_json, bucket=bucket, verbose=verbose)

    # include domain objects if requested
    if "getobjs" in params and params["getobjs"] and "root" in domain_json:
        root_id = domain_json["root"]
        include_attrs = False
        if "include_attrs" in params and params["include_attrs"]:
            include_attrs = True
        domain_objs = await getDomainObjects(app, root_id, include_attrs=include_attrs, bucket=bucket)
        if domain_objs:
            rsp_json["domain_objs"] = domain_objs

    # include dn_ids if requested
    if "getdnids" in params and params["getdnids"]:
        rsp_json["dn_ids"] = app["dn_ids"]

    hrefs = []
    hrefs.append({'rel': 'self', 'href': getHref(request, '/')})
    if "root" in domain_json:
        root_uuid = domain_json["root"]
        hrefs.append({'rel': 'database', 'href': getHref(request, '/datasets')})
        hrefs.append({'rel': 'groupbase', 'href': getHref(request, '/groups')})
        hrefs.append({'rel': 'typebase', 'href': getHref(request, '/datatypes')})
        hrefs.append({'rel': 'root', 'href': getHref(request, '/groups/' + root_uuid)})

    hrefs.append({'rel': 'acls', 'href': getHref(request, '/acls')})
    parent_domain = getParentDomain(domain)
    if not parent_domain or getPathForDomain(parent_domain) == '/':
        is_toplevel = True
    else:
        is_toplevel = False
    log.debug(f"href parent domain: {parent_domain}")
    if not is_toplevel:
        hrefs.append({'rel': 'parent', 'href': getHref(request, '/', domain=parent_domain)})

    rsp_json["hrefs"] = hrefs
    # mixin limits, version
    domain_json["limits"] = getLimits()
    domain_json["compressors"] = getCompressors()
    domain_json["version"] = getVersion()
    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

async def doFlush(app, root_id, bucket=None):
    """ return wnen all DN nodes have wrote any pending changes to S3"""
    log.info(f"doFlush {root_id}")
    params = {"flush": 1}
    if bucket:
        params["bucket"] = bucket
    client = get_http_client(app)
    dn_urls = app["dn_urls"]
    dn_ids = []
    log.debug(f"doFlush - dn_urls: {dn_urls}")
    failed_count = 0

    try:
        tasks = []
        for dn_url in dn_urls:
            req = dn_url + "/groups/" + root_id
            task = asyncio.ensure_future(client.put(req, params=params))
            tasks.append(task)
        done, pending = await asyncio.wait(tasks)
        if pending:
            # should be empty since we didn't use return_when parameter
            log.error("doFlush - got pending tasks")
            raise HTTPInternalServerError()
        for task in done:
            if task.exception():
                log.warn(f"doFlush - task had exception: {type(task.exception())}")
                failed_count += 1
            else:
                clientResponse = task.result()
                if clientResponse.status != 200:
                    log.warn(f"doFlush - expected 204 but got: {clientResponse.status}")
                    failed_count += 1
                else:
                    json_rsp = await clientResponse.json()
                    log.debug(f"PUT /groups rsp: {json_rsp}")
                    if json_rsp and "id" in json_rsp:
                        dn_ids.append(json_rsp["id"])
                    else:
                        log.error("expected dn_id in flush response from DN")
    except ClientError as ce:
        log.error(f"doFlush - ClientError for http_put('/groups/{root_id}'): {str(ce)}")
        raise HTTPInternalServerError()
    except CancelledError as cle:
        log.error(f"doFlush - CancelledError '/groups/{root_id}'): {str(cle)}")
        raise HTTPInternalServerError()
    log.info(f"doFlush for {root_id} complete, failed: {failed_count} out of {len(dn_urls)}")
    if failed_count > 0:
        log.error(f"doFlush fail count: {failed_count} returning 500")
        raise HTTPInternalServerError()
    else:
        log.info("doFlush no fails, returning dn ids")
        return dn_ids



async def PUT_Domain(request):
    """HTTP method to create a new domain"""
    log.request(request)
    app = request.app
    params = request.rel_url.query
    log.debug(f"PUT_domain params: {dict(params)}")
    # verify username, password
    username, pswd = getUserPasswordFromRequest(request) # throws exception if user/password is not valid
    await validateUserPassword(app, username, pswd)

    # inital perms for owner and default
    owner_perm = {'create': True, 'read': True, 'update': True, 'delete': True, 'readACL': True, 'updateACL': True }
    default_perm = {'create': False, 'read': True, 'update': False, 'delete': False, 'readACL': False, 'updateACL': False }

    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    bucket = getBucketForDomain(domain)

    log.info(f"PUT domain: {domain}, bucket: {bucket}")

    body = None
    if request.has_body:
        body = await request.json()
        log.debug(f"PUT domain with body: {body}")

    if ("getdnids" in params and params["getdnids"]) or (body and "getdnids" in body and body["getdnids"]):
        getdnids = True
    else:
        getdnids = False

    if ("flush" in params and params["flush"]) or (body and "flush" in body and body["flush"]):
        # flush domain - update existing domain rather than create a new resource
        log.info(f"Flush for domain: {domain}")
        domain_json = await getDomainJson(app, domain, reload=True)
        log.debug(f"got domain_json: {domain_json}")

        if domain_json is None:
            log.warn(f"domain: {domain} not found")
            raise HTTPNotFound()

        if 'owner' not in domain_json:
            log.error("No owner key found in domain")
            raise HTTPInternalServerError()

        if 'acls' not in domain_json:
            log.error("No acls key found in domain")
            raise HTTPInternalServerError()

        aclCheck(app, domain_json, "update", username)  # throws exception if not allowed
        rsp_json = None
        if "root" in domain_json:
            # nothing to to do for folder objects
            dn_ids = await doFlush(app, domain_json["root"], bucket=bucket)
            # flush  successful
            if dn_ids and getdnids:
                # no fails, but return list of dn ids
                rsp_json = {"dn_ids": dn_ids}
                log.debug(f"returning dn_ids for PUT domain: {dn_ids}")
                status_code = 200
            else: status_code = 204
        else:
            log.info("flush called on folder, ignoring")
            status_code = 204
        resp = await jsonResponse(request, rsp_json, status=status_code)
        log.response(request, resp=resp)
        return resp

    if "rescan" in params and params["rescan"]:
        # refresh scan info for the domain
        log.info(f"rescan for domain: {domain}")
        domain_json = await getDomainJson(app, domain, reload=True)
        log.debug(f"got domain_json: {domain_json}")
        if "root" in domain_json:
            # nothing to update for folders
            root_id = domain_json["root"]
            if not isValidUuid(root_id):  
                log.error(f"domain: {domain} with invalid  root id: {root_id}")
                raise HTTPInternalServerError()
            if not isSchema2Id(root_id):
                msg = "rescan not supported for v1 ids"
                log.info(msg)
                raise HTTPBadRequest(reashon=msg)
            aclCheck(app, domain_json, "update", username)  # throws exception if not authorized
            log.info(f"notify_root: {root_id}")
            notify_req = getDataNodeUrl(app, root_id) + "/roots/" + root_id
            post_params = {}
            if bucket:
                post_params["bucket"] = bucket
            await http_post(app, notify_req, data={}, params=post_params)
            resp = json_response(None, status=204)  # No Content response
            return resp

    is_folder = False
    owner = username
    linked_domain = None
    linked_bucket = None
    root_id = None

    if body and "folder" in body:
        if body["folder"]:
            is_folder = True
    if body and "owner" in body:
        owner = body["owner"]
    if body and "linked_domain" in body:
        if is_folder:
            msg = "Folder domains can not be used for links"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        linked_domain = body["linked_domain"]
        if not isValidDomain(linked_domain):
            msg = f"linked_domain: {linked_domain} is not valid"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if "linked_bucket" in body:
            linked_bucket = body["linked_bucket"]
        elif bucket:
            linked_bucket = bucket
        elif  "bucket_name" in request.app and request.app["bucket_name"]:
            linked_bucket = request.app["bucket_name"]
        else:
            linked_bucket = None

        if not linked_bucket:
            msg = "Could not determine bucket for linked domain"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    if owner != username and not isAdminUser(app, username):
        log.warn("Only admin users are allowed to set owner for new domains")
        raise HTTPForbidden()


    parent_domain = getParentDomain(domain)
    log.debug(f"Parent domain: [{parent_domain}]")

    if not parent_domain or getPathForDomain(parent_domain) == '/':
        is_toplevel = True
    else:
        is_toplevel = False

    if is_toplevel and not is_folder:
        msg = "Only folder domains can be created at the top-level"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if is_toplevel and not isAdminUser(app, username):
        msg = "creation of top-level domains is only supported by admin users"
        log.warn(msg)
        raise HTTPForbidden()


    parent_json = None
    if not is_toplevel:
        try:
            parent_json = await getDomainJson(app, parent_domain, reload=True)
        except ClientResponseError as ce:
            if ce.code == 404:
                msg = f"Parent domain: {parent_domain} not found"
                log.warn(msg)
                raise HTTPNotFound()
            elif ce.code == 410:
                msg = f"Parent domain: {parent_domain} removed"
                log.warn(msg)
                raise HTTPGone()
            else:
                log.error(f"Unexpected error: {ce.code}")
                raise HTTPInternalServerError()

        log.debug(f"parent_json {parent_domain}: {parent_json}")
        if "root" in parent_json and parent_json["root"]:
            msg = "Parent domain must be a folder"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    if parent_json:
        aclCheck(app, parent_json, "create", username)  # throws exception if not allowed

    if linked_domain:
        linked_json = await getDomainJson(app, linked_bucket + linked_domain, reload=True)
        log.debug(f"got linked json: {linked_json}")
        if "root" not in linked_json:
            msg = "Folder domains cannot ber used as link target"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        root_id = linked_json["root"]
        aclCheck(app, linked_json, "read", username)
        aclCheck(app, linked_json, "delete", username)  # TBD - why is delete needed?
    else:
        linked_json = None

    if not is_folder and not linked_json:
        # create a root group for the new domain
        root_id = createObjId("roots")
        log.debug(f"new root group id: {root_id}")
        group_json = {"id": root_id, "root": root_id, "domain": domain }
        log.debug(f"create group for domain, body: {group_json}")

        # create root group
        req = getDataNodeUrl(app, root_id) + "/groups"
        post_params = {}
        bucket = getBucketForDomain(domain)
        if bucket:
            post_params["bucket"] = bucket
        try:
            group_json = await http_post(app, req, data=group_json, params=post_params)
        except ClientResponseError as ce:
            msg="Error creating root group for domain -- " + str(ce)
            log.error(msg)
            raise HTTPInternalServerError()
    else:
        log.debug("no root group, creating folder")

    domain_acls = {}
    if parent_json and "acls" in parent_json:
        parent_acls = parent_json["acls"]
        for user_name in parent_acls:
            if user_name == "default":
                # will be created below if default_public is iset
                continue
            if user_name == owner:
                # will be created below
                continue
            if isAdminUser(app, user_name):
                # no need to copy admin ACLs since admin have full authority
                continue
            acl = parent_acls[user_name]
            has_action = False
            # don't copy ACL if all actions are False
            for k in ("create", "read", "update", "delete", "readACL", "updateACL"):
                if acl[k]:
                    has_action = True
                    break
            if has_action:
                # inherit any acls that are not default or owner acls
                domain_acls[user_name] = parent_acls[user_name]

    domain_json = { }

    # owner gets full control
    domain_acls[owner] = owner_perm
    if config.get("default_public") or is_folder:
        # this will make the domain public readable
        log.debug(f"adding default perm for domain: {domain}")
        domain_acls["default"] =  default_perm

    # construct dn request to create new domain
    req = getDataNodeUrl(app, domain)
    req += "/domains"
    body = { "owner": owner, "domain": domain }
    body["acls"] = domain_acls

    if root_id:
        body["root"] = root_id

    log.debug(f"creating domain: {domain} with body: {body}")
    try:
        domain_json = await http_put(app, req, data=body)
    except ClientResponseError as ce:
        msg="Error creating domain state -- " + str(ce)
        log.error(msg)
        raise HTTPInternalServerError()

    # domain creation successful
    # mixin limits
    domain_json["limits"] = getLimits()
    domain_json["compressors"] = getCompressors()
    domain_json["version"] = getVersion()

    # put  successful
    if getdnids:
        # mixin list of dn ids
        dn_ids = app["dn_ids"]
        domain_json["dn_ids"] = dn_ids
        log.debug(f"returning dn_ids for PUT domain: {dn_ids}")
    resp = await jsonResponse(request, domain_json, status=201)
    log.response(request, resp=resp)
    return resp

async def DELETE_Domain(request):
    """HTTP method to delete a domain resource"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

    meta_only = False  # if True, just delete the meta cache value
    keep_root = False
    if request.has_body:
        body = await request.json()
        if "meta_only" in body:
            meta_only = body["meta_only"]
        if "keep_root" in body:
            keep_root = body["keep_root"]
    else:
        if "meta_only" in params:
            meta_only = params["meta_only"]
        if "keep_root" in params:
            keep_root = params["keep_root"]

    domain = None
    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        log.warn(f"Invalid domain: {domain}")
        raise HTTPBadRequest(reason="Invalid domain name")
    bucket = getBucketForDomain(domain)
    log.debug(f"GET_Domain domain: {domain}")

    if not domain:
        msg = "No domain given"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    log.info(f"meta_only domain delete: {meta_only}")
    if meta_only:
        # remove from domain cache if present
        domain_cache = app["domain_cache"]
        if domain in domain_cache:
            log.info(f"deleting {domain} from domain_cache")
            del domain_cache[domain]
        resp = await jsonResponse(request, {})
        return resp

    username, pswd = getUserPasswordFromRequest(request)
    await validateUserPassword(app, username, pswd)

    parent_domain = getParentDomain(domain)
    if not parent_domain or getPathForDomain(parent_domain) == '/':
        is_toplevel = True
    else:
        is_toplevel = False

    if is_toplevel and not isAdminUser(app, username):
        msg = "Deletion of top-level domains is only supported by admin users"
        log.warn(msg)
        raise HTTPForbidden()

    try:
        domain_json = await getDomainJson(app, domain, reload=True)
    except ClientResponseError as ce:
        if ce.code == 404:
            log.warn("domain not found")
            raise HTTPNotFound()
        elif ce.code == 410:
            log.warn("domain has been removed")
            raise HTTPGone()
        else:
            log.error(f"unexpected error: {ce.code}")
            raise HTTPInternalServerError()

    aclCheck(app, domain_json, "delete", username)  # throws exception if not allowed

    # check for sub-objects if this is a folder
    if "root" not in domain_json:
        index = domain.find('/')
        s3prefix = domain[(index+1):] + '/'
        log.info(f"checking s3key with prefix: {s3prefix} in bucket: {bucket}")
        s3keys = await getStorKeys(app, include_stats=False, prefix=s3prefix, deliminator='/', bucket=bucket)
        for s3key in s3keys:
            if s3key.endswith("/"):
                log.warn(f"attempt to delete folder {domain} with sub-items")
                log.debug(f"got prefix: {s3keys[0]}")
                raise HTTPConflict(reason="folder has sub-items")


    req = getDataNodeUrl(app, domain)
    req += "/domains"

    params = {} # for http_delete requests to DN nodes
    params["domain"] = domain
    rsp_json = await http_delete(app, req, params=params)

    if "root" in domain_json and not keep_root:
        # delete the root group

        root_id = domain_json["root"]
        req = getDataNodeUrl(app, root_id)
        req += "/groups/" + root_id
        await http_delete(app, req, params=params)

    # remove from domain cache if present
    domain_cache = app["domain_cache"]
    if domain in domain_cache:
        del domain_cache[domain]

    # delete domain cache from other sn_urls
    """
    sn_urls = app["sn_urls"]
    log.debug(f"sn_urls: {sn_urls}")
    log.debug(f"node_number: {app['node_number']}")
    params = {}
    params["domain"] = getPathForDomain(domain)
    params["bucket"] = getBucketForDomain(domain)
    params["meta_only"] = 1  # can't pass booleans as params, so use 1 instead of True
    for node_no in sn_urls:
        log.debug(f"node_no: {node_no}")
        if node_no == app["node_number"]:
            continue # don't send to ourselves
        sn_url = sn_urls[node_no]
        req = sn_url + "/"
        log.info(f"sending sn request: {req}")
        try:
            sn_rsp = await http_delete(app, req, params=params)
            log.info(f"{req} response: {sn_rsp}")
        except ClientResponseError as ce:
            log.warn(f"got error for sn_delete: {ce}")
    """

    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

async def GET_ACL(request):
    """HTTP method to return JSON for given domain/ACL"""
    log.request(request)
    app = request.app

    acl_username = request.match_info.get('username')
    if not acl_username:
        msg = "Missing username for ACL"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    (username, pswd) = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    # use reload to get authoritative domain json
    try:
        domain_json = await getDomainJson(app, domain, reload=True)
    except ClientResponseError as ce:
        if ce.code in (404,410):
            msg = "domain not found"
            log.warn(msg)
            raise HTTPNotFound()
        else:
            log.error(f"unexpected error: {ce.code}")
            raise HTTPInternalServerError()

    # validate that the requesting user has permission to read ACLs in this domain
    if acl_username in (username, "default"):
        # allow read access for a users on ACL, or default
        aclCheck(app, domain_json, "read", username)  # throws exception if not authorized
    else:
        aclCheck(app, domain_json, "readACL", username)  # throws exception if not authorized

    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HTTPInternalServerError()

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HTTPInternalServerError()

    acls = domain_json["acls"]

    log.debug(f"got domain_json: {domain_json}")

    if acl_username not in acls:
        msg = f"acl for username: [{acl_username}] not found"
        log.warn(msg)
        raise HTTPNotFound()

    acl = acls[acl_username]
    acl_rsp = {}
    for k in acl.keys():
        acl_rsp[k] = acl[k]
    acl_rsp["userName"] = acl_username

    # return just the keys as per the REST API
    rsp_json = { }
    rsp_json["acl"] = acl_rsp
    hrefs = []
    hrefs.append({'rel': 'self', 'href': getHref(request, '/acls')})
    if "root" in domain_json:
        hrefs.append({'rel': 'root', 'href': getHref(request, '/groups/' + domain_json["root"])})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')})
    hrefs.append({'rel': 'owner', 'href': getHref(request, '/')})
    rsp_json["hrefs"] = hrefs

    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

async def GET_ACLs(request):
    """HTTP method to return JSON for domain/ACLs"""
    log.request(request)
    app = request.app

    (username, pswd) = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    # use reload to get authoritative domain json
    try:
        domain_json = await getDomainJson(app, domain, reload=True)
    except ClientResponseError:
        log.warn("domain not found")
        log.warn(msg)
        raise HTTPNotFound()

    if 'owner' not in domain_json:
        log.error("No owner key found in domain")
        raise HTTPInternalServerError()

    if 'acls' not in domain_json:
        log.error("No acls key found in domain")
        raise HTTPInternalServerError()

    acls = domain_json["acls"]

    log.debug(f"got domain_json: {domain_json}")
    # validate that the requesting user has permission to read this domain
    aclCheck(app, domain_json, "readACL", username)  # throws exception if not authorized

    acl_list = []
    acl_usernames = list(acls.keys())
    acl_usernames.sort()
    for acl_username in acl_usernames:
        entry = {"userName": acl_username}
        acl = acls[acl_username]

        for k in acl.keys():
            entry[k] = acl[k]
        acl_list.append(entry)
    # return just the keys as per the REST API
    rsp_json = { }
    rsp_json["acls"] = acl_list

    hrefs = []
    hrefs.append({'rel': 'self', 'href': getHref(request, '/acls')})
    if "root" in domain_json:
        hrefs.append({'rel': 'root', 'href': getHref(request, '/groups/' + domain_json["root"])})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')})
    hrefs.append({'rel': 'owner', 'href': getHref(request, '/')})
    rsp_json["hrefs"] = hrefs

    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

async def PUT_ACL(request):
    """HTTP method to add a new ACL for a domain"""
    log.request(request)
    app = request.app

    acl_username = request.match_info.get('username')
    if not acl_username:
        msg = "Missing username for ACL"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    (username, pswd) = getUserPasswordFromRequest(request)
    await validateUserPassword(app, username, pswd)

    if not request.has_body:
        msg = "PUT ACL with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    body = await request.json()
    acl_keys = getAclKeys()

    for k in body.keys():
        if k not in acl_keys:
            msg = f"Unexpected key in request body: {k}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if body[k] not in (True, False):
            msg = f"Unexpected value for key in request body: {k}"
            log.warn(k)
            raise HTTPBadRequest(reason=msg)

    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    # don't use app["domain_cache"]  if a direct domain request is made
    # as opposed to an implicit request as with other operations, query
    # the domain from the authoritative source (the dn node)
    req = getDataNodeUrl(app, domain)
    req += "/acls/" + acl_username
    log.info(f"sending dn req: {req}")
    body["domain"] = domain

    put_rsp = await http_put(app, req, data=body)
    log.info("PUT ACL resp: " + str(put_rsp))

    # ACL update successful
    resp = await jsonResponse(request, put_rsp, status=201)
    log.response(request, resp=resp)
    return resp


async def GET_Datasets(request):
    """HTTP method to return dataset collection for given domain"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

    (username, pswd) = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    # verify the domain
    try:
        domain_json = await getDomainJson(app, domain)
    except ClientResponseError as ce:
        if ce.code == 404:
            msg = f"Domain: {domain} not found"
            log.warn(msg)
            raise HTTPNotFound()
        elif ce.code == 410:
            msg = f"Domain: {domain} removed"
            log.warn(msg)
            raise HTTPGone()
        else:
            log.error(f"Unexpected error: {ce.code}")
            raise HTTPInternalServerError()
        msg = "domain not found"
        log.warn(msg)
        raise HTTPNotFound()

    if 'owner' not in domain_json:
        log.error("No owner key found in domain")
        raise HTTPInternalServerError()

    if 'acls' not in domain_json:
        log.error("No acls key found in domain")
        raise HTTPInternalServerError()

    log.debug(f"got domain_json: {domain_json}")
    # validate that the requesting user has permission to read this domain
    aclCheck(app, domain_json, "read", username)  # throws exception if not authorized

    limit = None
    if "Limit" in params:
        try:
            limit = int(params["Limit"])
        except ValueError:
            msg = "Bad Request: Expected int type for limit"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    marker = None
    if "Marker" in params:
        marker = params["Marker"]

    obj_ids = []
    if "root" in domain_json or domain_json["root"]:
        # get the dataset collection list
        collections = await get_collections(app, domain_json["root"])
        objs = collections["datasets"]
        obj_ids = getIdList(objs, marker=marker, limit=limit)

    log.debug(f"returning obj_ids: {obj_ids}")

    # create hrefs
    hrefs = []
    hrefs.append({'rel': 'self', 'href': getHref(request, '/datasets')})
    if "root" in domain_json:
        root_uuid = domain_json["root"]
        hrefs.append({'rel': 'root', 'href': getHref(request, '/groups/' + root_uuid)})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')})

    # return obj ids and hrefs
    rsp_json = { }
    rsp_json["datasets"] = obj_ids
    rsp_json["hrefs"] = hrefs

    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

async def GET_Groups(request):
    """HTTP method to return groups collection for given domain"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

    (username, pswd) = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    # use reload to get authoritative domain json
    try:
        domain_json = await getDomainJson(app, domain, reload=True)
    except ClientResponseError as ce:
        if ce.code == 404:
            msg = "domain not found"
            log.warn(msg)
            raise HTTPNotFound()
        else:
            log.error(f"Unexpected error: {ce.code}")
            raise HTTPInternalServerError()

    if 'owner' not in domain_json:
        log.error("No owner key found in domain")
        raise HTTPInternalServerError()

    if 'acls' not in domain_json:
        log.error("No acls key found in domain")
        raise HTTPInternalServerError()

    log.debug(f"got domain_json: {domain_json}")
    # validate that the requesting user has permission to read this domain
    aclCheck(app, domain_json, "read", username)  # throws exception if not authorized

    # get the groups collection list
    limit = None
    if "Limit" in params:
        try:
            limit = int(params["Limit"])
        except ValueError:
            msg = "Bad Request: Expected int type for limit"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    marker = None
    if "Marker" in params:
        marker = params["Marker"]

    obj_ids = []
    if "root" in domain_json or domain_json["root"]:
        # get the groups collection list
        collections = await get_collections(app, domain_json["root"])
        objs = collections["groups"]
        obj_ids = getIdList(objs, marker=marker, limit=limit)

    # create hrefs
    hrefs = []
    hrefs.append({'rel': 'self', 'href': getHref(request, '/groups')})
    if "root" in domain_json:
        root_uuid = domain_json["root"]
        hrefs.append({'rel': 'root', 'href': getHref(request, '/groups/' + root_uuid)})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')})

    # return obj ids and hrefs
    rsp_json = { }
    rsp_json["groups"] = obj_ids
    rsp_json["hrefs"] = hrefs

    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

async def GET_Datatypes(request):
    """HTTP method to return datatype collection for given domain"""
    log.request(request)
    app = request.app
    params = request.rel_url.query

    (username, pswd) = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    try:
        domain = getDomainFromRequest(request)
    except ValueError:
        msg = "Invalid domain"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    # use reload to get authoritative domain json
    try:
        domain_json = await getDomainJson(app, domain, reload=True)
    except ClientResponseError as ce:
        if ce.code in (404, 410):
            msg = "domain not found"
            log.warn(msg)
            raise HTTPNotFound()
        else:
            log.error(f"Unexpected Error: {ce.code})")
            raise HTTPInternalServerError()

    if 'owner' not in domain_json:
        log.error("No owner key found in domain")
        raise HTTPInternalServerError()

    if 'acls' not in domain_json:
        log.error("No acls key found in domain")
        raise HTTPInternalServerError()

    log.debug(f"got domain_json: {domain_json}")
    # validate that the requesting user has permission to read this domain
    aclCheck(app, domain_json, "read", username)  # throws exception if not authorized

    limit = None
    if "Limit" in params:
        try:
            limit = int(params["Limit"])
        except ValueError:
            msg = "Bad Request: Expected int type for limit"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    marker = None
    if "Marker" in params:
        marker = params["Marker"]

    # get the datatype collection list
    obj_ids = []
    if "root" in domain_json or domain_json["root"]:
        # get the groups collection list
        collections = await get_collections(app, domain_json["root"])
        objs = collections["datatypes"]
        obj_ids = getIdList(objs, marker=marker, limit=limit)

    # create hrefs
    hrefs = []
    hrefs.append({'rel': 'self', 'href': getHref(request, '/datatypes')})
    if "root" in domain_json:
        root_uuid = domain_json["root"]
        hrefs.append({'rel': 'root', 'href': getHref(request, '/groups/' + root_uuid)})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')})

    # return obj ids and hrefs
    rsp_json = { }
    rsp_json["datatypes"] = obj_ids
    rsp_json["hrefs"] = hrefs

    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp
