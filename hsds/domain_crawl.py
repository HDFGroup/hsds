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
# domain crawler
#

import asyncio

from aiohttp.web_exceptions import HTTPServiceUnavailable, HTTPConflict, HTTPBadRequest
from aiohttp.web_exceptions import HTTPInternalServerError, HTTPNotFound, HTTPGone


from .util.idUtil import getCollectionForId, getDataNodeUrl
from .util.httpUtil import http_put

from .servicenode_lib import getObjectJson, getAttributes
from . import hsds_logger as log


class DomainCrawler:
    def __init__(
        self,
        app,
        objs,
        action="get_obj",
        params=None,
        max_tasks=40,
        max_objects_limit=0,
    ):
        log.info(f"DomainCrawler.__init__  root_id: {len(objs)} objs")
        self._app = app
        self._action = action
        self._max_objects_limit = max_objects_limit
        self._params = params
        self._max_tasks = max_tasks
        self._q = asyncio.Queue()
        self._obj_dict = {}
        self.seen_ids = set()
        if not objs:
            log.error("no objs for crawler to crawl!")
            raise ValueError()

        for obj_id in objs:
            log.debug(f"adding {obj_id} to the queue")
            self._q.put_nowait(obj_id)
        if isinstance(objs, dict):
            self._objs = objs
        else:
            self._objs = None

    async def get_attributes(self, obj_id, attr_names):
        # get the given attributes for the obj_id
        log.debug(f"get_attributes for {obj_id}, {len(attr_names)} attributes")

        kwargs = {}
        for key in ("include_data", "ignore_nan", "bucket"):
            if key in self._params:
                kwargs[key] = self._params[key]
        kwargs["attr_names"] = attr_names
        log.debug(f"using kwargs: {kwargs}")

        status = 200
        # make sure to catch all expected exceptions, otherwise
        # the task will never complete
        try:
            attributes = await getAttributes(self._app, obj_id, **kwargs)
        except HTTPBadRequest:
            status = 400
        except HTTPNotFound:
            status = 404
        except HTTPGone:
            status = 410
        except HTTPServiceUnavailable:
            status = 503
        except HTTPInternalServerError:
            status = 500
        except Exception as e:
            log.error(f"unexpected exception from post request: {e}")
            status = 500

        if status == 200:
            log.debug(f"got attributes: {attributes}")
            self._obj_dict[obj_id] = attributes
        else:
            log.warn(f"Domain crawler - got {status} status for obj_id {obj_id}")
            self._obj_dict[obj_id] = {"status": status}

    async def put_attributes(self, obj_id, attr_items):
        # write the given attributes for the obj_id
        log.debug(f"put_attributes for {obj_id}, {len(attr_items)} attributes")
        req = getDataNodeUrl(self._app, obj_id)
        collection = getCollectionForId(obj_id)
        req += f"/{collection}/{obj_id}/attributes"
        params = {}
        if "bucket" in self._params:
            params["bucket"] = self._params["bucket"]
        data = {"attributes": attr_items}
        status = None
        put_rsp = None
        try:
            put_rsp = await http_put(self._app, req, data=data, params=params)
        except HTTPConflict:
            log.warn("DomainCrawler - got HTTPConflict from http_put")
            status = 409
        except HTTPServiceUnavailable:
            status = 503
        except HTTPInternalServerError:
            status = 500
        except Exception as e:
            log.error(f"unexpected exception {e}")

        if put_rsp is not None:
            log.info(f"PUT Attributes resp: {put_rsp}")
            if "status" in put_rsp:
                status = put_rsp["status"]
            else:
                status = 201
        log.debug(f"DomainCrawler fetch for {obj_id} - returning status: {status}")
        self._obj_dict[obj_id] = {"status": status}

    async def get_obj_json(self, obj_id):
        """ get the given obj_json for the obj_id.
            for each group found, search the links if include_links is set """
        log.debug(f"get_obj_json: {obj_id}")
        collection = getCollectionForId(obj_id)
        kwargs = {}

        for k in ("include_links", "include_attrs", "bucket"):
            if k in self._params:
                kwargs[k] = self._params[k]
        if collection == "groups" and self._params.get("follow_links"):
            follow_links = True
            kwargs["include_links"] = True  # get them so we can follow them
        else:
            follow_links = False
        if follow_links or self._params.get("include_attrs"):
            kwargs["refresh"] = True  # don't want a cached version in this case

        log.debug(f"follow_links: {follow_links}")
        log.debug(f"getObjectJson kwargs: {kwargs}")
        obj_json = None
        status = 200
        try:
            obj_json = await getObjectJson(self._app, obj_id, **kwargs)
        except HTTPNotFound:
            status = 404
        except HTTPServiceUnavailable:
            status = 503
        except HTTPInternalServerError:
            status = 500
        except Exception as e:
            log.error(f"unexpected exception {e}")
            status = 500
        log.debug(f"getObjectJson status: {status}")

        if obj_json is None:
            msg = f"DomainCrawler - getObjectJson for {obj_id} "
            if status >= 500:
                msg += f"failed, status: {status}"
                log.error(msg)
            else:
                msg += f"returned status: {status}"
                log.warn(msg)
            return

        log.debug(f"DomainCrawler - got json for {obj_id}")
        log.debug(f"obj_json: {obj_json}")

        log.debug("store obj json")
        self._obj_dict[obj_id] = obj_json  # store the obj_json

        # for groups iterate through all the hard links and
        # add to the lookup ids set

        log.debug(f"gotCollection: {collection}")

        if collection == "groups" and follow_links:
            if "links" not in obj_json:
                log.error("expected links key in obj_json")
                return
            links = obj_json["links"]
            log.debug(f"DomainCrawler links: {links}")
            for title in links:
                log.debug(f"DomainCrawler - got link: {title}")
                link_obj = links[title]
                num_objects = len(self._obj_dict)
                if self._params.get("max_objects_limit") is not None:
                    max_objects_limit = self._params["max_objects_limit"]
                    if num_objects >= max_objects_limit:
                        msg = "DomainCrawler reached limit of "
                        msg += f"{max_objects_limit}"
                        log.info(msg)
                        break
                if link_obj["class"] != "H5L_TYPE_HARD":
                    # just follow hardlinks
                    continue
                link_id = link_obj["id"]
                if link_id not in self._obj_dict:
                    # haven't seen this object yet, get obj json
                    log.debug(f"DomainCrawler - adding link_id: {link_id}")
                    self._obj_dict[link_id] = {}  # placeholder for obj id
                    self._q.put_nowait(link_id)

    async def crawl(self):
        workers = [asyncio.Task(self.work()) for _ in range(self._max_tasks)]
        # When all work is done, exit.
        msg = "DomainCrawler - await queue.join - "
        msg += f"count: {len(self._obj_dict)}"
        log.info(msg)
        await self._q.join()
        msg = "DomainCrawler - join complete - "
        msg += f"count: {len(self._obj_dict)}"
        log.info(msg)

        for w in workers:
            w.cancel()
        log.debug("DomainCrawler - workers canceled")

    async def work(self):
        while True:
            obj_id = await self._q.get()
            await self.fetch(obj_id)
            self._q.task_done()

    async def fetch(self, obj_id):
        log.debug(f"DomainCrawler fetch for id: {obj_id}")
        log.debug(f"action: {self._action}")
        if self._action == "get_obj":
            log.debug("DomainCrawler - get obj")
            # just get the obj json
            await self.get_obj_json(obj_id)
        elif self._action == "get_attr":
            log.debug("DomainCrawler - get attributes")
            # fetch the given attributes
            if self._objs is None:
                log.error("DomainCrawler - self._objs not set")
                return
            if obj_id not in self._objs:
                log.error(f"couldn't find {obj_id} in self._objs")
                return
            attr_names = self._objs[obj_id]
            if not isinstance(attr_names, list):
                log.error("expected list for attribute names")
                return
            if len(attr_names) == 0:
                log.warn("expected at least one name in attr_names list")
                return

            log.debug(f"DomainCrawler - got attribute names: {attr_names}")
            await self.get_attributes(obj_id, attr_names)
        elif self._action == "put_attr":
            log.debug("DomainCrawler - put attributes")
            # write attributes
            if self._objs and obj_id not in self._objs:
                log.error(f"couldn't find {obj_id} in self._objs")
                return
            attr_items = self._objs[obj_id]
            log.debug(f"got {len(attr_items)} attr_items")

            await self.put_attributes(obj_id, attr_items)
        else:
            msg = f"DomainCrawler: unexpected action: {self._action}"
            log.error(msg)

        msg = f"DomainCrawler - fetch complete obj_id: {obj_id}, "
        msg += f"{len(self._obj_dict)} objects found"
        log.debug(msg)
        log.debug(f"obj_dict: {self._obj_dict}")
