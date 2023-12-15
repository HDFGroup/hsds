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

import asyncio

from .util.idUtil import getCollectionForId, getDataNodeUrl
from .util.httpUtil import http_post

from .servicenode_lib import getObjectJson
from . import hsds_logger as log


class DomainCrawler:
    def __init__(
        self,
        app,
        objs,
        bucket=None,
        include_attrs=True,
        attr_names=[],
        include_data=False,
        follow_links=True,
        ignore_nan=False,
        max_tasks=40,
        max_objects_limit=0,
    ):
        log.info(f"DomainCrawler.__init__  root_id: {len(objs)} objs")
        self._app = app
        self._max_objects_limit = max_objects_limit
        self._include_attrs = include_attrs
        self._attr_names = attr_names
        self._include_data = include_data
        self._follow_links = follow_links
        self._ignore_nan = ignore_nan
        self._max_tasks = max_tasks
        self._q = asyncio.Queue()
        self._obj_dict = {}
        self.seen_ids = set()
        if not objs:
            log.error("no objs for crawler to crawl!")
            raise ValueError()
        for obj in objs:
            log.debug(f"adding {obj} to the queue")
            self._q.put_nowait(obj)
        self._bucket = bucket

    async def get_attributes(self, obj_id, attr_names):
        # get the given attributes for the obj_id
        log.debug(f"get_attributes for {obj_id}, {len(attr_names)} attributes")
        req = getDataNodeUrl(self._app, obj_id)
        collection = getCollectionForId(obj_id)
        req += f"/{collection}/{obj_id}/attributes"
        log.debug(f"POST Attributes: {req}")
        params = {}
        if self._include_data:
            params["IncludeData"] = 1
        if self._ignore_nan:
            params["ignore_nan"] = 1
        if self._bucket:
            params["bucket"] = self._bucket
        data = {"attributes": attr_names}
        log.debug(f"using params: {params}")
        dn_json = await http_post(self._app, req, data=data, params=params)
        log.debug(f"got attributes json from dn for obj_id: {dn_json}")
        if "attributes" not in dn_json:
            log.error(f"DomainCrawler - expected attributes in json, but got: {dn_json}")
            return
        attributes = dn_json["attributes"]

        if len(attributes) < len(attr_names):
            msg = f"POST attributes requested {len(attr_names)}, "
            msg += f"but only {len(attributes)} were returned"
            log.warn(msg)
        self._obj_dict[obj_id] = attributes

    async def get_obj_json(self, obj_id):
        # get the given obj_json for the obj_id
        kwargs = {
            "include_links": self._follow_links,
            "include_attrs": self._include_attrs,
            "bucket": self._bucket,
        }
        obj_json = await getObjectJson(self._app, obj_id, **kwargs)
        log.debug(f"DomainCrawler - got json for {obj_id}")

        # if including links, we need link count
        if self._follow_links and "link_count" in obj_json:
            del obj_json["link_count"]

        # similarly, don't need attributeCount if we have the attributes
        if self._include_attrs:
            del obj_json["attributeCount"]

        self._obj_dict[obj_id] = obj_json  # store the obj_json

        # if this is a group, iterate through all the hard links and
        # add to the lookup ids set
        if getCollectionForId(obj_id) == "groups" and self._follow_links:
            links = obj_json["links"]
            log.debug(f"DomainCrawler links: {links}")
            for title in links:
                log.debug(f"DomainCrawler - got link: {title}")
                link_obj = links[title]
                num_objects = len(self._obj_dict)
                if self._max_objects_limit > 0:
                    if num_objects >= self._max_objects_limit:
                        msg = "DomainCrawler reached limit of "
                        msg += f"{self._max_objects_limit}"
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
                    self._q.put_nowait({"id": link_id})

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

    async def fetch(self, obj):
        if "id" not in obj:
            log.error(f"DomainCrawler - expected to find id key, but got: {obj}")
            return
        obj_id = obj["id"]
        log.debug(f"DomainCrawler fetch for id: {obj_id}")
        if not self._attr_names and "attr_names" not in obj:
            # just get the obj json
            await self.get_obj_json(obj_id)
        else:
            # fetch the given attributes
            if "attr_names" in obj:
                attr_names = obj["attr_names"]
            else:
                attr_names = self._attr_names
            await self.get_attributes(obj_id, attr_names)

        msg = f"DomainCrawler - fetch complete obj_id: {obj_id}, "
        msg += f"{len(self._obj_dict)} objects found"
        log.debug(msg)
        log.debug(f"obj_dict: {self._obj_dict}")
