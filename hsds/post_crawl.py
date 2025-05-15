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
# post crawler
#

import asyncio

from aiohttp.web_exceptions import HTTPServiceUnavailable, HTTPConflict, HTTPBadRequest
from aiohttp.web_exceptions import HTTPInternalServerError, HTTPNotFound, HTTPGone

from .util.httpUtil import isOK
from .servicenode_lib import createObject, createObjectByPath
from . import hsds_logger as log


class PostCrawler:
    def __init__(
        self,
        app,
        items=None,
        root_id=None,
        bucket=None,
        max_tasks=40,
        ignore_error=False
    ):
        log.info("PostCrawler.__init__")
        self._app = app
        self._root_id = root_id
        self._bucket = bucket
        self._max_tasks = max_tasks
        self._ignore_error = ignore_error

        if not items:
            log.error("no post requests for crawler to crawl!")
            raise ValueError()
        if not bucket:
            log.error("bucket not set for PostCrawler")
            raise ValueError()
        self._count = len(items)
        self._items = items
        self._rsp_objs = [None,] * self._count
        self._q = asyncio.Queue()
        log.debug(f"PostCrawler adding index 0 - {self._count} to queue")
        for i in range(self._count):
            self._q.put_nowait(i)

    def get_rsp_objs(self):
        """ return list of object responses """

        return self._rsp_objs

    def get_status(self):
        """ return the highest status of any of the returned objects """
        status = None
        for i in range(self._count):
            item = self._rsp_objs[i]
            if not item:
                continue  # resp not filled in yet
            if "status" in item:
                item_status = item["status"]
                if status is None or item_status > status:
                    # return the more severe error
                    log.debug(f"setting status to {item_status}")
                    status = item_status
            elif "id" in item:
                # post request succeeded
                if status is None:
                    status = 201
            else:
                log.error(f"PostCrawler unexpected response for item {i}: {item}")
                status = 500

        return status

    async def crawl(self):
        max_tasks = min(self._max_tasks, self._count)
        workers = [asyncio.Task(self.work()) for _ in range(max_tasks)]
        # When all work is done, exit.
        msg = "PostCrawler - await queue.join - "
        msg += f"count: {self._count} with {max_tasks} workers"
        log.info(msg)
        await self._q.join()
        msg = "PostCrawler - join complete - "
        msg += f"count: {self._count}"
        log.info(msg)

        for w in workers:
            w.cancel()
        log.debug("PostCrawler - workers canceled")

        status = self.get_status()
        if status:
            log.debug(f"PostCrawler -- status: {status}")
            log.debug(f"ignore_error: {self._ignore_error}")
            if not self._ignore_error:
                # throw the appropriate exception if other than 200, 201
                if isOK(status):
                    pass  # ok
                elif status == 400:
                    log.warn("PostCrawler - BadRequest")
                    raise HTTPBadRequest(reason="unknown")
                elif status == 404:
                    log.warn("PostCrawler - not found")
                    raise HTTPNotFound()
                elif status == 409:
                    log.warn("PostCrawler - conflict")
                    raise HTTPConflict()
                elif status == 410:
                    log.warn("PostCrawler - gone")
                    raise HTTPGone()
                elif status == 500:
                    log.error("PostCrawler - internal server error")
                    raise HTTPInternalServerError()
                elif status == 503:
                    log.error("PostCrawler - server busy")
                    raise HTTPServiceUnavailable()
                else:
                    log.error(f"PostCrawler - unexpected status: {status}")
                    raise HTTPInternalServerError()
        else:
            # no tasks returned anything
            log.error("PostCrawler - no results returned")
            if not self._ignore_error:
                raise HTTPInternalServerError()

    async def work(self):
        while True:
            index = await self._q.get()
            await self.create(index)
            self._q.task_done()

    async def create(self, index):
        log.debug(f"PostCrawler fetch for index: {index}")
        item = self._items[index]
        log.debug(f"got item: {item}")
        kwargs = {"bucket": self._bucket}

        if "obj_id" in item:
            kwargs["obj_id"] = item["obj_id"]
        if "type" in item:
            kwargs["obj_type"] = item["type"]
        if "layout" in item:
            kwargs["layout"] = item["layout"]
        if "creation_props" in item:
            kwargs["creation_props"] = item["creation_props"]
        if "attrs" in item:
            kwargs["attrs"] = item["attrs"]
        if "parent_id" in item:
            kwargs["parent_id"] = item["parent_id"]
        elif "root_id" in item:
            kwargs["root_id"] = item["root_id"]
        if "h5path" in item:
            kwargs["h5path"] = item["h5path"]
        if "links" in item:
            kwargs["links"] = item["links"]

        log.debug(f"PostCrawler index {index} kwargs: {kwargs}")
        rsp_json = None
        try:
            if kwargs.get("parent_id") and kwargs.get("h5path"):
                rsp_json = await createObjectByPath(self._app, **kwargs)
            else:
                # create an anonymous group
                rsp_json = await createObject(self._app, **kwargs)
        except HTTPConflict:
            log.warn("PostCrawler - got HTTPConflict from http_post")
            rsp_json = {"status_code": 409}
        except HTTPServiceUnavailable:
            rsp_json = {"status_code": 503}
        except HTTPInternalServerError:
            rsp_json = {"status_code": 500}
        except Exception as e:
            log.error(f"unexpected exception {e}")
            rsp_json = {"status_code": 500}

        log.info(f"PostCrawler - index: {index} post rsp: {rsp_json}")

        self._rsp_objs[index] = rsp_json


async def createObjects(app, items, root_id=None, bucket=None):
    """ create an objects based on parameters in items list """

    if not root_id:
        msg = "no root_id given for createObjects"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    log.info(f"createObjects with {len(items)} items, root_id: {root_id}")

    post_crawler = PostCrawler(app, root_id=root_id, bucket=bucket, items=items)
    await post_crawler.crawl()
    if post_crawler.get_status() > 201:
        msg = f"createObjects returning status from crawler: {post_crawler.get_status()}"
        log.error(msg)
        raise HTTPInternalServerError()

    obj_list = post_crawler.get_rsp_objs()
    if not isinstance(obj_list, list):
        msg = f"createObjects expected list but got: {type(obj_list)}"
        log.error(msg)
        raise HTTPInternalServerError()
    return {"objects": obj_list}
