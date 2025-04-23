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

import time
import asyncio
from aiohttp.web_exceptions import HTTPBadRequest, HTTPForbidden, HTTPNotFound
from aiohttp.web_exceptions import HTTPGone, HTTPInternalServerError
from aiohttp.web_exceptions import HTTPServiceUnavailable

from .servicenode_lib import getObjectJson, getDomainResponse, getDomainJson
from .util.nodeUtil import getNodeCount

from . import hsds_logger as log


class FolderCrawler:
    def __init__(
        self,
        app,
        domains,
        bucket=None,
        get_root=False,
        verbose=False,
        max_tasks_per_node=100,
    ):
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
        workers = [asyncio.Task(self.work()) for _ in range(self._max_tasks)]
        # When all work is done, exit.
        msg = f"FolderCrawler max_tasks {self._max_tasks} = await queue.join "
        msg += f"- count: {len(self._domain_dict)}"
        log.info(msg)
        await self._q.join()
        folder_count = len(self._domain_dict)
        msg = f"FolderCrawler - join complete - count: {folder_count}"
        log.info(msg)

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
            msg = f"FolderCrawler - task {domain} start: {start:.3f} "
            msg += f"elapsed: {elapsed:.3f}"
            log.debug(msg)

    async def fetch(self, domain):
        msg = f"FolderCrawler - fetch for domain: {domain} bucket: "
        msg += f"{self._bucket}"
        log.debug(msg)
        domain_key = self._bucket + domain
        try:
            kwargs = {"reload": True}
            domain_json = await getDomainJson(self._app, domain_key, **kwargs)
            msg = f"FolderCrawler - {domain} got domain_json: {domain_json}"
            log.debug(msg)
            if domain_json:
                kwargs = {"verbose": self._verbose, "bucket": self._bucket}
                domain_rsp = await getDomainResponse(self._app, domain_json, **kwargs)
                for k in ("limits", "version", "compressors"):
                    if k in domain_rsp:
                        # don't return given key for multi-domain responses
                        del domain_rsp[k]
                msg = f"FolderCrawler - {domain} get domain_rsp: {domain_rsp}"
                log.debug(msg)
                # mixin domain name
                self._domain_dict[domain] = domain_rsp
                if self._get_root and "root" in domain_json:
                    root_id = domain_json["root"]
                    log.debug(f"fetching root json for {root_id}")
                    root_json = await getObjectJson(
                        self._app,
                        root_id,
                        include_links=False,
                        include_attrs=True,
                        bucket=self._bucket,
                    )
                    log.debug(f"got root_json: {root_json}")
                    self._group_dict[root_id] = root_json
            else:
                log.warn(f"FolderCrawler - no domain found for {domain}")
        except HTTPNotFound:
            # One of the domains not found, but continue through the list
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
            msg = f"fetch result - service unavailable for domain: {domain}"
            log.warn(msg)
        except Exception as e:
            msg = f"fetch result - unexpected exception for domain {domain}: "
            msg += f"exception of type {type(e)}, {e}"
            log.error(msg)
