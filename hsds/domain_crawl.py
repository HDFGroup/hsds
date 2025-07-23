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

from h5json.objid import getCollectionForId
from h5json.array_util import arrayToBytes

from .util.nodeUtil import getDataNodeUrl
from .util.httpUtil import isOK, http_put
from .util.globparser import globmatch
from .servicenode_lib import getObjectJson, getAttributes, putAttributes, getLinks, putLinks
from . import hsds_logger as log


class DomainCrawler:
    def __init__(
        self,
        app,
        objs,
        action="get_obj",
        bucket=None,
        follow_links=False,
        include_links=False,
        include_attrs=False,
        include_data=False,
        max_data_size=0,
        ignore_nan=False,
        encoding=None,
        create_order=False,
        pattern=None,
        limit=None,
        replace=False,
        ignore_error=False,
        max_tasks=40,
        max_objects_limit=0
    ):
        log.info(f"DomainCrawler.__init__  action: {action} - {len(objs)} objs")
        self._app = app
        self._action = action
        self._max_objects_limit = max_objects_limit
        self._follow_links = follow_links
        self._include_links = include_links
        self._include_attrs = include_attrs
        self._include_data = include_data
        self._max_data_size = max_data_size
        self._ignore_nan = ignore_nan
        self._encoding = encoding
        self._create_order = create_order
        self._pattern = pattern
        self._limit = limit
        self._count = 0  # items collected
        self._replace = replace
        self._max_tasks = max_tasks
        self._q = asyncio.Queue()
        self._obj_dict = {}
        self.seen_ids = set()
        self._ignore_error = ignore_error
        if not objs:
            log.error("no objs for crawler to crawl!")
            raise ValueError()
        if not bucket:
            log.error("bucket not set for DomainCrawler")
            raise ValueError()
        self._bucket = bucket
        for obj_id in objs:
            log.debug(f"adding {obj_id} to the queue")
            self._q.put_nowait(obj_id)
        if isinstance(objs, dict):
            self._objs = objs
        else:
            self._objs = None

    def follow_links(self, grp_id, links):
        # add any linked obj ids to the lookup ids set
        log.debug(f"follow links for {grp_id}, links: {links}")
        if getCollectionForId(grp_id) != "groups":
            log.warn(f"expected group id but got: {grp_id}")
            return
        link_count = 0
        for link in links:
            log.debug(f"DomainCrawler - follow links for: {link}")
            if isinstance(link, str):
                # we were passed a dict of link titles to link_jsons
                title = link
                link_obj = links[title]
            else:
                # were passed a list of link jsons
                if "title" not in link:
                    log.warn(f"expected to find title key in link: {link}")
                    continue
                title = link["title"]
                link_obj = link
            log.debug(f"link {title}: {link_obj}")
            if link_obj["class"] != "H5L_TYPE_HARD":
                # just follow hardlinks
                log.debug("not hard link, continue")
                continue
            link_id = link_obj["id"]
            link_collection = getCollectionForId(link_id)
            if self._action in ("get_link", "put_link") and link_collection != "groups":
                # only groups can have links
                log.debug(f"link id: {link_id} is not for a group, continue")
                continue
            num_objects = len(self._obj_dict)
            if self._max_objects_limit and num_objects >= self._max_objects_limit:
                msg = f"DomainCrawler reached limit of {self._max_objects_limit}"
                log.info(msg)
                break
            if link_id not in self._obj_dict:
                # haven't seen this object yet, get obj json
                log.debug(f"DomainCrawler - adding link_id: {link_id} to queue")
                self._obj_dict[link_id] = {}  # placeholder for obj id
                self._q.put_nowait(link_id)
                link_count += 1
            else:
                log.debug(f"link: {link_id} already in object dict")
        log.debug(f"follow links done, added {link_count} ids to queue")

    async def get_attributes(self, obj_id, attr_names):
        # get the given attributes for the obj_id
        msg = f"get_attributes for {obj_id}"
        if attr_names:
            msg += f", {len(attr_names)} attributes"
        log.debug(msg)

        kwargs = {"bucket": self._bucket}
        if self._include_data:
            kwargs["include_data"] = True
        if self._ignore_nan:
            kwargs["ignore_nan"] = True
        if self._encoding:
            kwargs["encoding"] = self._encoding
        if attr_names:
            kwargs["attr_names"] = attr_names
        else:
            # only apply these parameters if we are attempting to fetch all links
            if self._limit:
                kwargs["limit"] = self._limit
            if self._create_order:
                kwargs["create_order"] = True
            if self._pattern:
                kwargs["pattern"] = self._pattern
            if self._max_data_size > 0:
                kwargs["max_data_size"] = self._max_data_size
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

        follow_links = self._follow_links
        if isOK(status):
            log.debug(f"got attributes: {attributes}")
            if self._limit:
                left = self._limit - self._count
                if len(attributes) > left:
                    # truncate the attribute list
                    msg = f"limit reached, returning {left} attributes out"
                    msg += f"of {len(attributes)} for {obj_id}"
                    log.warn(msg)
                    attributes = attributes[:left]
                    follow_links = False
            self._count += len(attributes)
            self._obj_dict[obj_id] = attributes
        else:
            log.warn(f"Domain crawler - got {status} status for obj_id {obj_id}")
            self._obj_dict[obj_id] = {"status": status}

        collection = getCollectionForId(obj_id)

        if collection == "groups" and follow_links:
            links = None
            status = 200
            try:
                links = await getLinks(self._app, obj_id, bucket=self._bucket)
            except HTTPNotFound:
                status = 404
            except HTTPServiceUnavailable:
                status = 503
            except HTTPInternalServerError:
                status = 500
            except Exception as e:
                log.error(f"unexpected exception {e}")
                status = 500

            if status >= 500:
                log.warn(f"getLinks for {obj_id} returned: {status}")
            elif links:
                self.follow_links(obj_id, links)
            else:
                log.debug(f"no links for {obj_id}")

    async def put_attributes(self, obj_id, attr_items):
        # write the given attributes for the obj_id
        log.debug(f"put_attributes for {obj_id}, {len(attr_items)} attributes")
        req = getDataNodeUrl(self._app, obj_id)
        collection = getCollectionForId(obj_id)
        req += f"/{collection}/{obj_id}/attributes"
        kwargs = {"bucket": self._bucket}
        if self._replace:
            kwargs["replace"] = True
        status = None
        try:
            status = await putAttributes(self._app, obj_id, attr_items, **kwargs)
        except HTTPConflict:
            log.warn("DomainCrawler - got HTTPConflict from putAttributers")
            status = 409
        except HTTPServiceUnavailable:
            status = 503
        except HTTPInternalServerError:
            status = 500
        except Exception as e:
            log.error(f"unexpected exception {e}")

        log.debug(f"DomainCrawler fetch for {obj_id} - returning status: {status}")
        self._obj_dict[obj_id] = {"status": status}

    async def get_obj_json(self, obj_id):
        """ get the given obj_json for the obj_id.
            for each group found, search the links if follow_links is set """
        log.debug(f"DomainCrawler get_obj_json: {obj_id}")
        collection = getCollectionForId(obj_id)
        kwargs = {"bucket": self._bucket, "include_attrs": self._include_attrs}

        if collection == "groups" and self._follow_links:
            follow_links = True
            kwargs["include_links"] = True  # get them so we can follow them
        else:
            follow_links = False
        if follow_links or self._include_attrs:
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

        if isOK(status):
            log.debug(f"got obj json for: {obj_id}")
        else:
            msg = f"DomainCrawler - getObjectJson for {obj_id} "
            if status >= 500:
                msg += f"failed, status: {status}"
                log.error(msg)
            else:
                msg += f"returned status: {status}"
                log.warn(msg)
            return

        self._obj_dict[obj_id] = {"status": status}

        log.debug(f"DomainCrawler - got json for {obj_id}")

        log.debug("store obj json")
        self._obj_dict[obj_id] = obj_json  # store the obj_json

        # for groups iterate through all the hard links and
        # add to the lookup ids set

        log.debug(f"gotCollection: {collection}, follow_links: {follow_links}")

        if collection == "groups" and follow_links:
            if "links" not in obj_json:
                log.error("expected links key in obj_json")
                return
            links = obj_json["links"]
            self.follow_links(obj_id, links)

            if not self._include_links:
                # don't keep the links
                del obj_json["links"]

    async def get_links(self, grp_id, titles=None):
        """ if titles is set, get all the links in grp_id that
        have a title in the list.  Otherwise, return all links for the object. """
        log.debug(f"get_links: {grp_id} follow_links: {self._follow_links}")
        pattern = None
        if titles:
            log.debug(f"titles; {titles}")
        collection = getCollectionForId(grp_id)
        if collection != "groups":
            log.warn(f"get_links, expected groups id but got: {grp_id}")
            return
        kwargs = {"bucket": self._bucket}
        if titles:
            kwargs["titles"] = titles
        else:
            # only apply these parameters if we are attempting to fetch all links
            if self._limit:
                kwargs["limit"] = self._limit
            if self._create_order:
                kwargs["create_order"] = True

            if self._pattern:
                if self._follow_links:
                    # apply the pattern after we get the links back,
                    # otherwise we won't get the groups links that we
                    # need to follow
                    log.debug("will apply pattern on return")
                    pattern = self._pattern
                else:
                    kwargs["pattern"] = self._pattern

        log.debug(f"follow_links: {self._follow_links}")
        log.debug(f"getLinks kwargs: {kwargs}")

        links = None
        status = 200
        try:
            links = await getLinks(self._app, grp_id, **kwargs)
        except HTTPNotFound:
            status = 404
        except HTTPServiceUnavailable:
            status = 503
        except HTTPInternalServerError:
            status = 500
        except Exception as e:
            log.error(f"unexpected exception {e}")
            status = 500
        log.debug(f"get_links status: {status}")

        if not isOK(status):
            msg = f"DomainCrawler - get_links for {grp_id} "
            if status >= 500:
                msg += f"failed, status: {status}"
                log.error(msg)
            else:
                msg += f"returned status: {status}"
                log.warn(msg)
            return

        log.debug(f"DomainCrawler - got links for {grp_id}")

        if pattern:
            log.debug(f"applying pattern: {pattern}")
            filtered_links = []
            for link in links:
                title = link["title"]
                if globmatch(title, pattern):
                    filtered_links.append(link)
            msg = f"getLinks with pattern: {pattern} returning "
            msg += f"{len(filtered_links)} links from {len(links)}"
            log.debug(msg)
            new_links = filtered_links
        else:
            new_links = links  # store the links

        follow_links = self._follow_links
        # check that we are not exceeding the limit
        if self._limit:
            left = self._limit - self._count
            if left < len(new_links):
                # will need to truncate this list
                msg = f"limit reached, adding {left} new links out"
                msg += f" of {len(new_links)} for {grp_id}"
                log.warn(msg)
                new_links = new_links[:left]
                follow_links = False  # no need to search more
        self._count += len(new_links)
        log.debug(f"adding {len(new_links)} to obj_dict for {grp_id}")
        self._obj_dict[grp_id] = new_links

        # if follow_links, add any group links to the lookup ids set
        if follow_links:
            self.follow_links(grp_id, links)

    async def put_links(self, grp_id, link_items):
        # write the given links for the obj_id
        log.debug(f"DomainCrawler put_links for {grp_id}, {len(link_items)} links")
        req = getDataNodeUrl(self._app, grp_id)
        req += f"/groups/{grp_id}/links"
        kwargs = {"bucket": self._bucket}
        status = None
        try:
            status = await putLinks(self._app, grp_id, link_items, **kwargs)
        except HTTPConflict:
            log.warn("DomainCrawler - got HTTPConflict from http_put")
            status = 409
        except HTTPServiceUnavailable:
            log.warn("DomainCrawler - got HTTPServiceUnavailable exception")
            status = 503
        except HTTPInternalServerError:
            log.warn("DomainCrawler - got 500 error from DN")
            status = 500
        except Exception as e:
            log.error(f"unexpected exception {e}")

        log.debug(f"DomainCrawler fetch for {grp_id} - returning status: {status}")
        self._obj_dict[grp_id] = {"status": status}

    async def put_data(self, chunk_id, arr):
        # write a one-chunk dataset value
        log.debug(f"DomainCrawler put_data for {chunk_id}, arr: {arr}")
        req = getDataNodeUrl(self._app, chunk_id)
        req += "/chunks/" + chunk_id
        log.debug(f"put_data req: {req}")
        params = {"bucket": self._bucket}

        data = arrayToBytes(arr)

        log.debug(f"DomainCrawler - put_data req: {req}, {len(data)} bytes")
        try:
            rsp = await http_put(self._app, req, data=data, params=params)
            log.debug(f"http_put return: {rsp}")
            status = 200
        except HTTPConflict:
            log.warn("DomainCrawler - got HTTPConflict from http_put")
            status = 409
        except HTTPServiceUnavailable:
            log.warn("DomainCrawler - got HTTPServiceUnavailable exception")
            status = 503
        except HTTPInternalServerError:
            log.warn("DomainCrawler - got 500 error from DN")
            status = 500
        except Exception as e:
            log.error(f"unexpected exception {e}")
            status = 500
        finally:
            log.debug("DomainCrawler put_data end try")

        log.debug(f"DomainCrawler put_data for {chunk_id} - returning status: {status}")
        self._obj_dict[chunk_id] = {"status": status}

    def get_status(self):
        """ return the highest status of any of the returned objects """
        status = None
        for obj_id in self._obj_dict:
            item = self._obj_dict[obj_id]
            if "status" in item:
                item_status = item["status"]
                if status is None or item_status > status:
                    # return the more severe error
                    log.debug(f"setting status to {item_status}")
                    status = item_status
        return status

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

        status = self.get_status()
        if status:
            log.debug(f"DomainCrawler -- status: {status}")
            log.debug(f"ignore_error: {self._ignore_error}")
            if not self._ignore_error:
                # throw the appropriate exception if other than 200, 201
                if isOK(status):
                    pass  # ok
                elif status == 400:
                    log.warn("DomainCrawler - BadRequest")
                    raise HTTPBadRequest(reason="unknown")
                elif status == 404:
                    log.warn("DomainCrawler - not found")
                    raise HTTPNotFound()
                elif status == 409:
                    log.warn("DomainCrawler - conflict")
                    raise HTTPConflict()
                elif status == 410:
                    log.warn("DomainCrawler - gone")
                    raise HTTPGone()
                elif status == 500:
                    log.error("DomainCrawler - internal server error")
                    raise HTTPInternalServerError()
                elif status == 503:
                    log.error("DomainCrawler - server busy")
                    raise HTTPServiceUnavailable()
                else:
                    log.error(f"DomainCrawler - unexpected status: {status}")
                    raise HTTPInternalServerError()

    async def work(self):
        while True:
            obj_id = await self._q.get()
            await self.fetch(obj_id)
            self._q.task_done()

    async def fetch(self, obj_id):
        log.debug(f"DomainCrawler fetch for id: {obj_id}")
        if self._action == "get_obj":
            log.debug("DomainCrawler - get obj")
            # just get the obj json
            await self.get_obj_json(obj_id)
        elif self._action == "get_attr":
            log.debug("DomainCrawler - get attributes")
            # fetch the given attributes
            if self._objs is None or obj_id not in self._objs:
                attr_names = None  # fetch all attributes for obj_id
            else:
                attr_names = self._objs[obj_id]
            if attr_names is None:
                log.debug(f"fetch all attributes for {obj_id}")
            else:
                if not isinstance(attr_names, list):
                    log.error("expected list for attribute names")
                    return
                if len(attr_names) == 0:
                    log.warn("expected at least one name in attr_names list")
                    return

                log.debug(f"DomainCrawler - get attribute names: {attr_names}")
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
        elif self._action == "get_link":
            log.debug("DomainCrawler - get links")
            log.debug(f"self._objs: {self._objs}, type: {type(self._objs)}")

            if self._objs is None or obj_id not in self._objs:
                link_titles = None  # fetch all links for this object
            else:
                link_titles = self._objs[obj_id]
            if link_titles is None:
                log.debug(f"fetch all links for {obj_id}")
            else:
                if not isinstance(link_titles, list):
                    log.error("expected list for link titles")
                    return
                if len(link_titles) == 0:
                    log.warn("expected at least one name in link titles list")
                    return

                log.debug(f"DomainCrawler - get link titles: {link_titles}")
            await self.get_links(obj_id, link_titles)
        elif self._action == "put_link":
            log.debug("DomainCrawler - put links")
            # write links
            if self._objs and obj_id not in self._objs:
                log.error(f"couldn't find {obj_id} in self._objs")
                return
            link_items = self._objs[obj_id]
            log.debug(f"got {len(link_items)} link items for {obj_id}")

            await self.put_links(obj_id, link_items)
        elif self._action == "put_data":
            log.debug("DomainCrawler - put data")
            # write one chunk per dataset
            if self._objs and obj_id not in self._objs:
                log.error(f"couldn't find {obj_id} in self._objs")
                return
            data = self._objs[obj_id]
            log.debug(f"got {data} data for {obj_id}")

            await self.put_data(obj_id, data)
        else:
            msg = f"DomainCrawler: unexpected action: {self._action}"
            log.error(msg)

        msg = f"DomainCrawler - fetch complete obj_id: {obj_id}, "
        msg += f"{len(self._obj_dict)} objects processed"
        log.debug(msg)
        log.debug(f"obj_dict: {len(self._obj_dict)} items")
