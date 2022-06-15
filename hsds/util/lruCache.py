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
import numpy
import time

from .. import hsds_logger as log


def getArraySize(arr):
    """Return size in bytes of numpy array"""
    nbytes = arr.dtype.itemsize
    for n in arr.shape:
        nbytes *= n
    return nbytes


class Node(object):
    def __init__(self, id, data, mem_size=1024, isdirty=False, prev=None, next=None):
        self._id = id
        self._data = data
        self._mem_size = mem_size
        self._isdirty = isdirty
        self._prev = prev
        self._next = next
        self._last_access = time.time()


class LruCache(object):
    """LRU cache for Numpy arrays that are read/written from S3
    If name is "ChunkCache", chunk items are assumed by be ndarrays
    """

    def __init__(self, mem_target=32 * 1024 * 1024, name="LruCache", expire_time=None):
        self._hash = {}
        self._lru_head = None
        self._lru_tail = None
        self._mem_size = 0
        self._dirty_size = 0
        self._mem_target = mem_target
        self._expire_time = expire_time
        self._name = name
        self._dirty_set = set()

    def _delNode(self, key):
        # remove from LRU
        if key not in self._hash:
            raise KeyError(key)
        node = self._hash[key]
        prev = node._prev
        next_node = node._next
        if prev is None:
            if self._lru_head != node:
                raise KeyError("unexpected error")
            self._lru_head = next_node
        else:
            prev._next = next_node
        if next_node is None:
            if self._lru_tail != node:
                raise KeyError("unexpected error")
            self._lru_tail = prev
        else:
            next_node._prev = prev
        node._next = node._prev = None
        log.debug(f"LRU {self._name} node {node._id} removed {self._name}")
        return node

    def _moveToFront(self, key):
        # move this node to the front of LRU list
        if key not in self._hash:
            raise KeyError(key)
        node = self._hash[key]
        if self._lru_head == node:
            # already the front
            return node
        if node._prev is None:
            raise KeyError("unexpected error")
        prev = node._prev
        next_node = node._next
        node._prev = None
        node._next = self._lru_head
        prev._next = next_node
        self._lru_head._prev = node
        if next_node is not None:
            next_node._prev = prev
        else:
            if self._lru_tail != node:
                raise KeyError("unexpected error")
            self._lru_tail = prev
        self._lru_head = node
        return node

    def _hasKey(self, key, ignore_expire=False):
        """check if key is present node"""
        if key not in self._hash:
            return False
        if ignore_expire:
            return True
        node = self._hash[key]
        now = time.time()
        if self._expire_time:
            age = now - node._last_access
            if age > self._expire_time and not node._isdirty:
                msg = f"LRU {self._name} node {key} has been in cache for "
                msg += f"{now - node._last_access:.3f} seconds, expiring"
                log.debug(msg)
                return False
            else:
                return True
        else:
            return True

    def __delitem__(self, key):
        node = self._delNode(key)  # remove from LRU
        del self._hash[key]  # remove from hash
        # remove from LRU list

        self._mem_size -= node._mem_size
        if key in self._dirty_set:
            log.warning(f"LRU {self._name} removing dirty node: {key}")
            self._dirty_set.remove(key)
            self._dirty_size -= node._mem_size
            if self._dirty_size < 0:
                self._dirty_size = 0

    def __len__(self):
        """Number of nodes in the cache"""
        return len(self._hash)

    def __iter__(self):
        """Iterate over node ids"""
        node = self._lru_head
        while node is not None:
            yield node._id
            node = node._next

    def __contains__(self, key):
        """Test if key is in the cache"""
        return self._hasKey(key)

    def __getitem__(self, key):
        """Return numpy array from cache"""
        # doing a getitem has the side effect of moving this node
        # up in the LRU list
        if not self._hasKey(key):
            raise KeyError(key)
        node = self._moveToFront(key)
        return node._data

    def __setitem__(self, key, data):
        log.debug(f"setitem, key: {key}")
        if isinstance(data, numpy.ndarray):
            # can just compute size for numpy array
            mem_size = getArraySize(data)
        elif isinstance(data, dict):
            # TBD - come up with a way to get the actual data size
            # for dict objects
            mem_size = 1024
        elif isinstance(data, bytes):
            mem_size = len(data)
        else:
            raise TypeError("Unexpected type for LRUCache")

        if key in self._hash:
            # key is already in the LRU - update mem size, data and
            # move to front
            node = self._hash[key]
            old_size = self._hash[key]._mem_size
            mem_delta = node._mem_size - old_size
            self._mem_size += mem_delta
            node._data = data
            node._mem_size = mem_size
            self._moveToFront(key)
            if node._isdirty:
                self._dirty_size += mem_delta
            node._last_access = time.time()
            msg = f"LRU {self._name} updated node: {key}, "
            msg += f"was {old_size} bytes now {node._mem_size} bytes, "
            msg += f"dirty_size: {self._dirty_size}"
            log.debug(msg)
        else:
            node = Node(key, data, mem_size=mem_size)
            if self._lru_head is None:
                self._lru_head = self._lru_tail = node
            else:
                # newer items go to the front
                next_node = self._lru_head
                if next_node._prev is not None:
                    raise KeyError("unexpected error")
                node._next = next_node
                next_node._prev = node
                self._lru_head = node
            self._hash[key] = node
            self._mem_size += node._mem_size
            msg = f"LRU {self._name} adding {node._mem_size} to cache, "
            msg += f"mem_size is now: {self._mem_size}"
            log.debug(msg)
            if node._isdirty:
                self._dirty_size += node._mem_size
                msg = f"LRU {self._name} dirty_size is now: {self._dirty_size}"
                log.debug(msg)

            msg = f"LRU {self._name} added new node: {key} "
            msg += f"[{node._mem_size} bytes]"
            log.debug(msg)

        if self._mem_size > self._mem_target:
            # set dirty temporarily so we can't remove this node in reduceCache
            msg = f"LRU {self._name} mem_size greater than target "
            msg += f"{self._mem_target} reducing cache"
            log.debug(msg)
            isdirty = node._isdirty
            node._isdirty = True
            self._reduceCache()
            node._isdirty = isdirty

    def _reduceCache(self):
        # remove nodes from cache (if not dirty) until we are under
        # memory mem_target
        log.debug(f"LRU {self._name} reduceCache")

        node = self._lru_tail  # start from the back
        while node is not None:
            next_node = node._prev
            if not node._isdirty:
                log.debug(f"LRU {self._name} removing node: {node._id}")
                self.__delitem__(node._id)
                if self._mem_size <= self._mem_target:
                    msg = f"LRU {self._name} mem_size reduced below target"
                    log.debug(msg)
                    break
            else:
                pass  # can't remove dirty nodes
            node = next_node
        if self._mem_size > self._mem_target:
            msg = f"LRU {self._name} mem size of {self._mem_size} "
            msg += f"not reduced below target {self._mem_target}"
            log.debug(msg)
        # done reduceCache

    def clearCache(self):
        # remove all nodes from cache
        log.debug(f"LRU {self._name} clearCache")

        node = self._lru_tail  # start from the back
        while node is not None:
            next_node = node._prev
            if node._isdirty:
                msg = f"LRU {self._name} found dirty node during clear: "
                msg += f"{node._id}"
                log.error(msg)
                raise ValueError("Unable to clear cache")
            log.debug(f"LRU {self._name} removing node: {node._id}")
            self.__delitem__(node._id)
            node = next_node
        self._dirty_size = 0
        # done clearCache

    def consistencyCheck(self):
        """verify that the data structure is self-consistent"""
        id_list = []
        dirty_count = 0
        mem_usage = 0
        dirty_usage = 0
        # walk the LRU list
        node = self._lru_head
        node_type = None
        while node is not None:
            id_list.append(node._id)
            if node._id not in self._hash:
                raise ValueError(f"node: {node._id} not found in hash")
            if node._isdirty:
                dirty_count += 1
                if node._id not in self._dirty_set:
                    msg = f"expected to find id: {node._id} in dirty set"
                    raise ValueError(msg)
                dirty_usage += node._mem_size
            mem_usage += node._mem_size
            if node_type is None:
                node_type = type(node._data)
            else:
                if not isinstance(node._data, node_type):
                    raise TypeError("Unexpected datatype")
            node = node._next
        # finish forward iteration
        if len(id_list) != len(self._hash):
            msg = "unexpected number of elements in forward LRU list"
            raise ValueError()
        if dirty_count != len(self._dirty_set):
            raise ValueError("unexpected number of dirty nodes")
        if mem_usage != self._mem_size:
            raise ValueError("unexpected memory size")
        if dirty_usage != self._dirty_size:
            raise ValueError("unexpected dirty size")
        # go back through list
        node = self._lru_tail
        pos = len(id_list)
        reverse_count = 0
        while node is not None:
            reverse_count += 1
            if pos == 0:
                raise ValueError(f"unexpected node: {node._id}")
            if node._id != id_list[pos - 1]:
                msg = f"expected node: {id_list[pos-1]} but found: {node._id}"
                raise ValueError(msg)
            pos -= 1
            node = node._prev
        if reverse_count != len(id_list):
            msg = "elements in reverse list do not equal forward list"
            raise ValueError(msg)
        # done - consistencyCheck

    def setDirty(self, key):
        """setting dirty flag has the side effect of moving this node
        up in the LRU list"""
        log.debug(f"LRU {self._name} set dirty node id: {key}")

        node = self._moveToFront(key)
        if not node._isdirty:
            self._dirty_size += node._mem_size
            log.debug(f"LRU {self._name} - update dirty_size to: {self._dirty_size}")
        node._isdirty = True

        self._dirty_set.add(key)

    def clearDirty(self, key):
        """clear the dirty flag"""
        # clearing dirty flag has the side effect of moving this node
        # up in the LRU list
        # also, may trigger a memory cleanup

        log.debug(f"LRU {self._name} clear dirty node: {key}")
        node = self._moveToFront(key)
        if node._isdirty:
            self._dirty_size -= node._mem_size
        log.debug(f"LRU {self._name} dirty_size: {self._dirty_size}")
        node._isdirty = False

        if key in self._dirty_set:
            self._dirty_set.remove(key)
            if self._mem_size > self._mem_target:
                # maybe we can free up some memory now
                self._reduceCache()

    def isDirty(self, key):
        """return dirty flag"""
        # don't adjust LRU position
        return key in self._dirty_set

    def dump_lru(self):
        """Return LRU list as a string
        (for debugging)
        """
        node = self._lru_head
        s = "->"
        while node:
            s += node._id
            node = node._next
            if node:
                s += ","
        node = self._lru_tail
        s += "\n<-"
        while node:
            s += node._id
            node = node._prev
            if node:
                s += ","
        s += "\n"
        return s

    @property
    def cacheUtilizationPercent(self):
        return int((self._mem_size / self._mem_target) * 100.0)

    @property
    def dirtyCount(self):
        return len(self._dirty_set)

    @property
    def memUsed(self):
        return self._mem_size

    @property
    def memFree(self):
        memFree = self._mem_target - self._dirty_size
        if memFree < 0:
            memFree = 0
        return memFree

    @property
    def memTarget(self):
        return self._mem_target

    @property
    def memDirty(self):
        return self._dirty_size
