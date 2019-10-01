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
import hsds_logger as log

def getArraySize(arr):
    """ Return size in bytes of numpy array """
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



class LruCache(object):
    """ LRU cache for Numpy arrays that are read/written from S3
    """
    def __init__(self, mem_target=32*1024*1024, chunk_cache=True):
        self._hash = {}
        self._lru_head = None
        self._lru_tail = None
        self._mem_size = 0
        self._dirty_size = 0
        self._mem_target = mem_target
        self._chunk_cache = chunk_cache
        if chunk_cache:
            self._name = "ChunkCache"
        else:
            self._name = "MetaCache"
        self._dirty_set = set()

    def _getNode(self, key):
        """ Return node  """
        if key not in self._hash:
            raise KeyError(key)
        return self._hash[key]

    def _delNode(self, key):
        # remove from LRU
        node = self._getNode(key)
        prev = node._prev
        next = node._next
        if prev is None:
            if self._lru_head != node:
                raise KeyError("unexpected error")
            self._lru_head = next
        else:
            prev._next = next
        if next is None:
            if self._lru_tail != node:
                raise KeyError("unexpected error")
            self._lru_tail = prev
        else:
            next._prev = prev
        node._next = node._prev = None
        log.debug("LRU {} node {} removed from {}".format(self._name, node._id, self._name))
        return node

    def _moveToFront(self, key):
        # move this node to the front of LRU list
        node = self._getNode(key)
        if self._lru_head == node:
            # already the front
            return node
        if node._prev is None:
            raise KeyError("unexpected error")
        prev = node._prev
        next = node._next
        node._prev = None
        node._next = self._lru_head
        prev._next = next
        self._lru_head._prev = node
        if next is not None:
            next._prev = prev
        else:
            if self._lru_tail != node:
                raise KeyError("unexpected error")
            self._lru_tail = prev
        self._lru_head = node
        log.debug("LRU {} new headnode: {}".format(self._name, node._id))
        return node

    def __delitem__(self, key):
        node = self._delNode(key) # remove from LRU
        del self._hash[key]       # remove from hash
        # remove from LRU list

        self._mem_size -= node._mem_size
        if key in self._dirty_set:
            log.warning("LRU {} removing dirty node: {}".format(self._name, key))
            self._dirty_set.remove(key)
            self._dirty_size -= node._mem_size

    def __len__(self):
        """ Number of nodes in the cache """
        return len(self._hash)

    def __iter__(self):
        """ Iterate over node ids """
        node = self._lru_head
        while node is not None:
            yield node._id
            node = node._next

    def __contains__(self, key):
        """ Test if key is in the cache """
        if key in self._hash:
            return True
        else:
            return False

    def __getitem__(self, key):
        """ Return numpy array from cache """
        # doing a getitem has the side effect of moving this node
        # up in the LRU list
        node = self._moveToFront(key)
        return node._data

    def __setitem__(self, key, data):
        if self._chunk_cache:
            if not isinstance(data, numpy.ndarray):
                raise TypeError("Expected ndarray but got type: {}".format(type(data)))
            if len(key) < 38:
                # id should be prefix (e.g. "c-") and uuid value + chunk_index
                raise ValueError("Unexpected id length")
            if not key.startswith("c"):
                raise ValueError("Unexpected prefix")
            mem_size = getArraySize(data)  # can just compute size for numpy array
        else:
            if not isinstance(data, dict):
                raise TypeError("Expected dict but got type: {}".format(type(data)))
            # TBD - come up with a way to get the actual data size for dict objects
            mem_size = 1024

        if key in self._hash:
            # key is already in the LRU - update mem size, data and move to front
            node = self._hash[key]
            old_size = self._hash[key]._mem_size
            mem_delta = node._mem_size - old_size
            self._mem_size += mem_delta
            node._data = data
            node._mem_size = mem_size
            self._moveToFront(key)
            if node._isdirty:
                self._dirty_size += mem_delta
            log.debug("LRU {} updated node: {} [was {} bytes now {} bytes]".format(self._name, key, old_size, node._mem_size))
        else:
            node = Node(key, data, mem_size=mem_size)
            if self._lru_head is None:
                self._lru_head = self._lru_tail = node
            else:
                # newer items go to the front
                next = self._lru_head
                if next._prev is not None:
                    raise KeyError("unexpected error")
                node._next = next
                next._prev = node
                self._lru_head = node
            self._hash[key] = node
            self._mem_size += node._mem_size
            if node._isdirty:
                self._dirty_size += node._mem_size
            log.debug("LRU {} added new node: {} [{} bytes]".format(self._name, key, node._mem_size))

        if self._mem_size > self._mem_target:
            # set dirty temporarily so we can't remove this node in reduceCache
            isdirty = node._isdirty
            node._isdirty = True
            self._reduceCache()
            node._isdirty = isdirty

    def _reduceCache(self):
        # remove nodes from cache (if not dirty) until we are under memory mem_target
        log.debug("LRU {} reduceCache".format(self._name))

        node = self._lru_tail  # start from the back
        while node is not None:
            # log.debug("LRU check node: {}".format(self._name, node._id))
            next = node._prev
            if not node._isdirty:
                log.debug("LRU {} removing node: {}".format(self._name, node._id))
                self.__delitem__(node._id)
                if self._mem_size <= self._mem_target:
                    log.debug("LRU {} mem_sized reduced below target".format(self._name))
                    break
            else:
                # log.debug("LRU {} node: {} is dirty".format(self._name, node._id))
                pass # can't remove dirty nodes
            node = next
        if self._mem_size > self._mem_target:
            log.debug("LRU {} mem size of {} not reduced below target {}".format(self._name, self._mem_size, self._mem_target))
        # done reduceCache

    def consistencyCheck(self):
        """ verify that the data structure is self-consistent """
        id_list = []
        dirty_count = 0
        mem_usage = 0
        dirty_usage = 0
        # walk the LRU list
        node = self._lru_head
        while node is not None:
            id_list.append(node._id)
            if node._id not in self._hash:
                raise ValueError("node: {} not found in hash".format(node._id))
            if node._isdirty:
                dirty_count += 1
                if node._id not in self._dirty_set:
                    raise ValueError("expected to find id: {} in dirty set".format(node._id))
                dirty_usage += node._mem_size
            mem_usage += node._mem_size
            if self._chunk_cache and not isinstance(node._data, numpy.ndarray):
                raise TypeError("Unexpected datatype")
            node = node._next
        # finish forward iteration
        if len(id_list) != len(self._hash):
            raise ValueError("unexpected number of elements in forward LRU list")
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
                raise ValueError("unexpected node: {}".format(node._id))
            if node._id != id_list[pos - 1]:
                raise ValueError("expected node: {} but found: {}".format(id_list[pos-1], node._id))
            pos -= 1
            node = node._prev
        if reverse_count != len(id_list):
            raise ValueError("elements in reverse list do not equal forward list")
        # done - consistencyCheck


    def setDirty(self, key):
        # setting dirty flag has the side effect of moving this node
        # up in the LRU list
        log.debug("LRU {} set dirty node id: {}".format(self._name, key))

        node = self._moveToFront(key)
        if not node._isdirty:
            self._dirty_size += node._mem_size
        node._isdirty = True

        self._dirty_set.add(key)


    def clearDirty(self, key):
        # clearing dirty flag has the side effect of moving this node
        # up in the LRU list
        # also, may trigger a memory cleanup
        log.debug("LRU {} clear dirty node: {}".format(self._name, key))
        node = self._moveToFront(key)
        if node._isdirty:
            self._dirty_size -= node._mem_size
        node._isdirty = False

        if key in self._dirty_set:
            self._dirty_set.remove(key)
            if self._mem_size > self._mem_target:
                # maybe we can free up some memory now
                self._reduceCache()

    def isDirty(self, key):
        # don't adjust LRU position
        return key in self._dirty_set

    def dump_lru(self):
        """ Return LRU list as a string
            (for debugging)
        """
        node = self._lru_head
        s = "->"
        while node:
            s += node._id
            node = node._next
            if node:
                s +=  ","
        node = self._lru_tail
        s += "\n<-"
        while node:
            s += node._id
            node = node._prev
            if node:
                s +=  ","
        s += "\n"
        return s

    @property
    def cacheUtilizationPercent(self):
        return int((self._mem_size/self._mem_target)*100.0)

    @property
    def dirtyCount(self):
        return len(self._dirty_set)

    @property
    def memUsed(self):
        return self._mem_size

    @property
    def memTarget(self):
        return self._mem_target

    @property
    def memDirty(self):
        return self._dirty_size
