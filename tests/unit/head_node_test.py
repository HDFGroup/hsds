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
import asyncio
import logging
import sys
import unittest
from unittest.mock import patch

sys.path.append("../..")
from hsds import config
from hsds import headnode


def configWithTargets(sn_count, dn_count):
    """ return a config.get replacement that reports the given target counts """
    real_get = config.get

    def get(key, **kwargs):
        if key == "target_sn_count":
            return sn_count
        if key == "target_dn_count":
            return dn_count
        return real_get(key, **kwargs)

    return get


class HeadNodeTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(HeadNodeTest, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.WARNING)

    def testTargetCountsRequired(self):
        # With a target of 0 there are no slots to admit nodes into, and the
        # cluster would wait forever, so the head node refuses to start.
        for sn_count, dn_count in ((0, 0), (1, 0), (0, 4)):
            with patch.object(headnode.config, "get", configWithTargets(sn_count, dn_count)):
                with self.assertRaises(ValueError):
                    asyncio.run(headnode.init())

    def testTargetCountsSizeSlots(self):
        with patch.object(headnode.config, "get", configWithTargets(1, 4)):
            app = asyncio.run(headnode.init())
        self.assertEqual(app["active_sn_ids"], [None])
        self.assertEqual(app["active_dn_ids"], [None, None, None, None])

    def testRemoveNodeFreesSlot(self):
        # a node registering on an existing node's address under a new id (a
        # restarted container) replaces it; its slot must be freed, not left dangling
        with patch.object(headnode.config, "get", configWithTargets(1, 2)):
            app = asyncio.run(headnode.init())
        nodes = app["nodes"]
        for node_id, node_type, host in (("sn-1", "sn", "10.0.0.1"),
                                         ("dn-1", "dn", "10.0.0.2"),
                                         ("dn-2", "dn", "10.0.0.3")):
            nodes[node_id] = headnode.Node(node_id=node_id, node_type=node_type,
                                           node_host=host, node_port=6101)
        app["active_sn_ids"][0] = "sn-1"
        app["active_dn_ids"][:] = ["dn-1", "dn-2"]

        headnode.removeNode(app, host="10.0.0.2", port=6101)

        self.assertNotIn("dn-1", nodes)
        self.assertEqual(app["active_dn_ids"], [None, "dn-2"])
        dn_urls = headnode.getNodeUrls(nodes, app["active_dn_ids"])
        self.assertEqual(dn_urls, [None, "http://10.0.0.3:6101"])
        self.assertEqual(headnode.getActiveNodeCount(app, "dn"), 1)


if __name__ == "__main__":
    # setup test files

    unittest.main()
