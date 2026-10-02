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


if __name__ == "__main__":
    # setup test files

    unittest.main()
