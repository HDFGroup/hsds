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
import os
import sys
import unittest
from unittest.mock import patch

sys.path.append("../..")
from hsds import config  # noqa: E402

# the shipped config, not tests/unit/config.yml, which lacks these settings
REPO_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
CONFIG_DIR = os.path.normpath(os.path.join(REPO_ROOT, "admin", "config"))


class ConfigTest(unittest.TestCase):
    def setUp(self):
        self.addCleanup(config.cfg.clear)  # reload from the real environment afterwards
        config.cfg.clear()

    def testListSettingFromEnvironment(self):
        env = {"CONFIG_DIR": CONFIG_DIR, "REFERENCE_BUCKETS": "s3://hdf5.sample, other ,",
               "TOP_LEVEL_DOMAINS": "/home"}
        with patch.dict(os.environ, env):
            self.assertEqual(config.get("reference_buckets"), ["s3://hdf5.sample", "other"])
            self.assertEqual(config.get("top_level_domains"), ["/home"])

    def testScalarSettingFromEnvironment(self):
        with patch.dict(os.environ, {"CONFIG_DIR": CONFIG_DIR, "MAX_TCP_CONNECTIONS": "7"}):
            self.assertEqual(config.get("max_tcp_connections"), 7)


if __name__ == "__main__":
    unittest.main()
