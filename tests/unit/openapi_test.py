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
import unittest
import os
import yaml

# resolve relative to this file (not cwd) so this test works whether it's
# run from tests/unit, from the repo root, or via testall.py
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.normpath(os.path.join(THIS_DIR, "..", ".."))
OPENAPI_PATH = os.path.join(REPO_ROOT, "openapi.yml")


class OpenApiTest(unittest.TestCase):
    def testFileExists(self):
        self.assertTrue(
            os.path.isfile(OPENAPI_PATH), f"expected to find {OPENAPI_PATH}"
        )

    def testValidYaml(self):
        with open(OPENAPI_PATH) as f:
            doc = yaml.safe_load(f)
        self.assertTrue(isinstance(doc, dict))
        for key in ("openapi", "info", "paths", "components"):
            self.assertTrue(key in doc, f"expected top-level key: {key}")
        self.assertTrue(doc["openapi"].startswith("3."))
        self.assertTrue(len(doc["paths"]) > 0)

    def testValidOpenApiSchema(self):
        try:
            from openapi_spec_validator import validate
            from openapi_spec_validator.readers import read_from_filename
        except ImportError:
            self.skipTest("openapi_spec_validator not installed")
            return

        spec_dict, _base_uri = read_from_filename(OPENAPI_PATH)
        validate(spec_dict)  # raises if the spec is invalid


if __name__ == "__main__":
    unittest.main()
