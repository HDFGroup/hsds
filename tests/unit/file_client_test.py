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
import os
import os.path as pp
import tempfile
import unittest
import sys
from unittest.mock import patch

from aiohttp.web_exceptions import HTTPBadRequest

sys.path.append("../..")
from hsds.util.fileClient import FileClient, TMP_SUFFIX  # noqa: E402
from hsds.util import fileClient as fileClientModule  # noqa: E402

ROOT_DIR = pp.normpath("/var/hsds_data")


def _client(root_dir=ROOT_DIR):
    """Build a FileClient without running __init__, which requires the
    root_dir config to point at a directory that actually exists. Only the
    path-validation helpers are under test here, and they need just
    _root_dir."""
    client = FileClient.__new__(FileClient)
    client._root_dir = pp.normpath(root_dir)
    return client


class FileClientPathTest(unittest.TestCase):
    """Keys and buckets arrive from the request, so the path they build has to
    stay under the storage root."""

    def testValidateKeyRejectsParentSegment(self):
        client = _client()
        # a leading slash was already rejected, but ".." further along was not
        for key in (
            "../etc/shadow",
            "../../../etc/shadow",
            "a/b/../../../../etc/shadow",
            "a/../../b",
            "a\\..\\..\\b",
        ):
            with self.assertRaises(HTTPBadRequest, msg=f"key: {key}"):
                client._validateKey(key)

    def testValidateKeyAllowsOrdinaryKeys(self):
        client = _client()
        for key in ("db/obj.json", "a/b/c.h5", "x..y/z", "..dotted", "a/b..c"):
            client._validateKey(key)  # must not raise

    def testValidateBucketRejectsDotSegments(self):
        client = _client()
        for bucket in (".", ".."):
            with self.assertRaises(HTTPBadRequest, msg=f"bucket: {bucket}"):
                client._validateBucket(bucket)

    def testValidateBucketAllowsOrdinaryNames(self):
        client = _client()
        for bucket in ("hsdstest", "my-bucket", "a.b.c"):
            client._validateBucket(bucket)  # must not raise

    def testGetFilePathStaysInRoot(self):
        client = _client()
        path = client._getFilePath("hsdstest", "db/obj.json")
        self.assertEqual(path, pp.join(ROOT_DIR, "hsdstest", "db", "obj.json"))

    def testGetFilePathRejectsEscape(self):
        client = _client()
        with self.assertRaises(HTTPBadRequest):
            client._getFilePath("hsdstest", "../../../etc/shadow")

    def testCheckPathInRoot(self):
        # the backstop has to hold on its own, for any call path that builds a
        # path without going through _getFilePath
        client = _client()
        allowed = (
            pp.join(ROOT_DIR, "hsdstest", "db", "o.json"),
            ROOT_DIR,
        )
        for path in allowed:
            self.assertEqual(client._checkPathInRoot(path), pp.normpath(path))

        rejected = (
            pp.join(ROOT_DIR, "hsdstest", "../../../etc/shadow"),
            "/etc/shadow",
            # a sibling directory sharing the root's prefix - a startswith()
            # check would let this through
            ROOT_DIR + "_evil/x",
        )
        for path in rejected:
            with self.assertRaises(HTTPBadRequest, msg=f"path: {path}"):
                client._checkPathInRoot(path)


class FileClientWriteTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = tmp.name
        os.mkdir(pp.join(self.root, "bucket"))
        self.client = _client(self.root)
        self.client._app = {}

    def files(self):
        return sorted(f for _, _, fs in os.walk(self.root) for f in fs)

    def testWriteLeavesNoTempFiles(self):
        asyncio.run(self.client.put_object("db/x/.group.json", b"{}", bucket="bucket"))
        asyncio.run(self.client.put_object("db/x/.group.json", b"{\"a\": 1}", bucket="bucket"))
        self.assertEqual(self.files(), [".group.json"])
        with open(pp.join(self.root, "bucket/db/x/.group.json"), "rb") as f:
            self.assertEqual(f.read(), b"{\"a\": 1}")

    def testFailedWriteKeepsPreviousObject(self):
        asyncio.run(self.client.put_object("db/x/.group.json", b"old", bucket="bucket"))

        class FailingFile:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def write(self, data):
                raise IOError("disk full")

        def failing_open(path, loop=None, mode=None):
            open(path, "wb").close()  # a partial file exists when the write fails
            return FailingFile()

        with patch.object(fileClientModule.aiofiles, "open", failing_open):
            with self.assertRaises(Exception):
                asyncio.run(self.client.put_object("db/x/.group.json", b"new", bucket="bucket"))
        with open(pp.join(self.root, "bucket/db/x/.group.json"), "rb") as f:
            self.assertEqual(f.read(), b"old")
        self.assertEqual(self.files(), [".group.json"])

    def testListingSkipsTempFiles(self):
        asyncio.run(self.client.put_object("db/x/.group.json", b"{}", bucket="bucket"))
        leftover = pp.join(self.root, "bucket/db/x/.group.json.0123" + TMP_SUFFIX)
        open(leftover, "wb").close()  # as a crash mid-write would leave
        keys = asyncio.run(self.client.list_keys(prefix="db/", bucket="bucket"))
        self.assertEqual(list(keys), ["db/x/.group.json"])


if __name__ == "__main__":
    unittest.main()
