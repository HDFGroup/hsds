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
import stat
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.append("../..")
from hsds.util.httpUtil import bindToSocket  # noqa: E402
from hsds.hsds_app import _secureSocketDir  # noqa: E402


def _mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


@unittest.skipUnless(hasattr(os, "getuid"), "unix sockets and uids only")
class SocketTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.dir = self.tmpdir.name
        self.addCleanup(self.tmpdir.cleanup)

    def _url(self, name):
        return "http+unix://" + os.path.join(self.dir, name).replace("/", "%2F")

    def testPrivateSocketIsOwnerOnly(self):
        old_umask = os.umask(0o002)
        try:
            s = bindToSocket(self._url("dn_1.sock"), private=True)
            s.close()
            self.assertEqual(_mode(os.path.join(self.dir, "dn_1.sock")), 0o600)
            # the process umask is restored
            self.assertEqual(os.umask(0o002), 0o002)

            s = bindToSocket(self._url("sn_1.sock"))
            s.close()
            self.assertEqual(_mode(os.path.join(self.dir, "sn_1.sock")) & 0o060, 0o060)
        finally:
            os.umask(old_umask)

    def testSocketDirLosesGroupWrite(self):
        os.chmod(self.dir, 0o775)
        _secureSocketDir(self.dir)
        self.assertEqual(_mode(self.dir), 0o755)

    def testSocketDirOwnedByOtherUserRefused(self):
        with patch("hsds.hsds_app.os.getuid", return_value=os.getuid() + 1):
            with self.assertRaises(PermissionError):
                _secureSocketDir(self.dir)


if __name__ == "__main__":
    unittest.main()
