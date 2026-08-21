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
import sys

sys.path.append("../..")
from hsds.util.globparser import globmatch


class GlobParserTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(GlobParserTest, self).__init__(*args, **kwargs)
        # main

    def testPatterns(self):

        self.assertTrue(globmatch("ab123", "ab*"))
        self.assertFalse(globmatch("ab123", "abc*"))
        self.assertTrue(globmatch("abc123", "*123"))
        self.assertTrue(globmatch("abcxyz", "abc*xyz"))
        self.assertTrue(globmatch("abc123xyz", "abc*xyz"))
        self.assertTrue(globmatch("abc123xyz", "abc*yz"))
        self.assertTrue(globmatch("abc123xyz", "abc*z"))
        self.assertTrue(globmatch("a*c", "a[*]c"))
        self.assertFalse(globmatch("abc", "a[*]c"))

        self.assertFalse(globmatch("abc123", "*124"))
        self.assertTrue(globmatch("ab7", "ab[0-9]"))
        self.assertTrue(globmatch("abc123", "a?c*123"))

        # no wild chard chars
        self.assertTrue(globmatch("abc", "abc"))

        self.assertFalse(globmatch("abc", "abcd"))

        self.assertFalse(globmatch("", "abc"))
        self.assertFalse(globmatch("abc", ""))

        # test '?' wildcard
        self.assertTrue(globmatch("abc", "ab?"))
        self.assertTrue(globmatch("abc", "a??"))
        self.assertFalse(globmatch("abc", "a?"))
        self.assertFalse(globmatch("abc", "x??"))

        # test range
        self.assertTrue(globmatch("abc", "ab[c]"))
        self.assertFalse(globmatch("abc", "ab[x]"))
        self.assertTrue(globmatch("ab?", "ab[?]"))
        self.assertTrue(globmatch("[ab]", "[[]ab[]]"))
        self.assertFalse(globmatch("abc", "ab[?]"))
        self.assertFalse(globmatch("abc", "ab[0-9]"))
        self.assertTrue(globmatch("ab7", "ab[0-9]"))
        self.assertTrue(globmatch("abc123", "[a-c][a-c][a-c][1-3][1-3][1-3]"))
        self.assertFalse(globmatch("abx123", "[a-c][a-c][a-c][1-3][1-3][1-3]"))

        # test expected ValueError exceptions
        bad_patterns = (
            "[",
            "ab[",
            "ab[-c]",
            "ab]",
            "*ab*",
            "ab[12]",
            "ab[12-34]",
            "ab[c]]",
        )
        for pattern in bad_patterns:
            try:
                globmatch("abcdef", pattern)
                self.assertTrue(False)
            except ValueError:
                pass  # expected


if __name__ == "__main__":
    # setup test files

    unittest.main()
