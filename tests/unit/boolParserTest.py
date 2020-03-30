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
import time
import sys
import os

sys.path.append('../..')
from hsds.util.boolparser import BooleanParser

class BooleanParserTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(BooleanParserTest, self).__init__(*args, **kwargs)
        # main


    def testExpressions(self):

        p = BooleanParser('x1 == "hi" AND y2 > 42')
        variables = p.getVariables()
        self.assertEqual(len(variables), 2)
        self.assertTrue("x1" in variables)
        self.assertTrue("y2" in variables)
        self.assertTrue(p.evaluate({'x1': 'hi', 'y2': 43}))


        # use single instead of double quotes
        p = BooleanParser("x1 == 'hi' AND y2 > 42")
        variables = p.getVariables()

        self.assertEqual(len(variables), 2)
        self.assertTrue("x1" in variables)
        self.assertTrue("y2" in variables)
        self.assertTrue(p.evaluate({'x1': 'hi', 'y2': 43}))

        p = BooleanParser("x == 'hi' OR x == 'bye'")
        variables = p.getVariables()
        self.assertEqual(len(variables), 1)
        self.assertTrue("x" in variables)
        self.assertTrue(p.evaluate({'x': "bye"}))
        self.assertFalse(p.evaluate({'x': "aloha"}))

        # do lexigraphical comparison
        p = BooleanParser('x1 >= "cat" AND x1 <= "pig"')
        variables = p.getVariables()
        self.assertEqual(len(variables), 1)
        self.assertTrue("x1" in variables)
        self.assertTrue(p.evaluate({'x1': 'cat'}))
        self.assertFalse(p.evaluate({'x1': 'aardvark'}))
        self.assertTrue(p.evaluate({'x1': 'dog'}))
        self.assertTrue(p.evaluate({'x1': 'pig'}))
        self.assertFalse(p.evaluate({'x1': 'piglet'}))


        p = BooleanParser("x > 2 AND y < 3")
        self.assertTrue(p.evaluate({'x':3, 'y': 1}))
        self.assertFalse(p.evaluate({'x':1, 'y': 1}))

        try:
            p.evaluate({'x':'3', 'y': 1})
            self.assertTrue(False)  # expected exception
        except TypeError:
            pass # expected - type of x is not int

        try:
            p.evaluate({'x': {'a': 1, 'b': 2}, 'y': 1})
            self.assertTrue(False)  # expected exception - dict pased for x value
        except TypeError:
            pass # expected - type of x is not int

        try:
            p.evaluate({'y': 1})
            self.assertTrue(False)  # expected exception
        except TypeError:
            pass # expected - missing 'x' in dict

        try:
            BooleanParser("x > 2 AND")
            self.assertTrue(False)  # expected exception
        except IndexError:
            pass # expected - malformed exception

        try:
            BooleanParser("1 + 1 = 2")
            self.assertTrue(false)  # expected exception
        except Exception:
            pass # expected - malformed exception


if __name__ == '__main__':
    #setup test files

    unittest.main()
