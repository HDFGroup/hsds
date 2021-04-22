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
import sys
import unittest
sys.path.append('../..')

from hsds.lambda_function import lambda_handler

class LambdaTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(LambdaTest, self).__init__(*args, **kwargs)
        # main


    def testInvoke(self):
        event = {"method": "GET", "route": "/about"}
        context = {}
        rsp = lambda_handler(event, context)
        print("rsp:", rsp)
        self.assertTrue("statusCode" in rsp)
        self.assertEqual(rsp['statusCode'], 200)
         


if __name__ == '__main__':
    #setup test files

    unittest.main()
