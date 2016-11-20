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
import json
 
sys.path.append('../../hsds/util')
sys.path.append('../../hsds')
from arrayUtil import bytesArrayToList 


class ArrayUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ArrayUtilTest, self).__init__(*args, **kwargs)
        # main
    

    def testByteArrayToList(self): 
        data_items = (
            42, 
            "foo",
            b"foo",
            [1,2,3],
            (1,2,3),
            ["A", "B", "C"],
            [b"A", b"B", b"C"],
            [["A", "B"], [b'a', b'b', b'c']]
             
        )
        for data in data_items:
            json_data = bytesArrayToList(data)
            # will throw TypeError if not able to convert
            json.dumps(json_data)

if __name__ == '__main__':
    #setup test files
    
    unittest.main()


