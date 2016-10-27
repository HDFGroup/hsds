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
 
sys.path.append('../../hsds/util')
sys.path.append('../../hsds')
from dsetUtil import guess_chunk

class DsetUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DsetUtilTest, self).__init__(*args, **kwargs)
        # main
    

    def testGuessChunk(self):       
        shape = [100, 100]
        typesize = 8
        layout = guess_chunk(shape, None, typesize)
        self.assertEqual(layout, (25, 50))

        shape = [5]
        layout = guess_chunk(shape, None, typesize)
        self.assertEqual(layout, (5,))

        shape = [100, 100, 100]
        layout = guess_chunk(shape, None, typesize)
        self.assertEqual(layout, (13,13,25))

        shape = [100, 0]
        layout = guess_chunk(shape, None, typesize)
        print(layout)

                                  
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
