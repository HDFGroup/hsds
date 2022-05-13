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

cfg = {
    'hsds_endpoint': 'http://localhost:5101',
    'user_name': 'test_user1',
    'user_password': 'test',
    'bucket_name': '',   # bucket name to be used for requests
    'stream_test_domain': '/home/test_user1/stream/bigfile.h5',
    # dataset size will be nrows x ncols x 8
    # e.g.: 12000 * 2000 * 8 = 192,000,000 = 183MiB
    'stream_test_nrows': 12000,
    'stream_test_ncols': 2200,
}


def get(x):
    # see if there are an environment variable override
    if x.upper() in os.environ:
        return os.environ[x.upper()]
    # no command line override, just return the cfg value
    return cfg[x]
