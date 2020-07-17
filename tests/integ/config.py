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
    'hsds_endpoint': 'http://localhost',
    'head_endpoint': 'http://localhost:5100',
    'user_name': 'test_user1',
    'user_password': 'test',
    'user2_name': 'test_user2',
    'user2_password': 'test',
    'test_noauth': True,
    'default_public': False,  # set to true if the server makes new domains publicly readable
    'bucket_name': '',   # bucket name to be used for requests
    'hdf5_sample_bucket': '',  # S3 bucket for storing traditional HDF5 files (snp500.h5 and tall.h5), e.g. 'hdf5_sample'
    'test_group': 'test_group'  # group defined in admin/config/groups.txt that includes user user_name and user2_name
}

def get(x):
    # see if there are an environment variable override
    if x.upper() in os.environ:
        return os.environ[x.upper()]
    # no command line override, just return the cfg value
    return cfg[x]
