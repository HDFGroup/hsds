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
    'hsds_endpoint': 'http://cf.hdf.test',
    'head_endpoint': 'http://cf.hdf.test:5100',
    'user_name': 'test_user1',
    'user_password': 'test',
    'test_noauth': True,
    'default_public': False,  # set to true if the server makes new domains publically readable
    'hdf5_sample_bucket': 'hdf5.sample'  # S3 bucket for storing traditional HDF5 files
}
   
def get(x):     
    # see if there are an environment variable override
    if x.upper() in os.environ:
        return os.environ[x.upper()]
    # no command line override, just return the cfg value        
    return cfg[x]
