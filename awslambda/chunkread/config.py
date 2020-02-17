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
import sys

cfg = {
    'aws_access_key_id': 'xxx',  # Replace with access key for account
    'aws_secret_access_key': 'xxx',   # Replace with secret key for account
    'aws_iam_role': "hsds_role",  # For EC2 using IAM roles
    'aws_region': 'us-east-1',
    'aws_s3_gateway': '',   # use endpoint for the region HSDS is running in, e.g. 'https://s3.amazonaws.com' for us-east-1
    'bucket_name': '',  # set to usee a default bucket, otherwise bucket param is needed for all requests
    'log_level': 'INFO',   # ERROR, WARNING, INFO, DEBUG, or NOTSET,
    'min_chunk_size': '1m',  # 1 MB
    'max_chunk_size': '4m',  # 4 MB
    'max_request_size': '100m',  # 100 MB - should be no smaller than client_max_body_size in nginx tmpl
    'aio_max_pool_connections': 64  # number of connections to keep in conection pool for aiobotocore requests
}

def get(x):
    # see if there is a command-line override
    #print("config get:", x)
    option = '--'+x+'='
    retval = None
    for i in range(1, len(sys.argv)):
        #print(i, sys.argv[i])
        if sys.argv[i].startswith(option):
            # found an override
            arg = sys.argv[i]
            retval = arg[len(option):]  # return text after option string
    # see if there are an environment variable override
    if not retval and  x.upper() in os.environ:
        retval = os.environ[x.upper()]
    # no command line override, just return the cfg value
    if not retval and x in cfg:
        retval = cfg[x]
    if isinstance(retval, str) and len(retval) > 1 and retval[-1] in ('g', 'm', 'k') and retval[:-1].isdigit():
        # convert values like 512m to corresponding integer
        u = retval[-1]
        n = int(retval[:-1])
        if u == 'k':
            retval =  n * 1024
        elif u == 'm':
            retval = n * 1024*1024
        else: # u == 'g'
            retval = n * 1024*1024*1024
    return retval
