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
    'allow_noauth': True,  # enable unauthenticated requests
    'default_public': False, # new domains are publically readable by default
    'aws_access_key_id': 'xxx',  # Replace with access key for account
    'aws_secret_access_key': 'xxx',   # Replace with secret key for account
    'aws_iam_role': "hsds_role",  # For EC2 using IAM roles
    'aws_region': 'us-east-1',
    'hsds_endpoint': '', # used for hateos links in response
    'head_endpoint': '', # optionally used for nodes to register
    'aws_s3_gateway': 'https://s3.amazonaws.com',  
    'aws_dynamodb_gateway': 'https://dynamodb.us-east-1.amazonaws.com',
    'aws_dynamodb_users_table': '',
    'password_salt': '',
    'bucket_name': 'hdfgroup_hsdsdev',
    'sys_bucket_name': '',
    'head_host': 'localhost',
    'head_port': 5100,
    'an_port': 6100,
    'dn_host': 'localhost',
    'dn_port' : 6101,  # Start dn ports at 6101
    'sn_host': 'localhost',
    'sn_port': 5101,   # Start sn ports at 5101
    'target_sn_count': 4,
    'target_dn_count': 4,
    'log_file': 'head.log',
    'log_level': 'INFO',   # ERROR, WARNING, INFO, DEBUG, or NOTSET,
    'max_tcp_connections': 16,
    'head_sleep_time': 10,
    'node_sleep_time': 10,
    'async_sleep_time': 10,
    's3_sync_interval': 10,  # time to wait to write object data to S3 (in sec)     
    'max_chunks_per_request': 1000,  # maximum number of chunks to be serviced by one request
    'min_chunk_size': '1m',  # 1 MB
    'max_chunk_size': '4m',  # 4 MB
    'max_request_size': '100m',  # 100 MB
    'max_task_count': 100,  # maximum number of concurrent tasks before server will return 503 error
    'aio_max_pool_connections': 64,  # number of connections to keep in conection pool for aiobotocore requests
    'metadata_mem_cache_size': '128m',
    'chunk_mem_cache_size': '128m',  # 128 MB
    'timeout': 30,  # http timeout - 30 sec
    'anonymous_ttl': 10*60,  # time after which anonymous objects will be deleted - 10 m, 0 for infinite
    'gc_freq': 20*60,  # time between gc runs
    'password_file': '/usr/local/src/hsds/passwd.txt',  # filepath to a text file of username/passwords
    'server_name': 'Highly Scalable Data Service (HSDS)', # this gets returned in the about request
    'db_file': 'bucket.db',  # SQLite db file used by AN node
    'db_dir': '/data', # Directory path to store db file
    'greeting': 'Welcome to HSDS!',
    'about': 'HSDS is a webservice for HDF data',
    'top_level_domains': ["/home", "/shared"]  # list of possible top-level domains
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
