##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of HSDS (HDF5 Scalable Data Service), Libraries and      #
# Utilities.  The full HSDS copyright notice, including          #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################
import os
import sys

cfg = {
    'aws_access_key_id': 'xxx',  
    'aws_secret_access_key': 'xxx',    
    'aws_region': 'us-east-1',
    'bucket_name': 'hdfgroup.hsdsdev',
    'head_host': 'localhost',
    'head_port': 5100,
    'dn_host': 'localhost',
    'dn_port' : 5101,  # run multiple dn nodes on odd ports
    'sn_host': 'localhost',
    'sn_port': 5102,   # run multipe sn nodes on even ports
    'target_sn_count': 4,
    'target_dn_count': 4,
    'log_file': 'head.log',
    'log_level': 'INFO',   # ERROR, WARNING, INFO, DEBUG, or NOTSET,
    'max_tcp_connections': 16,
    'head_sleep_time': 10,
    'node_sleep_time': 10
     
}
   
def get(x): 
    # see if there is a command-line override
    option = '--'+x+'='
    for i in range(1, len(sys.argv)):
        #print i, sys.argv[i]
        if sys.argv[i].startswith(option):
            # found an override
            arg = sys.argv[i]
            return arg[len(option):]  # return text after option string    
    # see if there are an environment variable override
    if x.upper() in os.environ:
        return os.environ[x.upper()]
    # no command line override, just return the cfg value        
    return cfg[x]

  
  
