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
    "head_host": "192.168.99.100",
    "head_port": 5100,
    "user_name": "test_user1",
    "user_password": "test",
    "log_level": "INFO",  # ERROR, WARNING, INFO, DEBUG, or NOTSET,
    "max_tcp_connections": 16,
    "max_concurrent_tasks": 128,  # 16,
    "docker_machine_ip": "192.168.99.100",
    "hsds_endpoint": "http://192.168.99.100:5102",
    "head_endpoint": "http://192.168.99.100:5100",
    "group_target": 1000,
    "timeout": 30,  # in seconds
}


def get(x):
    # see if there are an environment variable override
    if x.upper() in os.environ:
        return os.environ[x.upper()]
    # no command line override, just return the cfg value
    return cfg[x]
