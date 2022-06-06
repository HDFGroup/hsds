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
    "host": "127.0.0.1",
    "port": 0,
    "num_bytes": 1048576,
    "batch_size": 10240,
    "use_shared_mem": 0,
    "socket_type": "AF_INET",  # AF_UNIX or AF_INET
}


def getCmdLineArg(x):
    # return value of command-line option
    # use "--x=val" to set option 'x' to 'val'
    # use "--x" for boolean flags
    option = "--" + x + "="
    for i in range(1, len(sys.argv)):
        arg = sys.argv[i]
        if arg == "--" + x:
            # boolean flag
            return True
        elif arg.startswith(option):
            # found an override
            override = arg[len(option) :]  # return text after option string
            return override
    return None


def get(x):
    # see if there is a cmd line override
    retval = getCmdLineArg(x)
    if retval:
        return retval
    # see if there are an environment variable override
    if x.upper() in os.environ:
        return os.environ[x.upper()]
    # no command line override, just return the cfg value
    return cfg[x]
