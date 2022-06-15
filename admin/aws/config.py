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
    "aws_region": "us-west-2",  # use us-west-2a to launch in one AZ
    "aws_s3_gateway": "https://s3.amazonaws.com",
    "hsds_ami": "ami-908430f0",  # 'ami-3443eb54',
    "bucket_name": "nasa.hsdsdev",
    "security_group_id": "sg-6e384417",
    "profile_name": "LimitedEC2",
    "subnet_id": "subnet-5b04173f",
    "key_name": "ACCESS",
    "instance_type": "m4.large",
    "project_tag": "ACCESS",
}


def get(x):
    # see if there is a command-line override
    option = "--" + x + "="
    for i in range(1, len(sys.argv)):
        # print i, sys.argv[i]
        if sys.argv[i].startswith(option):
            # found an override
            arg = sys.argv[i]
            option_len = len(option)
            return arg[option_len:]  # return text after option string
    # see if there are an environment variable override
    if x.upper() in os.environ:
        return os.environ[x.upper()]
    # no command line override, just return the cfg value
    return cfg[x]
