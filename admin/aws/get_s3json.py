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
import sys
import json
import time
import hashlib
import boto.s3
import boto.s3.connection
from boto.s3.key import Key
import config

# This is a utility to dump a JSON obj (group, dataset, ctype) given the
# the objects UUID

# NOTE - this script does not seem to work with minio -
# see: https://github.com/mozilla/socorro-collector/issues/17
#

# Print usage and exit
#


def printUsage():
    print(
        "usage: python get_s3json [--bucket_name=<bucket>] [--aws_s3_gateway=<s3_endpoint>] objid "
    )
    print("  objid: s3 JSON obj to fetch")
    print(
        "  Example: python get_s3json --aws_s3_gateway=http://192.168.99.100:9000 \
            --bucket_name=minio.hsdsdev t-cf2fc310-996f-11e6-8ef6-0242ac110005"
    )
    sys.exit()


#
# Get hash prefix
#
def getIdHash(id):
    """Return md5 prefix based on id value"""
    m = hashlib.new("md5")
    m.update(id.encode("utf8"))
    hexdigest = m.hexdigest()
    return hexdigest[:5]


def main():
    if len(sys.argv) == 1 or sys.argv[1] == "-h" or sys.argv[1] == "--help":
        printUsage()
        sys.exit(1)

    obj_id = sys.argv[-1]
    s3_gateway = config.get("aws_s3_gateway")
    print("aws_s3_gateway: {}".format(s3_gateway))
    region = config.get("aws_region")
    print("region: {}".format(region))
    print("now: {}".format(time.time()))
    conn = boto.s3.connect_to_region(
        region, calling_format=boto.s3.connection.OrdinaryCallingFormat()
    )

    bucket_name = config.get("bucket_name")
    print("bucket_name: {}".format(bucket_name))
    bucket = conn.get_bucket(bucket_name)

    if obj_id.startswith("d-") or obj_id.startswith("g-") or obj_id.startswith("t-"):
        # add the checksum prefix
        obj_id = getIdHash(obj_id) + "-" + obj_id

    k = Key(bucket)
    k.key = obj_id
    data = k.get_contents_as_string()
    if not isinstance(data, str):
        # Python 3 - convert from bytes to str
        data = data.decode("utf-8")
    json_data = json.loads(data)
    print(json.dumps(json_data, sort_keys=True, indent=4))


main()
