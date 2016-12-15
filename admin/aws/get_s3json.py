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
import boto.s3
import boto.s3.connection 
from boto.s3.key import Key
import config
 
# This is a utility to dump a JSON obj (group, dataset, ctype) given the
# the objects UUID
    

#
# Print usage and exit
#
def printUsage():
    print("usage: python get_s3json [--bucket_name=<bucket>] [--aws_s3_gateway=<s3_endpoint>] objid ")
    print("  objid: s3 JSON obj to fetch")
    print("  Example: python get_s3json --aws_s3_gateway=http://192.168.99.100:9000 --bucket_name=minio.hsdsdev t-cf2fc310-996f-11e6-8ef6-0242ac110005")
    sys.exit(); 
       
               
def main():
    if len(sys.argv) == 1 or sys.argv[1] == "-h" or sys.argv[1] == "--help":
        printUsage()
        sys.exit(1)
    
    obj_id = sys.argv[-1]
     
    region = config.get("aws_region")
    print("region", region)
    conn = boto.s3.connect_to_region(
        region,
        #aws_access_key_id=awsAccessKeyID,
        #aws_secret_access_key=awsSecretKey,
        calling_format=boto.s3.connection.OrdinaryCallingFormat()
    )
    
    bucket_name = config.get("bucket_name")
    print("bucket_name:", bucket_name)
    bucket = conn.get_bucket(bucket_name)
    k = Key(bucket)
    k.key = obj_id
    data = k.get_contents_as_string()
    json_data = json.loads(data)
    print(json.dumps(json_data, sort_keys=True, indent=4))

 

     
main()

    
	
