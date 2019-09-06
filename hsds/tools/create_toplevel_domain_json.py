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
import asyncio
import sys
import time
from aiobotocore import get_session
from aiohttp.client_exceptions import ClientOSError
from util.domainUtil import validateDomain, getParentDomain
from util.idUtil import getS3Key
from util.s3Util import putS3JSONObj, isS3Obj, releaseClient
import config
import hsds_logger as log

 
# This is a utility to create a top-level domain objecct.
# Sub-domains can be created using the REST API 
# (assuming the requestor has the proper authorization),
# but a top-level domain must be created using S3 directory rather
# than going through a service.

#
# Note - should be obsolete now.  Use admin account to create top level domains
#

#
# Print usage and exit
#
def printUsage():
    print("usage: python create_toplevel_domain_json.py --user=<username> [--private] --domain=<domain> ")
    print("  options --user: username of who will be owner of the domain (will have full permissions)")
    print("  options --private: if set, private for all other users, otherwise public read")
    print("  options --domain: domain to be assigned for the user.  If not set, the domain of <username>.home will be used")
    print(" ------------------------------------------------------------------------------")
    print("  Example - ")
    print("       python create_toplevel_domain_json.py --user=joebob --domain=/home/joebob ")
    print("  The user argument can also be a list of usernames (in this case the domain arg can't be used):")
    print("       python create_toplevel_domain_json.py --user='user1 user2 user3'")
    sys.exit(); 
    
async def createDomains(app, usernames, default_perm, domain_name=None):
    now = time.time()
    owner_perm = {'create': True, 'read': True, 'update': True, 'delete': True, 'readACL': True, 'updateACL': True } 
    for username in usernames:
        if domain_name is None:
            domain = "/home/" + username
        else:
            domain = domain_name
        validateDomain(domain)  # throws ValueError if invalid
        if domain != domain.lower():
            raise ValueError("top-level domains must be all lowercase")
        # construct the json obj
        domain_json = {}
        domain_json["owner"] = username
        acls = {}
        acls["default"] = default_perm
        acls[username] = owner_perm
        domain_json["acls"] = acls
        domain_json["lastModified"] = now
        domain_json["created"] = now
        await createDomain(app, domain, domain_json)

async def createDomain(app, domain, domain_json):
    try:
        s3_key = getS3Key(domain)
        domain_exists = await isS3Obj(app, s3_key)
        if domain_exists:
            raise ValueError("Domain already exists")
        parent_domain = getParentDomain(domain)
        if parent_domain is None:
            raise ValueError("Domain must have a parent")
        
        log.info("writing domain")
        await putS3JSONObj(app, s3_key, domain_json)
        print("domain created!  s3_key: {}  domain_json: {}".format(s3_key, domain_json))
    except ValueError as ve:
        print("Got ValueError exception: {}".format(str(ve)))
    except ClientOSError as coe:
        print("Got S3 error: {}".format(str(coe)))  

#
# Shutodwn - release S3 client
# 
async def shutdown(app):
    log.info("closing S3 connections")
    await releaseClient(app)  
               
def main():
    default_public_perm =  {'create': False, 'read': True, 'update': False, 'delete': False, 'readACL': False, 'updateACL': False } 
    default_private_perm =  {'create': False, 'read': False, 'update': False, 'delete': False, 'readACL': False, 'updateACL': False } 
     
    if len(sys.argv) == 1 or sys.argv[1] == "-h" or sys.argv[1] == "--help":
        printUsage()
        sys.exit(1)

    default_perm = default_public_perm  # will switch if private is specified
    userarg = None
    domain = None
    for arg in sys.argv[1:]:
        if arg.startswith('--user='):  
            userarg = arg[len('--user='):]
        elif arg == '--private':
            default_perm = default_private_perm
        elif arg.startswith('--domain='):
            domain = arg[len('--domain='):]
        else:
            print("Unexpected argument:", arg)
            printUsage()
            sys.exit(1)    
      
    if not userarg:
        print("No user supplied")
        printUsage()
        sys.exit(1)  
    usernames = []
    if userarg[0] == '[' and userarg[-1] == ']':
        names = userarg[1:-1].split(',')
        for name in names:
            usernames.append(name)
    else:
        usernames.append(userarg)

    for username in usernames:
        if username != username.lower():
            raise ValueError("username must be lowercase")
        if not username[0].isalpha():
            raise ValueError("first character of username must be character a-z")
        for c in username:
            if c != '_' and not c.isalnum():
                raise ValueError("username must consist of the characters a-z, numeric or underscore")
        if len(username) < 3:
            raise ValueError("username must have at least three characters")
    

    # we need to setup a asyncio loop to query s3
    loop = asyncio.get_event_loop()
    session = get_session(loop=loop)
    app = {}
    app["session"] = session
    app["loop"] = loop
    app["bucket_name"] = config.get("bucket_name")

    loop.run_until_complete(createDomains(app, usernames, default_perm, domain_name=domain))
    loop.run_until_complete(shutdown(app))

    
    loop.close()
    

    print("done!")

         
            
    

main()
