#!/usr/bin/env python

# This is a basic driver script for running the h5py example probeclimh5.py 
# on a cross section of aws instances types. This script was essentially 
# used to create a spreadsheet of probeclimh5.py runtimes and data download 
# times for basic baseline performance test. Note, you will need to config 
# boto for credentials, etc... If you have nothing in ~/.aws/ then config 
# boto first piror to running this test script. 
# example:
#   ./spawnprobes.py run.config
# We may not want to include this hack in any hsds release.

import sys
import logging
import time
import ConfigParser
import socket
try:
   import boto3, botocore
   import paramiko
   from paramiko.ssh_exception import *
except ImportError, e:
   sys.stderr.write("Missing module needed to run this example test.\n")
   sys.stderr.write("Maybe try pip install --user "+str(e.args[0].split()[-1])+'\n')
   sys.exit(1)

#---------------------------------------------------------------------------------------
def run_test(cmd, host, keyfile, uid):
   try:
      pky = paramiko.rsakey.RSAKey(filename=keyfile)
      logging.info("Issuing command \""+cmd+"\" @ "+host+', IdentityFile='+str(keyfile))
      for t in range(3, 8):
         try: 
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(host, username=uid, pkey=pky)
            stdin, stdout, stderr = ssh.exec_command(cmd)
            logging.info(stdout.read())
            logging.info(stderr.read())
            time.sleep(1)
            ssh.close()
            break
         except (SSHException, socket.error), e:
            rst = 2**t 
            logging.info('WARN : '+str(e)+', retrying in '+str(rst)+'s ...')
            time.sleep(rst)
   except (BadHostKeyException, AuthenticationException), e: 
      logging.warn('WARN : '+str(e)+" : for host : "+str(host)+", try # "+str(t))
#run_test

#---------------------------------------------------------------------------------------
def spawn_test(rmtscript, itype, region, ami, keypairname, keypairfile, unixid='ec2-user', secgrp=[]):
   logging.info("starting test with type "+itype+" in region "+region+" using "+ami+' security group '+str(secgrp))
   try:
      res = None
      ec2d = boto3.resource('ec2', region_name=region)
      rid = ec2d.create_instances( ImageId=ami, InstanceType=itype, MinCount=1, 
                                    MaxCount=1, SecurityGroupIds=secgrp, KeyName=keypairname)
      if len(rid) < 1:
         sys.stderr.write("create_instances didn't return any id's?\n")
         return
      else:
         res = rid[0] 
      res.wait_until_running()
      ec2d.create_tags( Resources=[res.id], Tags=[{'Key': 'Name', 'Value': 'probetest'}] )
      res.reload()
      run_test(rmtscript, res.public_dns_name, keypairfile, unixid) 
      res.terminate()
   except botocore.exceptions.ClientError, e:
      loging.error(str(e))
#spawn_test

if __name__ == '__main__':
   logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
   if len(sys.argv) < 2:
      sys.stderr.write("spawnprobes.py config\n")
      sys.exit(1)

   cnfg = ConfigParser.ConfigParser()
   cnfg.read(sys.argv[1])
   itypes = [ s.strip() for s in cnfg.get('params', 'itypes').split(',') ]
   for typ in itypes:
      spawn_test( cnfg.get('params', 'remotescript'), typ, cnfg.get('params', 'region'), \
                   cnfg.get('params', 'ami'), cnfg.get('params', 'keypairname'), 
                   cnfg.get('params', 'keypairfile'), cnfg.get('params', 'unixid'), 
                   [cnfg.get('params', 'securitygroup')] )
#__main__

