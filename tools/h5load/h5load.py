#!/usr/bin/env python
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
import os
import re
import urllib2
import getopt
import datetime
import string
import threading
import logging
import ConfigParser
import urlparse
import h5py 
import h5pyd 

UTILNAME = 'h5load'
__version__ = '0.0.1'
BASIC_LOAD, = range(1) # add more here later

#----------------------------------------------------------------------------------
def hsds_basic_load(fls, endpnt, url, nprocs, usr=None, passwd=None, journal=None):
   if len(fls) > 1:
      logging.warn("multi-file merging into one endpoint/url not supported yet, using first h5 file %s" % fls[0])
   try:
      h5fd = h5py.File(fls[0], 'r')
      for grp in h5fd: logging.debug("found %s group" % grp) 
      h5fd.close() 
      return 0
   except IOError, e: 
      logging.error(str(e))
      return 1
#hsds_basic_load

#----------------------------------------------------------------------------------
def usage():
   print("\n  %s [ OPTIONS ] -d <url>" % UTILNAME)
   print("    OPTIONS:")
   print("     -s | --source <file.h5> :: The hdf5 source file. There can be")
   print("                   multiple -s options for file merging (TBI). If")
   print("                   no -s options is given files are read from stdin")
   print("     -e | --endpoint <domain> :: The hsds endpoint, e.g. http://example.com:8080")
   print("     -d | --url <hsdsurl> :: The hsds endpoint, e.g. foo.bar.data" )
   print("     -u | --user <username>   :: User name credential")
   print("     -p | --passwd <password> :: Password credential")
   print("     -c | --conf <file.cnf>  :: A credential and config file")
   print("     -t | --nprocs <n>   :: The number of processes to use.")
   print("     --account <name> :: The name of the config file account section")
   print("                for the credentials in -c file.cnf (Default is [default])" )
   print("     --cnf-eg        :: Print a config file and then exit")
   print("     --log <logfile> :: logfile path")
   print("     --load-type :: How to perform \"copy objects to hsds\" (default is basic)")
   print("     --journal <file> :: Journaling file for object integrity tracking (TBI)," )
   print("                         preforms object checksum-ing")
   print("     -v | --verbose :: Change log level to DEBUG.")
   print("     -h | --help    :: This message.")
   print("     %s version %s\n" % (UTILNAME, __version__))
#end print_usage

#----------------------------------------------------------------------------------
def load_config(urinm):  #TODO: add more error handleing..
   uri = urlparse.urlparse(urinm)
   if re.search('http[s]*', uri.scheme): 
      try:
         fname = os.path.split(urinm)[1]
         fout = open(fname, "w")
         fout.write( urllib2.urlopen(urinm).read() )
         fout.close()
      except urllib2.URLError, e:
         sys.stderr.write(urinm+' : '+str(e)+'\n') # logger not initialize yet.
         os.unlink(fname)
         sys.exit(1)
   else:
      fname = urinm
   config = ConfigParser.SafeConfigParser()
   config.read(fname)
   return config 
#load_config

#----------------------------------------------------------------------------------
def print_config_example():
   print("[default]")
   print("user = <uid>")
   print("password = <passwd>")
   print("endpoint = https://example.com")
#print_config_example

#----------------------------------------------------------------------------------
if __name__ == "__main__":
   longOpts=[ 'help', 'verbose=', 'endpoint=', 'user=', 'source=', 'password=', \
              'nprocs=', "conf=", "account=", 'cnf-eg', 'log=', 'url=', 'journal=', \
              'load-type=']
   verbose, nprcs, loadtype = 0, 1, BASIC_LOAD
   endpnt, usr, passwd, cnfg, logfname, desturl, journal = [None]*7
   credCnfDefaultSec = 'default'
   srcfls = []
   loglevel = logging.INFO

   try:
      opts, args = getopt.getopt(sys.argv[1:], "hve:u:s:p:t:c:d:", longOpts)
      for o, v in opts:
         if o == "-h" or o == '--help':
            usage()
            sys.exit(0)
         elif o == "-v" or o == '--verbose':
            loglevel = logging.DEBUG
         elif o == "-s" or o == '--source':
            srcfls.append(v)
         elif o == "-e" or o == '--endpoint':
            endpnt = v
         elif  o == '--journal':
            journal = v
         elif  o == '--load-type':
            try: loadtype = int(v)
            except ValueError, e:
               sys.stderr.write('WARN load-type is '+str(loadtype)+'? Setting to default value of BASIC_LOAD\n')
               loadtype = BASIC_LOAD
         elif o == "-d" or o == '--url':
            desturl = v
         elif o == "-u" or o == '--user':
            usr = v
         elif o == "-p" or o == '--passwd':
            passwd = v
         elif o == "-c" or o == '--conf':
            cnfg = v
         elif o == '--cnf-eg':
            print_config_example()
            sys.exit(0)
         elif o == '--account':
            credCnfDefaultSec = v
         elif o == '--log':
            logfname = v
         elif o == "-t" or o == '--nprocs':
            try: nprcs = int(v)
            except ValueError, e:
               sys.stderr.write('WARN num procs '+str(nprcs)+'?, setting to default value of 1\n')
               nprcs = 1
   except(getopt.GetoptError), e:
      sys.stderr.write(str(e)+"\n")
      sys.exit(1)

   if cnfg:
      #Note, command-line args take precedence over these
      config = load_config(cnfg)
      if usr == None and config.get(credCnfDefaultSec, 'user'):
         usr = config.get(credCnfDefaultSec, 'user')
      if passwd == None and config.get(credCnfDefaultSec, 'password'):
         passwd = config.get(credCnfDefaultSec, 'password')
      if endpnt == None and config.get(credCnfDefaultSec, 'endpoint'):
         endpnt = config.get(credCnfDefaultSec, 'endpoint')

   logging.basicConfig(filename=logfname, format='%(asctime)s %(message)s', level=loglevel)
   
   if desturl == None:
      logging.error('No destination url given, try -h for help\n')
      sys.exit(1)
   
   if endpnt == None:
      logging.error('No endpoint given, try -h for help\n')
      sys.exit(1)

   try:
      if len(srcfls) == 0:
         logging.info("building h5 file list from stdin...")
         srcfls = [ f.strip() for f in sys.stdin ]

      logging.info("loading %d source files to %s" % (len(srcfls), os.path.join(endpnt, desturl)))
      if journal: logging.info("using journal %s" % journal)
      logging.debug("user=%s, passwd=%s, nprcs=%d" % (str(usr), str(passwd), nprcs))

      if loadtype == BASIC_LOAD:
         r = hsds_basic_load(srcfls, endpnt, desturl, nprcs, usr, passwd, journal)
         sys.exit(r)
      else:
         logging.error("load type %d unknown" % loadtype )
         sys.exit(1)
   except KeyboardInterrupt:
      logging.error('Aborted by user via keyboard interrupt.')
      sys.exit(1)
#__main__

