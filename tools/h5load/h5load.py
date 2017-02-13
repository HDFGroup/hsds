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
import math
import time
import urllib2
import getopt
import threading
import logging
import ConfigParser
import urlparse
import Queue
import itertools 
import numpy
import h5py 
import h5pyd 
from operator import add

UTILNAME = 'h5load'
__version__ = '0.0.1'
BASIC_LOAD, = range(1) # add more here later
PUTBLK = 2**20 * 4  # 4 MB

#----------------------------------------------------------------------------------
class pushItem:
   def __init__(self, fname, objname, offset, blocksize):
      self.fname, self.objname, self.offset, self.blocksize = fname, objname, offset, blocksize
      self.slices = None

   def get_slices(self):
      if self.slices == None:
         end = map(add, self.offset, self.blocksize)
         self.slices = tuple(map(slice, self.offset, end))
      return self.slices 

   def __str__(self):
      tmp = "fname=%s, objname=%s, offset=%s, size=%s, slices=%s" % (self.fname, self.objname, str(self.offset), str(self.blocksize), str(self.slices))
      return tmp
# pushItem 

#----------------------------------------------------------------------------------
def h5open(endpoint, objname, mode, usr=None, passwd=None, journal=None):
   '''Opens an hdf5 file or an hsds endpoint. For the open file 
      case, the endpoint could just be a directory. '''
   uri, jout = [None]*2
   if endpoint != None:
      fnm = os.path.join(endpoint, objname)
      uri = urlparse.urlparse(fnm)
   else:
      fnm = objname
   if uri != None and re.search('http[s]*', uri.scheme): 
      fd = h5pyd.File(objname, mode, endpoint=endpoint, username=usr, password=passwd)
   else:
      fd = h5py.File(fnm, mode)

   if journal:
      if os.path.exists(journal):
         logging.warn("WARN: journal file %s exists, over writing..." % journal)
      jout = open(journal, "w")

   return fnm, fd, jout 
# h5open

#---------------------------------------------------------------------------------
def push_items(queue, endpoint, url, usr, password):
   thrd = threading.current_thread()
   logging.info('starting thread '+str(thrd.ident)+' ...')

   fname, fdo, d = h5open(endpoint, url, "r+", usr, password) 

   # Attempt to cache these (input file handle and dataset handle) but we can't 
   # expect the input file will always be the same due to file merging. Note, 
   # file merging isn't implemented yet though.
   curfin, curfname, curdata, curdataout = [None]*4

   try:
      while True:
         if queue.empty() == True: 
            break

         itm = queue.get()
         if curfname != itm.fname:
            if curfin != None: 
               curfin.close()
            curfin = h5py.File(itm.fname, "r")
            curdata = curfin.get(itm.objname)
            curdataout = fdo.get(itm.objname)
            curfname = itm.fname
         
         slc = itm.get_slices()
         logging.debug(str(thrd.ident)+' :' +str(itm)+' --> '+fname+":"+itm.objname+', '+str(curdata)+' --> '+str(curdataout)) 
         # finally, write/upload the data
         try:
            curdataout[slc] = curdata[slc] 
         except TypeError, e:
            logging.error(str(e)+' : '+str(slc))

         queue.task_done()
   except Queue.Empty: 
      pass
   fdo.close()
   logging.info('thread '+str(thrd.ident)+' done...') 
#get_sizes

#----------------------------------------------------------------------------------
def copy_attribute(obj, name, attrobj):
   logging.debug("creating attribute %s in %s" % (name, obj.name))
   obj.attrs.create(name, attrobj)
# copy_attribute
      
#----------------------------------------------------------------------------------
def create_dataset_basic(fd, dobj):
   global PUTBLK 
   logging.debug("creating dataset %s" % (dobj.name))
   # We defer loading the actual data at this point, just create the object and try 
   # to make it as close to the original as possible for the basic copy/load.
   # This routine returns the dataset object (which will be loaded later, most likely)
   try:
      # If this dataset isn't already chunked we force a rechunk (if possible), for now.
      # TODO: add a better guess on these chunk sizes here using the PUTBLK value
      if dobj.chunks == None:
         try:
            adpchunk = h5py.filters.guess_chunk(dobj.shape, dobj.maxshape, dobj.dtype.itemsize)
         except ValueError, e:
            logging.warn("WARN : a rechunk guess on dataset %s failed, leaving as is" % (dobj.name)) 
            adpchunk = None
      else: 
         adpchunk = dobj.chunks 
      
      logging.debug("setting %s chunk size to %s, data shape %s" % (dobj.name, str(adpchunk), str(dobj.shape)))

      grp = fd.create_dataset( dobj.name, shape=dobj.shape, dtype=dobj.dtype, chunks=adpchunk, \
                               compression=dobj.compression, shuffle=dobj.shuffle, \
                               fletcher32=dobj.fletcher32, maxshape=dobj.maxshape, \
                               compression_opts=dobj.compression_opts, fillvalue=dobj.fillvalue, \
                               scaleoffset=dobj.scaleoffset)
      for da in dobj.attrs:
         copy_attribute(grp, da, dobj.attrs[da])
      
      if adpchunk:
         # Set possible chunks/dataset offset metadata. If the chunk sizes are small 
         # relative to the dataset size then this is a bit of overhead, example a
         # 1 dimensional dataset that was originally compressed may have a chunk size 
         # of 1.  TODO: adjust for this case 
         logging.debug("building %s offsets..." % (dobj.name))
         rngs = []
         for i, d in enumerate(dobj.shape): 
            rngs.append( range(0, d, adpchunk[i]) )
         # Note, the last dimension specified is the fastest changing on disk (hdf5lib). 
         offsets = itertools.product(*rngs)
      else:
         offsets = [[0]*len(dobj.shape)]
         adpchunk = dobj.shape

      return dobj.name, adpchunk, offsets 
   except Exception, e:
      logging.error("ERROR : failed to creating dataset in create_dataset_basic : "+str(e))
      return None
# create_dataset_basic

#----------------------------------------------------------------------------------
def create_group(fd, gobj):
   logging.debug("creating group %s" % (gobj.name))
   grp = fd.create_group(gobj.name)
   for ga in gobj.attrs:
      copy_attribute(grp, ga, gobj.attrs[ga])
# create_group
      
#----------------------------------------------------------------------------------
def hsds_basic_load(fls, endpnt, url, maxthrds, usr=None, passwd=None, journal=None):
   if len(fls) > 1:
      logging.warn("multi-file merging into one endpoint/url not supported yet, using first h5 file %s" % fls[0])

   datsets = []
   try:
      finname, finfd, jrnfout  = h5open(None, fls[0], "r", journal=journal)
      foutname, foutfd, d = h5open(endpnt, url, "w", usr=usr, passwd=passwd)

      def object_create_helper(name, obj):
         if isinstance(obj, h5py.Dataset):
            r = create_dataset_basic(foutfd, obj)
            if r != None: 
               datsets.append(r)
         elif isinstance(obj, h5py.Group):
            create_group(foutfd, obj)

      # build a rough map of the file using the internal function above
      finfd.visititems(object_create_helper)

      # Fully flush the h5pyd handle. The core of the source hdf5 file 
      # has been created on the hsds service up to now.
      foutfd.close() 
      
      # close up the source file, see reason(s) for this below
      finfd.close() 

      for d in datsets:
         name, chunks, offsets = d
         # queue up chunk metadata, chunks that will be delivered to hsds
         queue = Queue.Queue()
         for of in offsets: 
            queue.put( pushItem(finname, name, of, chunks) ) 

         # Init thread set for this batch. Note, we force the file 
         # handles into their own thread for safety (hence new threads
         # for each dataset), it adds minimal overhead. 
         thrds = []
         if queue.qsize() < maxthrds:
            nthrds = queue.qsize()
         else:
            nthrds = maxthrds
         logging.debug("using %d threads for %s" % (nthrds, d[0]))
         for i in range(nthrds):
            t = threading.Thread(target=push_items, args=(queue, endpnt, url, usr, passwd) )
            t.daemon = False
            t.start()
            thrds.append(t)
         for t in thrds:
            t.join()
      # for datsets

      return 0
   except IOError, e: 
      logging.error(str(e))
      return 1
# hsds_basic_load

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
   print("     -t | --nthreads <n>   :: The maximum number of threads to use. For small")
   print("                              datasets number of threads used may be < n")
   print("     --account <name> :: The name of the config file account section")
   print("                for the credentials in -c file.cnf (Default is [default])" )
   print("     --cnf-eg        :: Print a config file and then exit")
   print("     --log <logfile> :: logfile path")
   print("     --load-type <n> :: How to perform \"copy objects to hsds\" (default is basic=%d)" % () )
   print("     --journal <file> :: Journaling file for object integrity tracking (TBI)," )
   print("                         preforms object checksum-ing")
   print("     -v | --verbose :: Change log level to DEBUG.")
   print("     -h | --help    :: This message.")
   print("     %s version %s\n" % (UTILNAME, __version__))
#end print_usage

#----------------------------------------------------------------------------------
def load_config(urinm):  #TODO: add more error handling..
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
              'nthreads=', "conf=", "account=", 'cnf-eg', 'log=', 'url=', 'journal=', \
              'load-type=']
   verbose, nthrds, loadtype = 0, 1, BASIC_LOAD
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
         elif o == "-t" or o == '--nthreads':
            try: nthrds = int(v)
            except ValueError, e:
               sys.stderr.write('WARN num threads '+str(nthrds)+'?, setting to default value of 1\n')
               nthrds = 1
   except(getopt.GetoptError), e:
      sys.stderr.write(str(e)+"\n")
      sys.exit(1)

   if cnfg:
      # Note, command-line args take precedence over these config params
      try:
         config = load_config(cnfg)
         if usr == None and config.get(credCnfDefaultSec, 'user'):
            usr = config.get(credCnfDefaultSec, 'user')
         if passwd == None and config.get(credCnfDefaultSec, 'password'):
            passwd = config.get(credCnfDefaultSec, 'password')
         if endpnt == None and config.get(credCnfDefaultSec, 'endpoint'):
            endpnt = config.get(credCnfDefaultSec, 'endpoint')
      except ConfigParser.NoSectionError, e:
         sys.stderr.write('ConfigParser error : '+str(e)+'\n')
         sys.exit(1)

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
      logging.debug("user=%s, passwd=%s, max thrds=%d" % (str(usr), str(passwd), nthrds))

      if loadtype == BASIC_LOAD:
         r = hsds_basic_load(srcfls, endpnt, desturl, nthrds, usr, passwd, journal)
         sys.exit(r)
      else:
         logging.error("load type %d unknown" % loadtype )
         sys.exit(1)
   except KeyboardInterrupt:
      logging.error('Aborted by user via keyboard interrupt.')
      sys.exit(1)
#__main__

