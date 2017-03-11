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
import getopt
import threading
import logging
import itertools 
import tempfile
import zlib
import gzip
import hashlib
from operator import add

if sys.version_info >= (3, 0):
   import configparser
   import queue
   import urllib.parse as urlparsesh
else:
   import ConfigParser as configparser
   import Queue as queue
   import urlparse as urlparsesh

try:
   import numpy
   import pycurl
   import h5py 
   import h5pyd 
except ImportError as e:
   sys.stderr.write("ERROR : %s : install it to use this utility...\n" % str(e)) 
   sys.exit(1)

__version__ = '0.0.1'

UTILNAME = 'h5load'
BASIC_LOAD, = list(range(1)) # add more here later
PUTBLK = 2**20 * 4  # 4 MB
ISHTTP = re.compile('http[s]*')

#----------------------------------------------------------------------------------
class pushItem:
   def __init__(self, fname, objname, offset, blocksize):
      self.fname, self.objname, self.offset, self.blocksize = fname, objname, offset, blocksize
      self.slices, self.md5 = [None]*2
      self.crc32 = 0

   def get_slices(self):
      if self.slices == None:
         ends = tuple(map(add, self.offset, self.blocksize))
         self.slices = tuple( map(slice, self.offset, ends) )
      return self.slices 

   def __str__(self):
      return "fname=%s, objname=%s, offset=%s, size=%s, slices=%s, crc32=%s, md5=%s" % \
             (self.fname, self.objname, str(self.offset), str(self.blocksize), str(self.slices), str(self.crc32 & 0xffffffff), self.md5)
   
   def __repr__(self):
      # TODO: optimize
      t = '('
      for s in self.slices:
         t+= "%d:%d " % (s.start, s.stop)
      t= t[:-1]+')'
      return "%s,%s,%s,%s,%s,%s\n" % \
             (self.objname, str(self.offset), str(self.blocksize), t, str(self.crc32 & 0xffffffff), self.md5)
   
   def set_chunk_crc32(self, data):
      self.crc32 = zlib.crc32(data) 
   
   def set_chunk_md5(self, data):
      self.md5 = hashlib.md5(data).hexdigest()
# pushItem 

#----------------------------------------------------------------------------------
class Journal:
   JOURNAL_LOCK = threading.Lock()

   def __init__(self, fname, libzd=False):
      self.fname, self.libzd = fname, libzd
      self.fd = None 
      self.fopen()

   def fopen(self):
      if os.path.exists(self.fname):
         logging.warn("WARN: journal file %s exists, over writing..." % journal)

      if self.libzd:
         logging.debug("journal file will be gzip'ed...") 
         self.fd = gzip.open(self.fname, 'wb')
      else:
         self.fd = open(self.fname, "wb")
   
   def write_pitem(self, pi):
      self.JOURNAL_LOCK.acquire()
      self.fd.write(repr(pi).encode())
      self.JOURNAL_LOCK.release()
   
   def write_raw(self, s):
      self.JOURNAL_LOCK.acquire()
      self.fd.write(s)
      self.JOURNAL_LOCK.release()
   
   def close(self):
      if self.fd != None:
         self.fd.close()

   def __str__(self):
      return "fname=%s, append=%r, fd=%s, lock=%s" % (self.fname, self.append, str(self.fd), str(self.lock))

   def __del__(self):
      self.close()
# Journal

#----------------------------------------------------------------------------------
def h5open(objname, endpoint, mode, usr=None, passwd=None):
   """Opens an hdf5 file or hsds endpoint. 

      This routine will open an hdf5 file, which could be netcdf 4 file, or an hsds endpoint/url

      Args:
         objname: The name url or file, e.g. 
                  /foo/bar/tasmax_day_BCSD_rcp45_r1i1p1_CanESM2_2050.nasa.data.hdfgroup.org 
                  or foobar.h5
         endpoint: The endpoint of the service, e.g. https://www.example.com
                   This could be None, a directory or the path to an external http link.
         mode: The open mode, r, r+, w, etc...
         usr: A user name if needed
         passwd: A password associated with the user name.

      Note:
         objname and endpoint are related in that if something like 
         objname=data.h5 and endpoint=./ it is assumed that this is a 
         local file and h5py is used, if objname=data.h5 and endpoint=http://www.example.com 
         with a mode "w", "a" or "r+" it is assumed that this is an hsds endpoint url and 
         h5pyd is used (for an hsds write/load). If objname=data.h5 and endpoint=http://www.example.com 
         with mode "r" then it is assumed that this is an external file posted somewhere and the 
         file will be downloaded to a temp file location, opened and it's tempfile file handle and 
         tempfile name will be returned.

      Returns:
        A file descriptor and a fname or endpoint/url name
   """
   global ISHTTP 
   fd, fname = [None]*2
   if endpoint:
      uri = urlparsesh.urlparse(endpoint)
   else:
      uri = urlparsesh.urlparse(objname)

   if uri != None and uri.netloc == '':
      fname = objname
      logging.debug("opening local file \"%s\" (h5py) ...", fname)
      fd = h5py.File(objname, mode)
   elif uri != None and uri.netloc != '' and mode in ['r', 'rb']:
      tfile = tempfile.NamedTemporaryFile(suffix='.h5', delete=False, mode="wb")
      logging.debug("staging in file \"%s\" to \"%s\" (h5py) ...", objname, tfile.name)
      crl = pycurl.Curl()
      crl.setopt(crl.USERAGENT, 'h5load utility')
      crl.setopt(crl.URL, objname)
      crl.setopt(crl.FOLLOWLOCATION, True)
      crl.setopt(crl.WRITEDATA, tfile)
      crl.perform()
      crl.close()
      tfile.close()
      fd = h5py.File(tfile.name, mode)
      fname = tfile.name 
   elif uri != None and uri.netloc != '' and mode in ['w', 'r+', 'a']:
      fname = os.path.join(endpoint, objname)
      logging.debug("opening hsds/h5serv on \"%s\" (h5pyd) ...", fname)
      try:
         fd = h5pyd.File(objname, mode, endpoint=endpoint, username=usr, password=passwd)
      except ValueError as e:
         logging.error("ERROR : opening hsds/h5serv on \"%s\" failed (h5pyd) %s ...", fname, str(e))
         sys.exit(1)

   return fname, fd 
# h5open

#---------------------------------------------------------------------------------
def push_items(iqueue, endpoint, url, usr, password, jfl=None, domd5=None):
   thrd = threading.current_thread()
   logging.info('starting thread '+str(thrd.ident)+' ...')

   fname, fdo = h5open(url, endpoint, "r+", usr, password) 

   # Attempt to cache these (input file handle and dataset handle) but we can't 
   # expect the input file will always be the same due to file merging. Note, 
   # file merging isn't implemented yet though.
   curfin, curfname, curdata, curdataout = [None]*4

   try:
      while True:
         if iqueue.empty() == True: 
            break

         itm = iqueue.get()
         if curfname != itm.fname:
            if curfin != None: 
               curfin.close()
            curfin = h5py.File(itm.fname, "r")
            curdata = curfin.get(itm.objname)
            curdataout = fdo.get(itm.objname)
            curfname = itm.fname
         
         slc = itm.get_slices()
         # finally, write/upload the data
         try:
            if jfl:
               byts = curdata[slc].tostring(order='C')
               if domd5:
                  itm.set_chunk_md5(byts)
               else:
                  itm.set_chunk_crc32(byts)
               jfl.write_pitem(itm)
         
            logging.debug(str(thrd.ident)+' :' +str(itm)+' --> '+fname+":"+itm.objname+', '+str(curdata)+' --> '+str(curdataout)) 

            curdataout[slc] = curdata[slc] 
         except TypeError as e:
            logging.error(str(e)+' : '+str(slc))

         iqueue.task_done()
   except iqueue.Empty: 
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
         except ValueError as e:
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
            rngs.append( list(range(0, d, adpchunk[i])) )
         # Note, the last dimension specified is the fastest changing on disk (hdf5lib). 
         offsets = itertools.product(*rngs)
      else:
         offsets = [[0]*len(dobj.shape)]
         adpchunk = dobj.shape

      return dobj.name, adpchunk, offsets 
   except Exception as e:
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
def hsds_basic_load(fls, endpnt, url, maxthrds, usr=None, passwd=None, journal=None, domd5=None, gzjourn=None):
   if len(fls) > 1:
      logging.warn("multi-file merging into one endpoint/url not supported yet, using first h5 file %s" % fls[0])

   datsets, journfl  = [], None
   try:
      if journal:
         journfl = Journal(journal, libzd=gzjourn)
         
      finname, finfd, = h5open(fls[0], None, "r")
      foutname, foutfd = h5open(url, endpnt, "w", usr=usr, passwd=passwd)

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
         iqueue = queue.Queue()
         for of in offsets: 
            iqueue.put( pushItem(finname, name, of, chunks) ) 

         # Init thread set for this batch. Note, we force the file 
         # handles into their own thread for safety (hence new threads
         # for each dataset), it adds minimal overhead. 
         thrds = []
         if iqueue.qsize() < maxthrds:
            nthrds = iqueue.qsize()
         else:
            nthrds = maxthrds
         logging.debug("using %d threads for %s" % (nthrds, d[0]))
         for i in range(nthrds):
            t = threading.Thread(target=push_items, args=(iqueue, endpnt, url, usr, passwd, journfl, domd5) )
            t.daemon = False
            t.start()
            thrds.append(t)
         for t in thrds:
            t.join()
      # for datsets

      return 0
   except IOError as e: 
      logging.error(str(e))
      return 1
# hsds_basic_load

#----------------------------------------------------------------------------------
def usage():
   print(("\n  %s [ OPTIONS ] -d <url>" % UTILNAME))
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
   print(("     --load-type <n> :: How to perform \"copy objects to hsds\" (default is basic=%d)" % (BASIC_LOAD) ))
   print("     -j | --journal <file> :: A journaling file for object integrity tracking," )
   print("                              preforms chunk-level crc32'ing or md5'ing (default crc32)")
   print("     --md5 :: With journaling use chunk-level md5 instead of crc32")
   print("     --jz :: use gzip to compress the journal file. Output file is gzip compatible.")
   print("     -v | --verbose :: Change log level to DEBUG.")
   print("     -h | --help    :: This message.")
   print(("     %s version %s\n" % (UTILNAME, __version__)))
#end print_usage

#----------------------------------------------------------------------------------
def load_config(urinm):  #TODO: add more error handling..
   uri = urlparsesh.urlparse(urinm)
   if re.search('http[s]*', uri.scheme): 
      try:
         fname = os.path.split(urinm)[1]
         fout = open(fname, "wb")
         crl = pycurl.Curl()
         crl.setopt(crl.USERAGENT, 'h5load utility')
         crl.setopt(crl.URL, urinm)
         crl.setopt(crl.WRITEDATA, fout)
         crl.perform()
         if crl.getinfo(pycurl.HTTP_CODE) != 200:
            sys.stderr.write('ERROR : failed to get '+urinm+'\n') # logger not initialize yet.
            crl.close()
            sys.exit(1)
         crl.close()
         fout.close()
      except IOError as e:
         sys.stderr.write('ERROR : '+fname+' : '+str(e)+'\n') # logger not initialize yet.
         if os.path.exists(fname): os.unlink(fname)
         sys.exit(1)
   else:
      fname = urinm
   config = configparser.SafeConfigParser()
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
              'load-type=', "md5", "jz"]
   verbose, nthrds, loadtype = 0, 1, BASIC_LOAD
   endpnt, usr, passwd, cnfg, logfname, desturl, journal, domd5, gzpd = [None]*9
   credCnfDefaultSec = 'default'
   srcfls = []
   loglevel = logging.INFO

   try:
      opts, args = getopt.getopt(sys.argv[1:], "hve:u:s:p:t:c:d:j:", longOpts)
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
         elif o == '--md5':
            domd5 = 1
         elif  o == '-j' or o == '--journal':
            journal = v
         elif  o == '--load-type':
            try: loadtype = int(v)
            except ValueError as e:
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
         elif  o == '--jz':
            gzpd = 1
         elif o == '--cnf-eg':
            print_config_example()
            sys.exit(0)
         elif o == '--account':
            credCnfDefaultSec = v
         elif o == '--log':
            logfname = v
         elif o == "-t" or o == '--nthreads':
            try: nthrds = int(v)
            except ValueError as e:
               sys.stderr.write('WARN num threads '+str(nthrds)+'?, setting to default value of 1\n')
               nthrds = 1
   except(getopt.GetoptError) as e:
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
      except configparser.NoSectionError as e:
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

      if journal: 
         if gzpd: gzpdn = 'yes'
         else: gzpdn = 'no'
         logging.info("using journal %s (libz\'d = %s)" % (journal, gzpdn))

      logging.debug("user=%s, passwd=%s, max thrds=%d" % (str(usr), str(passwd), nthrds))

      if loadtype == BASIC_LOAD:
         r = hsds_basic_load(srcfls, endpnt, desturl, nthrds, usr, passwd, journal, domd5, gzpd)
         sys.exit(r)
      else:
         logging.error("load type %d unknown" % loadtype )
         sys.exit(1)
   except KeyboardInterrupt:
      logging.error('Aborted by user via keyboard interrupt.')
      sys.exit(1)
#__main__

