#!/usr/bin/env python

import sys, os, re
import urllib2, urllib
import json
import datetime
import logging
import numpy

GDDP_INVENTORY = 'https://s3-us-west-2.amazonaws.com/nasanex/NEX-GDDP/nex-gddp-s3-files.json'

#---------------------------------------------------------------------------------
def select_gddp_data_on_s3(model, expr, var, whist=1):
   global GDDP_INVENTORY 
   logging.info("getting inventory for %s, %s, %s (historical=%d)" % (model, expr, var, whist))
   dat = json.loads( urllib2.urlopen(GDDP_INVENTORY).read() )
   if whist == 1:
      datof = { dat[k]['year'] : k  for k in dat.keys() if dat[k]['model'] == model \
                  and dat[k]['variable'] == var  and (dat[k]['experiment_id'] == expr \
                  or dat[k]['experiment_id'] == 'historical')  }
   else:
      datof = { dat[k]['year'] : k  for k in dat.keys() if dat[k]['model'] == model \
                  and dat[k]['variable'] == var  and dat[k]['experiment_id'] == expr } 
   return datof 
#select_s3_data

#---------------------------------------------------------------------------------
def ping_remote_file(rfname):
   try:
      site = urllib.urlopen(rfname)
      meta = site.info()
      cl = meta.getheaders("Content-Length")[0]
      cl = float(cl)
      return cl
   except TypeError:
      logging.warn("WARN ping %s failed" % (rfname))
      return None
#ping_remote_file

#---------------------------------------------------------------------------------
def show_size():
   logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

   #model = 'CanESM2'
   model = 'ACCESS1-0'
   expr = 'Rcp45'
   tname = 'Gddp'+model+expr 
   var = "tasmax"
   szs = []
   
   s3files = select_gddp_data_on_s3(model.lower(), expr.lower(), var, 1)
   tstart = datetime.datetime.now()
   years = s3files.keys()
   years.sort()
   for i, y in enumerate(years):
      logging.info("Pinging [%d] %s" % (i, s3files[y]))
      fsz = ping_remote_file(s3files[y])
      if fsz is not None: szs.append(fsz)
   #done

   a = numpy.array(szs, dtype='f8')

   logging.info("%s %s %s" % (model.lower(), expr.lower(), var.lower() ))
   s, mu = a.sum(), a.mean()
   gb = 2.0**30.0
   logging.info('  total size = %0.0f bytes, %0.2f GB' % (s, s/gb))
   logging.info('  mean file size = %0.0f bytes, %0.2f GB' % (mu, mu/gb))
   logging.info('  size variability = %0.1f ' % a.std())
   
   tend = datetime.datetime.now()
   tmex = tend - tstart
   logging.info('Query Example Exec ~Time:'+str(tmex))
#show_size

#---------------------------------------------------------------------------------
if __name__ == '__main__':
   show_size()
