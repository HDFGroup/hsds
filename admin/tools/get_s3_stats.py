#!/usr/bin/env python

#
# A rudimentry script to probe three test datasets...
#

import urllib2, urllib
import Queue
import threading
import logging
import json
import numpy

INVENTORIES = ( 'https://s3-us-west-2.amazonaws.com/nasanex/NEX-GDDP/nex-gddp-s3-files.json', \
                'https://s3-us-west-2.amazonaws.com/nasanex/NEX-DCP30/nex-dcp30-s3-files.json', \
                'https://s3-us-west-2.amazonaws.com/nasanex/LOCA/loca-s3-files.json')

NTRHS = 10

#---------------------------------------------------------------------------------
def get_json(invlink):
   logging.info("getting inventory from %s" % (invlink))
   jdata = json.loads( urllib2.urlopen(invlink).read() )
   return jdata

#---------------------------------------------------------------------------------
def select_model(jdata, model):
   return  [ k for k in jdata.keys() if jdata[k]['model'] == model ]

#---------------------------------------------------------------------------------
def select_experiment(jdata, expr):
   return  [ k for k in jdata.keys() if jdata[k]['experiment_id'] == expr ]

#---------------------------------------------------------------------------------
def select_var(jdata, vr):
   return  [ k for k in jdata.keys() if jdata[k]['variable'] == vr ]

#---------------------------------------------------------------------------------
def get_sets(jdata):
   models = set( [jdata[k]['model'] for k in jdata.keys()] )
   exprs = set( [jdata[k]['experiment_id'] for k in jdata.keys()] )
   vrs = set( [jdata[k]['variable'] for k in jdata.keys()] )
   return models, exprs, vrs 

#---------------------------------------------------------------------------------
def get_remote_info_json(jfname):
   try:
      logging.info('loading example '+jfname)
      rfo = urllib.urlopen(jfname)
      di = json.loads(rfo.read())
      nat, glbs = 0, 0
      for k,v in di.items():
        if k != 'dimensions' or k != 'variables':
            glbs +=1 
      for k,v in di['variables'].items():
         for a in v: nat += 1  
      dims = [ l for k, v in di['dimensions'].items() for d, l in v.items() if d == 'length' ]
      return { 'num global attr' : glbs, 'num vars' : len(di['variables'].keys()), 'num dims' : \
               len(di['dimensions'].keys()), 'ave attrs per var' : nat / len(di['variables'].keys()), \
               'dims sizes' : dims }
   except Exception, e:
      logging.warn("WARN get_remote_info_json on %s : %s, update S3 bucket" % (jfname, str(e)))
      return {}

#---------------------------------------------------------------------------------
def get_remote_size(rfname):
   try:
      rfo = urllib.urlopen(rfname)
      cl = rfo.info().getheaders("Content-Length")[0]
      return float(cl)
   except Exception, e:
      logging.warn("WARN get_remote_size on %s failed : %s" % (rfname, str(e)))
      return None
#get_remote_size

#---------------------------------------------------------------------------------
def queue_list(invlink):
   queue = Queue.Queue()
   jsn = get_json(invlink)
   for k in jsn.keys():
      queue.put( k )
   return queue, jsn
#queue_list

#---------------------------------------------------------------------------------
def get_sizes(jsn):
   thrd = threading.current_thread()
   logging.info('starting thread '+str(thrd.ident)+' ...')
   try:
      while True:
         if queue.empty() == True: 
            break
         itm = queue.get()
         logging.info(str(thrd.ident)+' :' +str(itm))
         val = get_remote_size(itm)
         if val != None: jsn[itm]['objsize'] = val
         queue.task_done()
   except Queue.Empty: 
      pass
   logging.info('thread '+str(thrd.ident)+' done...') 
#get_sizes

#---------------------------------------------------------------------------------
def set_stat(jsn, items, selectfunc, hiswdth=64, lab='histogram'):
   d = {}
   lables, means, stds, sums = [], [], [], []
   for i in items:
      fls = selectfunc(jsn, i)
      d[i] = [ jsn[f]['objsize'] for f in fls ]
      np = numpy.array(d[i], dtype='f8')
      lables.append( i )
      means.append( np.mean() )
      stds.append( np.std() )
      sums.append( np.sum() )

   # print quasi histogram
   gb = 2.0**30.0
   mb = 2.0**20.0
   sumsnp = numpy.array(sums, dtype='f8')
   sumsmax, sumsmin = sumsnp.max(), sumsnp.min() 
   sumall = sumsnp.sum() 
   fstr = "%15s |%s      [%0.1fGB sz, %0.1fMB stdv]" 
   print "\n\n%s : min=%.1f, max=%.1f total=%.0f (GB)\n%s" % (lab, sumsmin/gb, sumsmax/gb, sumall/gb, '-'*(hiswdth+15))
   for i, s in enumerate(sums): 
      nx = int(round((sums[i] / (sumsmax*1.2))*hiswdth))
      hst = '*'*nx
      hst += ' '*(hiswdth-nx)
      print fstr % (lables[i], hst, sums[i]/gb, stds[i]/mb)
#set_stat

#---------------------------------------------------------------------------------
def summarize_size(jsn, n):
   models, exprs, vrs = get_sets(jsn)
   logging.info('building stats for models ...')
   set_stat(jsn, models, select_model, lab='models')
   logging.info('building stats for experiments ...')
   set_stat(jsn, exprs, select_experiment, lab='scenarios')
   logging.info('building stats for variables ...')
   set_stat(jsn, vrs, select_var, lab='variables')
   print 'Number of files: ', n
#summarize_size

#---------------------------------------------------------------------------------
if __name__ == '__main__':
   logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
   for inv in INVENTORIES:
      thrds = []
      queue, jsn = queue_list( inv )
      eginfo = get_remote_info_json( jsn.iterkeys().next()[:-2]+'json') 
      nfiles = queue.qsize()
      for i in range(NTRHS):
         t = threading.Thread(target=get_sizes, args=(jsn,))
         t.daemon = False
         t.start()
         thrds.append(t)
      for t in thrds: 
         t.join()
   
      summarize_size(jsn, nfiles)
      for k, v, in eginfo.items(): 
         print k, '=', v
#__main__

