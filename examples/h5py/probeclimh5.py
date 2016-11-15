#!/usr/bin/env python

# Example script that simulates common data access patterns for a basic 
# science user doing post processing, viewing and or analyzing data.
# The hdf5 lib should be complied in threadsafe mode for this. 
#
# Script used for h5py, h5pyd and hsds to contrast timings.
# 
# Though the example may seem contrived, the data access pattern 
# below is common for the workstation bound scientist...

import sys, os
import datetime
import logging
import threading
import random
import csv
import urlparse
import re
try:
    import numpy
    import h5py
    import h5pyd
    import datencutil
    import requests
except ImportError, e:
    sys.stderr.write(str(e)+' : module needed to run this example...\n')
    sys.exit(1)

# lock and file handle for timing results csv
CSV_LOCK, CSV = None, None
CSV_HEADING = ['routine', 'iteration', 'seconds', 'micro_seconds']

# Number of random time series for multiple_time_series
MULTI_SERIES_N = 16

LOGLEVEL = logging.INFO   
#LOGLEVEL = logging.DEBUG

def timing_tally_init(urif, vname):
    '''Routine sets up a timing output csv stream. The lock is used
       because we often have several tests running simultaneously.'''
    global CSV_LOCK, CSV, CSV_HEADING
    CSV_LOCK = threading.Lock()
    nobuf = os.fdopen(sys.stdout.fileno(), 'w', 0)
    CSV = csv.writer(nobuf)
    CSV.writerow([urif, vname])
    CSV.writerow(CSV_HEADING) 

def tally_write(row):
    global CSV_LOCK, CSV 
    CSV_LOCK.acquire()
    CSV.writerow(row)
    CSV_LOCK.release()

def nums_2_date(tarr, unitstxt, cal):
    '''Routine converts date numbers into datetimes. For display...'''
    jd0, units, tzoffset, origin = datencutil.get_origin_num(unitstxt, cal)
    return datencutil.num2date(tarr, jd0, units, cal, tzoffset)

def random_box(nx, ny, ps=(20,30)):
    '''Routine create a box of random size % nx and % ny (size range is 
       ps[0] to ps[1]) at a random start, within 0,0 to nx-1, ny-1 ''' 
    s = random.uniform(ps[0]/100.0, ps[1]/100.0)
    xsz, ysz = int(nx*s), int(ny*s)
    x = random.randint(0, nx-xsz-1)
    y = random.randint(0, ny-ysz-1)
    return [x, y, x+xsz, y+ysz]

def random_cube(nx, ny, nz, ps=(20,30)):
    '''Routine create a cube of random size % nx, % ny and % nz 
       (size range is ps[0] to ps[1]) at a random start, within 0,0,0 
       to nx-1, ny-1, nz-1 ''' 
    s = random.uniform(ps[0]/100.0, ps[1]/100.0)
    xsz, ysz, zsz = int(nx*s), int(ny*s), int(nz*s)
    x = random.randint(0, nx-xsz-1)
    y = random.randint(0, ny-ysz-1)
    z = random.randint(0, nz-zsz-1)
    return [x, y, z, x+xsz, y+ysz, z+zsz]

def random_loc(nx, ny, n=1):
    '''Routine builds a list of 1 or more random x, y locations within 0,0 to nx-1, ny-1''' 
    llst = []
    for i in range(0, n):
        x = random.randint(0, nx-1)
        y = random.randint(0, ny-1)
        llst.append((x, y))
    return llst

def prime_h5_source(fname, vname, tms=None):
    '''Simple routine to open and check for a var and return time step datetimes, if 
        requested, and a fill value'''
    hfd = h5py.File(fname, 'r')
    if vname not in hfd.keys():
        logging.error('ERROR : %s not available in %s' % (fname, vname) )
        sys.exit(1)
    fillv = hfd[vname].attrs['_FillValue'] # WARN, this is assumed...
    if tms != None:
        # assume time and the required units exists for this example, then convert to "readable" datetimes
        dtms = nums_2_date( hfd['time'], hfd['time'].attrs['units'], hfd['time'].attrs['calendar'] )
        return hfd, fillv, dtms
    else:
        return hfd, fillv
# prime_h5_source

def prime_h5rest_source(urinm, vname, tms=None):
    '''Simple routine to open and check for a var and return time step datetimes, if 
        requested, and a fill value from h5sevr or hsds rest service'''
    hfd = h5pyd.File(urinm[1], "r", endpoint=urinm[0])
    if vname not in hfd.keys():
        logging.error('ERROR : %s not available in %s' % (urinm[2]+'/'+urinm[1], vname) )
        sys.exit(1)

    fillv = hfd[vname].attrs['_FillValue'] # WARN, this is assumed...
    if tms != None:
        # assume time and the required units exists for this example, then convert to "readable" datetimes
        dtms = nums_2_date( hfd['time'][:], hfd['time'].attrs['units'], hfd['time'].attrs['calendar'] )
        return hfd, fillv, dtms
    else:
        return hfd, fillv
# prime_h5rest_source

def spatial_subset(fname, vname, its, srctype):
    '''Spatial Subset - Getting one spatial subset out of a set of files (time dimension is fixed)'''
    logging.info('spatial_subset called with (%s, %s), iterations %d' % (fname, vname, its) )

    if srctype == 'rest':
        hfd, fillv, dtm = prime_h5rest_source(fname, vname, 1)
    else:
        hfd, fillv, dtm = prime_h5_source(fname, vname, 1)

    tmrnge = len(dtm)

    for i in range(0, its):
        tmstr = datetime.datetime.now() 
            
        t = random.randint(0, tmrnge-1)
        # dimension layout assumed for simplicity here... (t,y,x)
        if srctype == 'rest':
            box = random_box( hfd['lon'].shape[0], hfd['lat'].shape[0] )
            dat = hfd[vname][t, box[1]:box[3], box[0]:box[2]]
        else:
            box = random_box( hfd['lon'].shape[0], hfd['lat'].shape[0] )
            dat = hfd[vname][t, box[1]:box[3], box[0]:box[2]]

        dat = numpy.ma.masked_where(dat==fillv, dat)
        m, v = dat.mean(), dat.var()
        logging.debug('spatial_subset called with (%s)[%s] , iteration %d, mean %s, variance %s' % \
                        (str(box),  dtm[t].strftime("%Y.%m.%d"), i, str(m), str(v)) )
        tmtot = datetime.datetime.now() - tmstr 
        tally_write(['spatial_subset', i+1, tmtot.seconds, tmtot.microseconds])
    
    hfd.close()
#spatial_subset

def single_time_series(fname, vname, its, srctype):
    '''Single time-series - Get a single point timeseries (x and y dimensions are fixed)'''
    logging.info('single_time_series called with (%s, %s), iterations %d' % (fname, vname, its) )
    if srctype == 'rest':
        hfd, fillv, dtm = prime_h5rest_source(fname, vname, 1)
    else:
        hfd, fillv, dtm = prime_h5_source(fname, vname, 1)

    tmrnge = len(dtm)

    for i in range(0, its):
        tmstr = datetime.datetime.now() 
        xy = random_loc(hfd['lon'].shape[0], hfd['lat'].shape[0])[0]
        dat = hfd[vname][ 0:tmrnge-1, xy[1], xy[0] ]
        dat = numpy.ma.masked_where(dat==fillv, dat)
        m, v = dat.mean(), dat.var()
        logging.debug('single_time_series called with (%s) %s to %s, iteration %d, mean %s, variance %s' % \
                        (str(xy), dtm[0].strftime("%Y.%m.%d"), dtm[tmrnge-1].strftime("%Y.%m.%d"), i, str(m), str(v)) )
        tmtot = datetime.datetime.now() - tmstr 
        tally_write(['single_time_series', i+1, tmtot.seconds, tmtot.microseconds])
    
    hfd.close()
#single_time_series

def multiple_time_series(fname, vname, its, srctype):
    '''Multiple time-series - Get multiple timeseries (x and y are fixed, but there is a set of them)'''
    global MULTI_SERIES_N 
    logging.info('multiple_time_series called with (%s, %s), iterations %d' % (fname, vname, its) )
    if srctype == 'rest':
        hfd, fillv, dtm = prime_h5rest_source(fname, vname, 1)
    else:
        hfd, fillv, dtm = prime_h5_source(fname, vname, 1)

    tmrnge = len(dtm)

    for i in range(0, its):
        tmstr = datetime.datetime.now() 
        xyall = random_loc(hfd['lon'].shape[0], hfd['lat'].shape[0], MULTI_SERIES_N)

        for xy in xyall: 
            if srctype == 'rest':
                dat = hfd[vname][ 0:tmrnge-1, xy[1], xy[0] ]
            else:
                dat = hfd[vname][ 0:tmrnge-1, xy[1], xy[0] ]
            dat = numpy.ma.masked_where(dat==fillv, dat)
            m, v = dat.mean(), dat.var()
            logging.debug("multiple_time_series : %s, mean %s, variance %s " % (str(xy), str(m), str(v)) )

        logging.debug('multiple_time_series called with %d locations over times %s to %s, iteration %d' % \
                         (MULTI_SERIES_N, dtm[0].strftime("%Y.%m.%d"), dtm[tmrnge-1].strftime("%Y.%m.%d"), i) ) 
        tmtot = datetime.datetime.now() - tmstr 
        tally_write(['multiple_time_series', i+1, tmtot.seconds, tmtot.microseconds])

    hfd.close()
#multiple_time_series

def space_time_data_cubes(fname, vname, its, srctype):
    '''Space/time data cubes - Get a timeseries of spatial subsets (one or more subsets through time).'''
    logging.info('space_time_data_cubes called with (%s, %s), iterations %d' % (fname, vname, its) )
    if srctype == 'rest':
        hfd, fillv, dtm = prime_h5rest_source(fname, vname, 1)
    else:
        hfd, fillv, dtm = prime_h5_source(fname, vname, 1)

    tmrnge = len(dtm)

    for i in range(0, its):
        tmstr = datetime.datetime.now() 
        # dimension layout assumed for simplicity here... (t,y,x)
        cube = random_cube( hfd['lon'].shape[0], hfd['lat'].shape[0], tmrnge)
        dat = hfd[vname][cube[2]:cube[5], cube[1]:cube[4], cube[0]:cube[3]]

        dat = numpy.ma.masked_where(dat==fillv, dat)
        m, v = dat.mean(), dat.var()
        logging.debug('space_time_data_cubes called %s, over times %s to %s, iteration %d, cube mean %s, cube variance %s' % \
                         (str(cube), dtm[cube[2]].strftime("%Y.%m.%d"), dtm[cube[5]].strftime("%Y.%m.%d"), i, str(m), str(v)) ) 
        tmtot = datetime.datetime.now() - tmstr 
        tally_write(['space_time_data_cubes', i+1, tmtot.seconds, tmtot.microseconds])
    
    hfd.close()
#space_time_data_cubes

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', level=LOGLEVEL)
    tst_routines = [spatial_subset, single_time_series, multiple_time_series, space_time_data_cubes]
    nroutines = len(tst_routines)
    thrds, nthrds, nits, srctype = [], nroutines, 1, 'file'

    if len(sys.argv) < 3:
        logging.error('probeclimh5.py <clim-file.h5 | h5-endoint> <var-name> [ <nthreads> ] [iters] ') 
        sys.exit(1)
    else:
        fname = sys.argv[1]
        vname = sys.argv[2]
        if len(sys.argv) > 3: nthrds = int(sys.argv[3])
        if len(sys.argv) > 4: nits = int(sys.argv[4])

    logging.debug("init stdout for results ...")
    timing_tally_init(fname, vname)

    if nthrds < nroutines: 
        nthrds = nroutines
        logging.warn("WARN : number of threads is < number of test routines. N threads set to %d" % nthrds)
    if nthrds%nroutines != 0: 
        nthrds = nthrds + (nthrds%nroutines)
        logging.warn("WARN : number of threads is not evenly divisible by the number of test routines. N threads set to %d for probe fairness" % nthrds)
    
    logging.debug("starting with number of threads %d, number of test routines %d, number of iterations %d" % (nthrds, nroutines, nits))

    uri = urlparse.urlparse(fname)
    if re.search('http[s]*', uri.scheme): 
        srctype = 'rest'
        fname = (uri.scheme+'://'+uri.netloc, uri.path[1:])

    tstart = datetime.datetime.now()

    for i in range(nthrds):
        func = tst_routines[i%nroutines]
        t = threading.Thread(target=func, args=(fname, vname, nits, srctype))
        t.daemon = False
        t.start()
        thrds.append(t)

    for t in thrds: 
        t.join()

    tmtot = datetime.datetime.now() - tstart
    r = requests.get('http://169.254.169.254/latest/meta-data/instance-type')
    if r.status_code == 200:
        ityp = r.text
    else:
        ityp = 'NA'
    tally_write(['exec ~time : %s : insttype %s' % (tmtot, ityp)])
#__main__

