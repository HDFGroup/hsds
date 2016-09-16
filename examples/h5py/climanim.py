#!/usr/bin/env python

# Example script that creates an animation of several time steps from
# NEX clim data; the data is usually parked on aws S3. Note: the complete 
# set of the time steps often spans multiple files, of which we want to 
# get rid of (the multiple files) if possible. The set might be from
# a single model's historical (1950 to 2005) plus the model's rcp85
# for one variable (tasmax, tasmin, ...)
#
# This example intentionally executes in a serial manner. Though the
# example may seem contrived, the data access pattern below is common 
# for the workstation bound scientist...

import sys, os
import tempfile
import logging
import contextlib 
import subprocess
import datetime
import glob
import shutil
try:
    import numpy
    import requests
    import h5py
    import matplotlib.pyplot as plt
    import datencutil
except ImportError, e:
    sys.stderr.write(str(e)+' : module needed to run this example...\n')
    sys.exit(1)

def plot_data_frame(data, data_min, data_max, frametxt, flstr, tmpdir, labunit=u'\N{DEGREE SIGN}C'):
    """
    Creates a simple plot, writes it to a temp file ...
    """
    plt.rcParams['figure.figsize'] = (12, 8)
    plt.imshow(data, vmin=data_min, vmax=data_max, origin='lower', \
                cmap=plt.cm.get_cmap('jet',18))
    plt.colorbar(drawedges=True,ticks=numpy.linspace(data_min,data_max,10), \
                  orientation='horizontal',extend='both',pad=0.05, \
                  shrink=0.8).set_label(labunit)
    plt.title(frametxt)
    fname = os.path.join(tmpdir, 'f.'+"%s" % flstr +'.png')
    plt.savefig(fname)
    plt.close()
#plot_data_frame
        
def time_step_temp_img(h5fd, vname, tmidx, datet, tmpdir, sub=None):
    """ 
    Extracts a time step from the h5 file handle, cleans up the 
    data and sends it to the basic plotting routine...
    Routine assumes dimension layout t,y,x for this basic example 
    """
    # TODO: add spatial subset with param sub
    step = h5fd[vname][tmidx,:,:]
    if vname == 'tasmax' or vname == 'tasmin': # unit conversion kluge
        fillv = h5fd[vname].attrs['_FillValue']
        dat = numpy.ma.masked_where(step==fillv, step - 273.15) 
        data_min, data_max = -10.0, 40.0
    else:
        dat = numpy.ma.masked_where(step==fillv, step) 
        data_min, data_max = dat.min(), dat.max() 
    plot_data_frame(dat, data_min, data_max, vname+' - '+datet.strftime("%Y %b %e"),  datet.strftime("%Y%m%d"), tmpdir)
#time_step_tempimg

def nums_2_date(tarr, unitstxt, cal):
    jd0, units, tzoffset, origin = datencutil.get_origin_num(unitstxt, cal)
    return datencutil.num2date(tarr, jd0, units, cal, tzoffset)
#nums_2_date

def h5_file_2_imgs(fname, vname, tmpdir):
    """ 
    Routine sets up an h5 file for time step extraction
    """
    hf = h5py.File(fname,'r')
    if vname not in hf.keys():
        logging.error('ERROR : '+str(vname)+' not available in '+fname)
        return
    # assume time and the required units exists for this example, then convert to "readable" datetimes
    dtms = nums_2_date( hf['time'], hf['time'].attrs['units'], hf['time'].attrs['calendar'] )

    # Loop over all time steps for this file
    for t, d in enumerate(dtms):
        logging.info("working on [%03d] %s" % (t, str(d)))
        time_step_temp_img(hf, vname, t, d, tmpdir)
    hf.close()
#h5_file_2_imgs

def make_vid(tempdir):
    fls = glob.glob(os.path.join(tempdir, '*.png'))
    if fls and len(fls) == 0: 
        logging.warn("no png\'s found")
    pngs = os.path.normpath(os.path.join(tempdir, '*.png'))
    fo = os.path.normpath(os.path.join(tempdir, '..', 'mov.mp4'))
    if os.path.exists(fo):
        os.unlink(fo)
    cmd = [ 'ffmpeg', '-loglevel', 'panic', '-r', '3', '-pattern_type', 'glob', '-i', '\''+pngs+'\'', '-c', 'libx264', \
            '-vframes', str(len(fls)), '-preset', 'slower', '-pix_fmt', 'yuv420p', fo]
    cmd = " ".join(cmd)
    logging.info(cmd)
    try:
        subprocess.call(cmd, shell=True) 
    except Exception, e:
        logging.error(str(e))
#make_vid

def proc_files(flist, vname):
    '''
    Routine stages in one file at a time to the local system for 
    processing. A file is removed once the processing for that file 
    has completed (no caching for later use...) Note: we want to 
    essentially remove of this routine and just call hsds directly..
    '''
    mb4 = (2**30)*4 # buffer size
    try:
        tdir = tempfile.mkdtemp(prefix='h52img-')
        for f in flist:
            logging.info('processing '+f)
            with contextlib.closing( requests.get(f, stream=True)) as rfin:
                fout = tempfile.NamedTemporaryFile(suffix='.h5', dir=tdir, delete=False)
                for chk in rfin.iter_content(chunk_size=mb4):
                    fout.write(chk)
                fout.close()
                rfin.close()
                logging.info(fout.name)
                h5_file_2_imgs(fout.name, vname, tdir) 
                os.unlink(fout.name)
        make_vid(tdir)
        shutil.rmtree(tdir)
    except (OSError, KeyboardInterrupt), e:
        logging.warn(str(e))
#h5_files_2_imgs

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    if len(sys.argv) != 1:
        logging.error('climanim.py - pipe in a list of files to make an ' + \
                      'animation, e.g. cat flist.txt | ./climanim.py '+\
                      'Files in the list should be an appropriate set, '+\
                      'historical+rcp85 of some variable, etc...')
        sys.exit(1)

    tstart = datetime.datetime.now()

    h5files = [ r.strip() for r in sys.stdin ]
    proc_files(h5files, 'tasmax')

    tmex = datetime.datetime.now() - tstart
    r = requests.get('http://169.254.169.254/latest/meta-data/instance-type')
    if r.status_code == 200:
        ityp = r.text
    else:
        ityp = 'NA'
    logging.info('example exec ~time : %s : insttype \"%s\"' % (tmex, ityp))
#__main__
# Expected output using exampleurls.txt as input can be found here https://ecocast.arc.nasa.gov/data/drop/d2f67eeaf202b4ccd3612efbd32feeff/mov.mp4  (2016)
