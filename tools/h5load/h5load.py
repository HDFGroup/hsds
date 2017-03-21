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
import logging
import os.path as op

try:
    import h5py 
    import h5pyd 
    from chunkiter import ChunkIterator
except ImportError as e:
    sys.stderr.write("ERROR : %s : install it to use this utility...\n" % str(e)) 
    sys.exit(1)

if sys.version_info >= (3, 0):
    import configparser
else:
    import ConfigParser as configparser
    
__version__ = '0.0.1'

UTILNAME = 'hsload'
 
#----------------------------------------------------------------------------------
def copy_attribute(obj, name, attrobj):
    logging.debug("creating attribute %s in %s" % (name, obj.name))
    obj.attrs.create(name, attrobj)
# copy_attribute
      
#----------------------------------------------------------------------------------
def create_dataset(fd, dobj):
    logging.info("createdataet:", dobj.name, type(dobj.name))
    try:
        logging.info("setting %s chunk size to %s, data shape %s" % (dobj.name, str(dobj.chunks), str(dobj.shape)))
        fillvalue = None
        try:    
            # can trigger a runtime error if fillvalue is undefined
            fillvalue = dobj.fillvalue
        except RuntimeError:
            pass  # ignore

        dset = fd.create_dataset( dobj.name, shape=dobj.shape, dtype=dobj.dtype, chunks=dobj.chunks, \
                               compression=dobj.compression, shuffle=dobj.shuffle, \
                               fletcher32=dobj.fletcher32, maxshape=dobj.maxshape, \
                               compression_opts=dobj.compression_opts, fillvalue=fillvalue, \
                               scaleoffset=dobj.scaleoffset)

        for da in dobj.attrs:
            logging.info("createdataet/attribute:", da, type(da), dobj.attrs[da])
            copy_attribute(dset, da, dobj.attrs[da])

        it = ChunkIterator(dset)

        for s in it:
            logging.info("writing dataset data for slice: {}".format(s))
            arr = dobj[s]
            dset[s] = arr
    except Exception as e:
        logging.error("ERROR : failed to creating dataset in create_dataset : "+str(e))
     
# create_dataset

#----------------------------------------------------------------------------------
def create_group(fd, gobj):
    logging.debug("creating group %s" % (gobj.name))
    grp = fd.create_group(gobj.name)
    for ga in gobj.attrs:
        copy_attribute(grp, ga, gobj.attrs[ga])
# create_group

#----------------------------------------------------------------------------------
def create_datatype(fd, obj):
    logging.debug("creating datatype %s" % (obj.name))
    fd[obj.name] = obj.dtype
    ctype = fd[obj.name]
    for ga in obj.attrs:
        copy_attribute(ctype, ga, obj.attrs[ga])
# create_datatype
      
#----------------------------------------------------------------------------------
def load_file(filename, domain, endpoint=None, username=None, password=None):
    try:
        logging.info("input file: {}".format(filename))   
        finfd = h5py.File(filename, "r")
        logging.info("output domain: {}".format(domain))
        foutfd = h5pyd.File(domain, "w", endpoint=endpoint, username=username, password=password)

        def object_create_helper(name, obj):
            if isinstance(obj, h5py.Dataset):
                create_dataset(foutfd, obj)
            elif isinstance(obj, h5py.Group):
                create_group(foutfd, obj)
            elif isinstance(obj, h5py.Datatype):
                create_datatype(foutfd, obj)
            else:
                logging.error("no handler for object class: {}".format(type(obj)))

        # build a rough map of the file using the internal function above
        finfd.visititems(object_create_helper)
        
        # Fully flush the h5pyd handle. The core of the source hdf5 file 
        # has been created on the hsds service up to now.
        foutfd.close() 
      
        # close up the source file, see reason(s) for this below
        finfd.close() 

        return 0
    except IOError as e: 
        logging.error(str(e))
        return 1
# hsds_basic_load

#----------------------------------------------------------------------------------
def usage():
    print("Usage:\n")
    print(("    %s [ OPTIONS ]  SOURCE  DOMAIN" % UTILNAME))
    print(("    %s [ OPTIONS ]  SOURCE  FOLDER" % UTILNAME))
    print("")
    print("Description:")
    print("    Copy HDF5 file to Domain or multiple files to a Domain folder")
    print("       SOURCE: HDF5 file or multiple files if copying to folder ")
    print("       DOMAIN: HDF Server domain (Unix or DNS style)")
    print("       FOLDER: HDF Server folder (Unix style ending in '/')")
    print("")
    print("Options:")
    print("     -e | --endpoint <domain> :: The HDF Server endpoint, e.g. http://example.com:8080")
    print("     -u | --user <username>   :: User name credential")
    print("     -p | --password <password> :: Password credential")
    print("     -c | --conf <file.cnf>  :: A credential and config file")
    print("     --cnf-eg        :: Print a config file and then exit")
    print("     --logfile <logfile> :: logfile path")
    print("     --loglevel debug|info|warning|error :: Change log level")
    print("     -h | --help    :: This message.")
    print("")
    print(("%s version %s\n" % (UTILNAME, __version__)))
#end print_usage


#----------------------------------------------------------------------------------
def print_config_example():
    print("[default]")
    print("hs_username = <username>")
    print("hs_password = <passwd>")
    print("hs_endpoint = https://hdfgroup.org:7258")
#print_config_example

#----------------------------------------------------------------------------------
if __name__ == "__main__":
    loglevel = logging.DEBUG
    cnfgfname, logfname = [None]*2
    username, password, endpoint = [None]*3
    credCnfDefaultSec = 'default'
    src_files = []
    argn = 1

    while argn < len(sys.argv):
        arg = sys.argv[argn]
        val = None
         
        if arg[0] == '-' and len(src_files) > 0:
            # options must be placed before filenames
            usage()
            sys.exit(-1)
        if len(sys.argv) > argn + 1:
            val = sys.argv[argn+1] 
        if arg == "--loglevel":
            if val == "debug":
                loglevel = logging.DEBUG
            elif val == "info":
                loglevel = logging.INFO
            elif val == "warning":
                loglevel = logging.WARNING
            elif val == "error":
                loglevel = logging.ERROR
            else:
                printUsage()  
                sys.exit(-1)
            argn += 2
        elif arg == '--logfile':
            logfname = val
            argn += 2     
        elif arg in ("-h", "--help"):
            usage()
            sys.exit(0)
        elif arg in ("-e", "--endpoint"):
            endpoint = val
            argn += 2
        elif arg in ("-u", "--username"):
            username = val
            argn += 2
        elif arg in ("-p", "--password"):
            password = val
            argn += 2
        elif arg in ("-c", "--config"):
            cnfgfname = val
            argn += 2
        elif arg == '--cnf-eg':
            print_config_example()
            sys.exit(0)
        elif arg[0] == '-':
             usage()
             sys.exit(-1)
        else:
            src_files.append(arg)
            argn += 1
    # end arg parsing

    if cnfgfname:
        try:
            config = configparser.SafeConfigParser()
            config.read(cnfgfname)
            if username == None and config.get(credCnfDefaultSec, 'hs_username'):
               username = config.get(credCnfDefaultSec, 'hs_username')
            if password == None and config.get(credCnfDefaultSec, 'hs_password'):
               password = config.get(credCnfDefaultSec, 'hs_password')
            if endpoint == None and config.get(credCnfDefaultSec, 'hs_endpoint'):
               endpoint = config.get(credCnfDefaultSec, 'hs_endpoint')
        except configparser.NoSectionError as e:
            sys.stderr.write('ConfigParser error : '+str(e)+'\n')
            sys.exit(1)

    if len(src_files) < 2:
        # need at least a src and destination
        usage()
        sys.exit(-1)

    domain = src_files[-1]
    src_files = src_files[:-1]
    
    logging.basicConfig(filename=logfname, format='%(asctime)s %(message)s', level=loglevel)

    logging.info("username:", username)
    logging.debug("password:", password)
    logging.info("endpoint:", endpoint)
    logging.info("source files: {}".format(src_files))
    logging.info("target domain: {}".format(domain))
    if len(src_files) > 1 and (domain[0] != '/' or domain[-1] != '/'):
        usage()
        sys.exit(-1)
   
    if endpoint is None:
        logging.error('No endpoint given, try -h for help\n')
        sys.exit(1)
    logging.info("endpoint: {}".format(endpoint))

    try:
        for src_file in src_files:
            tgt = domain
            if tgt[-1] == '/':
                # folder destination
                tgt = tgt + op.basename
            r = load_file(src_file, tgt, endpoint=endpoint, username=username, password=password)
    except KeyboardInterrupt:
        logging.error('Aborted by user via keyboard interrupt.')
        sys.exit(1)
#__main__

