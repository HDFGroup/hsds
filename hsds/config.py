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
import os
import sys
import yaml

cfg = {}
   
def _load_cfg():
        
    # load config yaml
    yml_file = None
    for config_dir in [".", "/config"]:
        file_name = os.path.join(config_dir, "config.yml")
        if os.path.isfile(file_name):
            yml_file = file_name
            break
    if not yml_file:
        msg = "unable to find config file"
        print(msg)
        raise FileNotFoundError(msg)
    print(f"_load_cfg with '{yml_file}''")
    try:
        with open(yml_file, "r") as f:
            yml_config = yaml.safe_load(f)
    except yaml.scanner.ScannerError as se:
        msg = f"Error parsing config.yml: {se}"
        print(msg)
        raise KeyError(msg)

    # load override yaml
    yml_override = None
    if "CONFIG_OVERRIDE_PATH" in os.environ:
        override_yml_filepath = os.environ["CONFIG_OVERRIDE_PATH"]
    else:
        override_yml_filepath = "/config/override.yml"
    if os.path.isfile(override_yml_filepath):
        print(f"loading override configuation: {override_yml_filepath}")
        try:
            with open(override_yml_filepath, "r") as f:
                yml_override = yaml.safe_load(f)
        except yaml.scanner.ScannerError as se:
            msg = f"Error parsing '{override_yml_filepath}': {se}"
            print(msg)
            raise KeyError(msg)
        print("override settings:")
        for k in yml_override:
            v = yml_override[k]
            print(f"  {k}: {v}")
        

    # apply overrides for each key and store in cfg global
    for x in yml_config:
        cfgval = yml_config[x]
        # see if there is a command-line override
        option = '--'+x+'='
        override = None
        for i in range(1, len(sys.argv)):
            #print(i, sys.argv[i])
            if sys.argv[i].startswith(option):
                # found an override
                arg = sys.argv[i]
                override = arg[len(option):]  # return text after option string   
                print(f"got cmd line override for {x}")
                 
            
        # see if there are an environment variable override
        if override is None and x.upper() in os.environ:
            override = os.environ[x.upper()]
            print(f"got env value override for {x} ")

        # see if there is a yml override
        if override is None and yml_override and x in yml_override:
            override = yml_override[x]
            print(f"got config override for {x}")


        if override:
            if cfgval is not None:
                try:
                    override = type(cfgval)(override) # convert to same type as yaml
                except ValueError as ve:
                    msg = f"Error applying command line override value for key: {x}: {ve}"
                    print(msg)
                    # raise KeyError(msg)
            cfgval = override # replace the yml value

        if isinstance(cfgval, str) and len(cfgval) > 1 and cfgval[-1] in ('g', 'm', 'k') and cfgval[:-1].isdigit():
            # convert values like 512m to corresponding integer
            u = cfgval[-1]
            n = int(cfgval[:-1])
            if u == 'k':
                cfgval =  n * 1024
            elif u == 'm':
                cfgval = n * 1024*1024
            else: # u == 'g'
                cfgval = n * 1024*1024*1024
        cfg[x] = cfgval

def get(x):
    if not cfg:
        _load_cfg()
    if x not in cfg:
        raise KeyError(f"config value {x} not found")
    return cfg[x]
