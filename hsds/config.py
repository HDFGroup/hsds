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

cfg = None  # global config handle
   
def _load_cfg(cfg):
    if cfg is None:
        msg = "cfg global is not set"
        print(msg)
        raise ValueError(msg)

    # load config yaml
    yml_file = "/config/config.yml"
    try:
        with open(yml_file, "r") as f:
            yml_config = yaml.load(f)
    except FileNotFoundError as fnfe:
        msg = f"Unable to config file: {fnfe}"
        print(msg)
        raise
    except yaml.scanner.ScannerError as se:
        msg = f"Error parsing config.yml: {se}"
        print(msg)
        raise KeyError(msg)

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
            
        # see if there are an environment variable override
        if override is None and x.upper() in os.environ:
            override = os.environ[x.upper()]

        if override is not None:
            if cfgval is not None:
                try:
                    override = type(cfgval)(override) # convert to same type as yaml
                except ValueError as ve:
                    msg = f"Error applying command line override value {override} for key: {x}: {ve}"
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
        print(f"config set {x} to {cfgval}, type: {type(cfgval)}")
    cfg["yml_loaded"] = True  # mark yaml as being loaded

def get(x):
    global cfg
    print(f"cfg.get({x}), cfg type: {type(cfg)}")
    if not cfg or "yml_loaded" not in cfg:
        print("loading config values")
        _load_cfg(cfg)
        print(f"cfg - {len(cfg)} keys defined")
    if x not in cfg:
        print(f"key {x} not found in cfg, existing keys:")
        for k in cfg:
            v = cfg[k]
            print(f"cfg[{k}]: {v}")
        print("throwing keyerror")
        raise KeyError(f"config value {x} not found")
    print(f"cfg.get({x}) returning: {cfg[x]}")
    return cfg[x]
