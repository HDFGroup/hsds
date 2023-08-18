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
import argparse
import os
import json
import sys
import logging
import time

from .hsds_app import HsdsApp
from . import config

_HELP_USAGE = "Starts HSDS, a REST-based service for HDF5 data."

_HELP_EPILOG = """Examples:

- with a POSIX-based storage using a directory: ./hsdata for storage:

  hsds --root_dir ~/hsdata

- with POSIX-based storage and config settings and password file:

  hsds --root_dir ~/hsdata --password-file ./admin/config/passwd.txt \
  --config_dir ./admin/config

- with minio data storage:

  hsds --s3-gateway http://localhost:6007 --access-key-id demo:demo
      --secret-access-key DEMO_PASS --password-file ./admin/config/passwd.txt

- with AWS S3 storage and a bucket in the us-west-2 region:

  hsds --s3-gateway http://s3.us-west-2.amazonaws.com --access-key-id ${AWS_ACCESS_KEY_ID} \
      --secret-access-key ${AWS_SECRET_ACCESS_KEY} --password-file ./admin/config/passwd.txt

"""

# maximum number of characters if socket directory is given
# Exceeding this can cause errors - see: https://github.com/HDFGroup/hsds/issues/129
# Underlying issue is reported here: https://bugs.python.org/issue32958
MAX_SOCKET_DIR_PATH_LEN = 63


class UserConfig:
    """
    User Config state
    """

    def __init__(self, config_file=None, **kwargs):
        self._cfg = {}
        if config_file:
            self._config_file = config_file
        elif os.path.isfile(".hscfg"):
            self._config_file = ".hscfg"
        else:
            self._config_file = os.path.expanduser("~/.hscfg")
        # process config file if found
        if os.path.isfile(self._config_file):
            line_number = 0
            with open(self._config_file) as f:
                for line in f:
                    line_number += 1
                    s = line.strip()
                    if not s:
                        continue
                    if s[0] == "#":
                        # comment line
                        continue
                    index = line.find("=")
                    if index <= 0:
                        print(
                            "config file: {} line: {} is not valid".format(
                                self._config_file, line_number
                            )
                        )
                        continue
                    k = line[:index].strip()
                    nlen = index + 1
                    v = line[nlen:].strip()
                    if v and v.upper() != "NONE":
                        self._cfg[k] = v
        # override any config values with environment variable if found
        for k in self._cfg.keys():
            if k.upper() in os.environ:
                self._cfg[k] = os.environ[k.upper()]

        # finally update any values that are passed in to the constructor
        for k in kwargs.keys():
            self._cfg[k.upper()] = kwargs[k]

    def __getitem__(self, name):
        """Get a config item"""

        # Load a variable from environment. It would have only been loaded in
        # __init__ if it was also specified in the config file.
        env_name = name.upper()
        if name not in self._cfg and env_name in os.environ:
            self._cfg[name] = os.environ[env_name]

        return self._cfg[name]

    def __setitem__(self, name, obj):
        """set config item"""
        self._cfg[name] = obj

    def __delitem__(self, name):
        """Delete option."""
        del self._cfg[name]

    def __len__(self):
        return len(self._cfg)

    def __iter__(self):
        """Iterate over config names"""
        keys = self._cfg.keys()
        for key in keys:
            yield key

    def __contains__(self, name):
        return name in self._cfg or name.upper() in os.environ

    def __repr__(self):
        return json.dumps(self._cfg)

    def keys(self):
        return self._cfg.keys()

    def get(self, name, default=None):
        if name in self:
            return self[name]
        else:
            return default


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        usage=_HELP_USAGE,
        epilog=_HELP_EPILOG,
    )

    parser.add_argument(
        "--root_dir",
        type=str,
        dest="root_dir",
        help="Directory where to store the object store data",
    )
    parser.add_argument(
        "--bucket_name",
        nargs=1,
        type=str,
        dest="bucket_name",
        help='Name of the bucket to use (e.g., "hsds.test").',
    )
    parser.add_argument(
        "--host", default="", type=str, dest="host", help="host name for url"
    )
    parser.add_argument(
        "--hs_username",
        type=str,
        dest="hs_username",
        help="username to be added to list of valid users",
        default="",
    )
    parser.add_argument(
        "--hs_password",
        type=str,
        dest="hs_password",
        help="password for hs_username",
        default="",
    )
    parser.add_argument(
        "--password_file",
        type=str,
        dest="password_file",
        help="location of hsds password file",
        default="",
    )

    parser.add_argument(
        "--logfile",
        default="",
        type=str,
        dest="logfile",
        help="filename for logout (default stdout).",
    )
    parser.add_argument(
        "--loglevel",
        default="",
        type=str,
        dest="loglevel",
        help="log verbosity: DEBUG, WARNING, INFO, OR ERROR",
    )
    parser.add_argument(
        "-p", "--port", default=0, type=int, dest="port", help="Service node port"
    )
    parser.add_argument(
        "--count",
        default=4,
        type=int,
        dest="dn_count",
        help="Number of dn sub-processes to create.",
    )
    parser.add_argument(
        "--socket_dir",
        default="",
        type=str,
        dest="socket_dir",
        help="directory for socket endpoint",
    )
    parser.add_argument(
        "--config_dir",
        default="",
        type=str,
        dest="config_dir",
        help="directory for config data",
    )

    args = parser.parse_args()

    kwargs = {}  # options to pass to hsdsapp

    # setup logging
    if args.loglevel:
        log_level_cfg = args.loglevel
        kwargs["log_level"] = args.loglevel
    elif "LOG_LEVEL" in os.environ:
        log_level_cfg = os.environ["LOG_LEVEL"]
    else:
        log_level_cfg = "INFO"
    if log_level_cfg == "DEBUG":
        log_level = logging.DEBUG
    elif log_level_cfg == "INFO":
        log_level = logging.INFO
    elif log_level_cfg in ("WARN", "WARNING"):
        log_level = logging.WARN
    elif log_level_cfg == "ERROR":
        log_level = logging.ERROR
    else:
        print(f"unsupported log_level: {log_level_cfg}, using INFO instead")
        log_level = logging.INFO

    print("set logging to:", log_level)
    logging.basicConfig(level=log_level)

    userConfig = UserConfig()

    # set username based on command line, .hscfg, $USER, or $JUPYTERHUB_USER
    if args.hs_username:
        username = args.hs_username
    elif "HS_USERNAME" in userConfig:
        username = userConfig["HS_USERNAME"]
    elif not args.password_file:
        # no password file, add the login name as user
        username = os.getlogin()
    else:
        username = None

    # get password based on command line or .hscfg
    if args.hs_password:
        password = args.hs_password
    elif "HS_PASSWORD" in userConfig:
        password = userConfig["HS_PASSWORD"]
    else:
        password = os.getlogin()

    if username:
        kwargs["username"] = username
        kwargs["password"] = password

    if args.password_file:
        if not os.path.isfile(args.password_file):
            sys.exit(f"password file: {args.password_file} not found")
        kwargs["password_file"] = args.password_file

    # use unix domain socket if a socket dir is set
    if args.socket_dir:
        socket_dir = os.path.abspath(args.socket_dir)
        if not os.path.isdir(socket_dir):
            raise FileNotFoundError(f"directory: {socket_dir} not found")
        kwargs["socket_dir"] = socket_dir
    else:
        # USE TCP connect
        if args.host:
            kwargs["host"] = args.host
        else:
            kwargs["host"] = "localhost"
        # sn_port only relevant for TCP connections
        if args.port:
            kwargs["sn_port"] = args.port
        else:
            kwargs["sn_port"] = 5101  # TBD - use config

    if args.logfile:
        logfile = os.path.abspath(args.logfile)
    elif args.host:
        logfile = os.path.abspath("hs.log")
    else:
        socket_dir = os.path.abspath(args.socket_dir)
        logfile = os.path.join(socket_dir, "hs.log")
    print("logfile:", logfile)
    kwargs["logfile"] = logfile

    if args.root_dir:
        kwargs["root_dir"] = args.root_dir

    config_dir = None
    if args.config_dir:
        if not os.path.isdir(args.config_dir):
            print(f"config_dir: {args.config_dir} not found")
        else:
            config_dir = args.config_dir
    if config_dir:
        kwargs["config_dir"] = config_dir

    if args.dn_count:
        kwargs["dn_count"] = args.dn_count

    if args.bucket_name:
        bucket_name = args.bucket_name
    else:
        bucket_name = config.get("bucket_name")
    if not bucket_name:
        sys.exit("bucket_name not set")
    if args.root_dir:
        root_dir = args.root_dir
    else:
        root_dir = config.get("root_dir")
    if not root_dir:
        # check that AWS_S3_GATEWAY or AZURE_CONNECTION_STRING is set
        if not config.get("aws_s3_gateway") and not config.get("azure_connection_string"):
            sys.exit("root_dir not set (and no S3 or Azure connection info)")
    else:
        if not os.path.isdir(root_dir):
            sys.exit(f"directory: {root_dir} not found")
        bucket_path = os.path.join(root_dir, bucket_name)
        if not os.path.isdir(bucket_path):
            os.mkdir(bucket_path)

    app = HsdsApp(**kwargs)
    app.run()

    waiting_on_ready = True

    while True:
        try:
            time.sleep(1)
            app.check_processes()
        except KeyboardInterrupt:
            print("got keyboard interrupt")
            break
        except Exception as e:
            print(f"got exception: {e}")
            break
        if waiting_on_ready and app.ready:
            waiting_on_ready = False
            print("")
            print("READY! use endpoint:", app.endpoint)
            print("")

    print("shutting down server")
    app.stop()
