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
import sys
import time
import os
import subprocess
import socket
import queue
import threading
from contextlib import closing
import logging


def find_free_port():
    # note use the --unix-socket <path> option with curl for sockets

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('127.0.0.1', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def print_process_output(queues):
    while True:
        got_output = False
        for q in queues:
            try:
                line = q.get_nowait()  # or q.get(timeout=.1)
            except queue.Empty:
                pass  # no output on this queue yet
            else:
                print(line.decode("utf-8").strip())
                got_output = True
        if not got_output:
            break  # all queues empty for now


def enqueue_output(out, queue):
    for line in iter(out.readline, b''):
        queue.put(line)
    logging.debug("enqueu_output close()")
    out.close()


_HELP_USAGE = "Starts hsds a REST-based service for HDF5 data."

_HELP_EPILOG = """Examples:

- with openio/sds data storage:

  hsds --s3-gateway http://localhost:6007 --access-key-id demo:demo
      --secret-access-key DEMO_PASS --password-file ./admin/config/passwd.txt
      --bucket-name hsds.test

- with a POSIX-based storage for 'hsds.test' sub-folder in the './data'
  folder:

  hsds --bucket-dir ./data/hsds.test
"""


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        usage=_HELP_USAGE,
        epilog=_HELP_EPILOG)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--root_dir', type=str, dest='root_dir',
        help='Directory where to store the object store data')
    group.add_argument(
        '--bucket_name', nargs=1, type=str, dest='bucket_name',
        help='Name of the bucket to use (e.g., "hsds.test").')
    parser.add_argument('--host', default='localhost',
                        type=str, dest='host',
                        help="host address for service node")
    parser.add_argument('--hs_username', type=str,  dest='hs_username',
                        help="username to be added to list of valid users",
                        default='')
    parser.add_argument('--hs_password', type=str,  dest='hs_password',
                        help="password for hs_username", default='')

    parser.add_argument('--logfile', default='',
                        type=str, dest='logfile',
                        help="filename for logout (default stdout).")
    parser.add_argument('--loglevel', default='',
                        type=str, dest='loglevel',
                        help="log verbosity: DEBUG, WARNING, INFO, OR ERROR")
    parser.add_argument('-p', '--port', default=0,
                        type=int, dest='port',
                        help='Service node port')
    parser.add_argument('--count', default=1, type=int,
                        dest='target_dn_count',
                        help='Number of dn sub-processes to create.')

    args, extra_args = parser.parse_known_args()

    # setup logging
    if args.loglevel:
        log_level_cfg = args.loglevel
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

    logging.debug("args:", args)
    logging.debug("extra_args:", extra_args)
    logging.debug("count:", args.target_dn_count)
    logging.debug("port:", args.port)
    logging.debug("hs_username:", args.hs_username)
    # port = find_free_port()
    # logging.debug("port:", port)
    if "--use_socket" in extra_args:
        use_socket = True
        socket_dir = "%2Ftmp%2F"  # TBD - replace with temp dir
    else:
        use_socket = False
        socket_dir = None

    if args.port == 0:
        sn_port = 5101
    else:
        sn_port = args.port

    if use_socket:
        sn_url = f"http+unix://{socket_dir}sn_1.sock"
    else:
        sn_url = "http://localhost:5101"

    dn_urls = []

    for i in range(args.target_dn_count):
        if use_socket:
            dn_url = f"http+unix://{socket_dir}dn_{(i+1)}.sock"
        else:
            dn_port = find_free_port()
            dn_url = f"http://localhost:{dn_port}"
            logging.debug(f"dn_port[{i}]",  dn_port)
        logging.debug(f"dn_url: {dn_url}")
        dn_urls.append(dn_url)
        
    # sort the ports so that node_number can be determined based on dn_url
    dn_urls.sort()
    dn_urls_arg = ""
    for dn_url in dn_urls:
        if dn_urls_arg:
            dn_urls_arg += ','
        dn_urls_arg += dn_url

    logging.debug("dn_urls:", dn_urls)
    if use_socket:
        rangeget_url = f"http+unix://{socket_dir}rangeget.sock"
    else:
        rangeget_port = find_free_port()
        rangeget_url = f"http://localhost:{rangeget_port}"
    logging.debug(f"rangeget_url:  {rangeget_url}")

    common_args = ["--standalone", ]
    if args.loglevel:
        print("setting log_level to:", args.loglevel)
        common_args.append(f"--log_level={args.loglevel}")
    common_args.append(f"--sn_url={sn_url}")
    common_args.append(f"--rangeget_url={rangeget_url}")
    common_args.append("--dn_urls="+dn_urls_arg)
    if use_socket:
        common_args.append("--use_socket")
    common_args.extend(extra_args)  # pass remaining args as config overrides

    if not use_socket:
        logging.debug(f"host: {args.host}")
        public_dns = f"http://{args.host}"
        if sn_port != 80:
            public_dns += ":" + str(sn_port)
        logging.info(f"public_dns: {public_dns}")
        common_args.append(f"--public_dns={public_dns}")

    if args.root_dir is not None:
        logging.debug(f"arg.root_dir: {args.root_dir}")
        root_dir = os.path.expanduser(args.root_dir)
        if not os.path.isdir(root_dir):
            msg = "Error - directory used in --root-dir option doesn't exist"
            logging.error(msg)
            sys.exit(msg)
        common_args.append(f"--root_dir={root_dir}")
        logging.debug("root_dir:", root_dir)
    else:
        bucket_name = args.bucket_name[0]
        common_args.append(f"--bucket_name={bucket_name}")

    # create handle for log file if specified
    if args.logfile:
        pout = open(args.logfile, 'w')
    else:
        pout = subprocess.PIPE   # will pipe to parent

    # Start apps
    logging.debug("Creating subprocesses")
    processes = []
    queues = []

    # create processes for count dn nodes, sn node, and rangeget node
    for i in range(args.target_dn_count+2):
        if i == 0:
            # args for service node
            pargs = ["hsds-servicenode", "--log_prefix=sn "]
            if args.hs_username:
                pargs.append(f"--hs_username={args.hs_username}")
            if args.hs_password:
                pargs.append(f"--hs_password={args.hs_password}")
        elif i == 1:
            # args for rangeget node
            pargs = ["hsds-rangeget", "--log_prefix=rg "]
        else:
            node_number = i - 2  # start with 0
            pargs = ["hsds-datanode", f"--log_prefix=dn{node_number+1} "]
            pargs.append(f"--node_number={node_number}")
        logging.info(f"starting {pargs[0]}")
        pargs.extend(common_args)
        p = subprocess.Popen(pargs, bufsize=0, shell=False, stdout=pout)
        processes.append(p)
        if not args.logfile:
            # setup queue so we can check on process output without blocking
            q = queue.Queue()
            t = threading.Thread(target=enqueue_output, args=(p.stdout, q))
            queues.append(q)
            t.daemon = True  # thread dies with the program
            t.start()

    try:
        while True:
            print_process_output(queues)
            time.sleep(0.1)
            for p in processes:
                if p.poll() is not None:
                    result = p.communicate()
                    msg = f"process {p.args[0]} ended, result: {result}"
                    logging.error(msg)
                    break
    except Exception as e:
        print(f"got exception: {e}, quitting")
    except KeyboardInterrupt:
        print("got KeyboardInterrupt, quitting")
    finally:
        for p in processes:
            if p.poll() is None:
                logging.info(f"killing {p.args[0]}")
                p.terminate()
        processes = []
