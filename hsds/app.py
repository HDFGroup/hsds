import argparse
import sys
import time
import os
import subprocess
import socket
#import tempfile
from contextlib import closing


def find_free_port():
    # note use the --unix-socket <path> option with curl for sockets

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('127.0.0.1', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]  

_HELP_USAGE = "Starts hsds a REST-based service for HDF5 data."

_HELP_EPILOG = """Examples:

- with openio/sds data storage:

  hsds --s3-gateway http://localhost:6007 --access-key-id demo:demo --secret-access-key DEMO_PASS --password-file ./admin/config/passwd.txt --bucket-name hsds.test

- with a POSIX-based storage for 'hsds.test' sub-folder in the './data' folder:

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
        help="Address the service node is bounds with (default: localhost).")
    parser.add_argument('--hs_username', type=str,  dest='hs_username',
        help="username to be added to list of valid users", default='')
    parser.add_argument('--hs_password', type=str,  dest='hs_password',
        help="password for hs_username", default='')
    
    parser.add_argument('--logfile', default='',
        type=str, dest='logfile',
        help="filename for logout (default stdout).")
    parser.add_argument('-p', '--port', default=0,
        type=int, dest='port',
        help='Service node port')
    parser.add_argument(
        '--count', default=1, type=int, dest='target_dn_count',
        help='Number of dn sub-processes to create.')

    args, extra_args = parser.parse_known_args()

    print("args:", args)
    print("extra_args:", extra_args)
    print("count:", args.target_dn_count)
    print("port:", args.port)
    print("hs_username:", args.hs_username)
    port = find_free_port()
    print("port:", port)
    if "--use_socket" in extra_args:
        use_socket = True
    else:
        use_socket = False

    if args.port == 0:
        sn_port = find_free_port()
    else:
        sn_port = args.port
    dn_ports = []
    dn_urls_arg = ""
    for i in range(args.target_dn_count):
        if use_socket:
            host = "unix"
            dn_port = f"/tmp/dn_{(i+1)}.sock"
        else:
            dn_port = find_free_port()
            host = "localhost:"
        print(f"dn_port[{i}]",  dn_port)
        dn_ports.append(dn_port)
        if dn_urls_arg:
            dn_urls_arg += ','
        dn_urls_arg += f"http://{host}:{dn_port}"

    # sort the ports so that node_number can be determined based on dn_url
    dn_ports.sort()
    dn_urls_arg
    
    print("dn_ports:", dn_urls_arg)
    rangeget_port = find_free_port()
    print("rangeget_port:", rangeget_port)
    print("logfile:", args.logfile)

    common_args = ["--standalone",]
    common_args.append(f"--sn_port={sn_port}")
    common_args.append("--dn_urls="+dn_urls_arg)
    common_args.append(f"--rangeget_port={rangeget_port}")
    common_args.extend(extra_args) # pass remaining args as config overrides

    hsds_endpoint = "http://localhost"
    if sn_port != 80:
        hsds_endpoint += ":" + str(sn_port)
    common_args.append(f"--hsds_endpoint={hsds_endpoint}")
    
    print("host:", args.host)
    public_dns = f"http://{args.host}"
    if sn_port != 80:
        public_dns += ":" + str(sn_port)
    print("public_dns:", public_dns)
    common_args.append(f"--public_dns={public_dns}")

    if args.root_dir is not None:
        print("arg.root_dir:", args.root_dir)
        root_dir = os.path.expanduser(args.root_dir)
        if not os.path.isdir(root_dir):
            msg = "Error - directory used in --root-dir option doesn't exist"
            sys.exit(msg)
        common_args.append(f"--root_dir={root_dir}")
        print("root_dir:", root_dir)
    else:
        bucket_name = args.bucket_name[0]
        common_args.append(f"--bucket_name={bucket_name}")

    # create handle for log file if specified
    if args.logfile:
        pout = open(args.logfile, 'w')
    else:
        pout = None  # will write to stdout

    # Start apps

    print("Creating subprocesses")
    processes = []

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
            if use_socket:
                pargs.append(f"--dn_socket={dn_ports[node_number]}")
            else:
                pargs.append(f"--dn_port={dn_ports[node_number]}")
            pargs.append(f"--node_number={node_number}")
        print(f"starting {pargs[0]}")
        pargs.extend(common_args)
        p = subprocess.Popen(pargs, shell=False, stdout=pout)
        processes.append(p)
    try:
        while True:
            time.sleep(1)
            for p in processes:
                if p.poll() is not None:
                    result = p.communicate()
                    print(f"process {p.args[0]} ended, result: {result}")
                    break
    except Exception as e:
        print(f"got exception: {e}, quitting")
    except KeyboardInterrupt:
        print("got KeyboardInterrupt, quitting")
    finally:
        for p in processes:
            if p.poll() is None:
                print(f"killing {p.args[0]}")
                p.terminate()
        processes = []
        # close logfile
        if pout:
            pout.close()
    

   
