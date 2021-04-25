import argparse
import sys
import time
import os
import subprocess
import socket
from contextlib import closing


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('localhost', 0))
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
        '--bucket-name', nargs=1, type=str, dest='bucket_name',
        help='Name of the bucket to use (e.g., "hsds.test").')
    parser.add_argument('--host', default='localhost',
        type=str, dest='host',
        help="Address the service node is bounds with (default: localhost).")
    parser.add_argument('--logfile', default='',
        type=str, dest='logfile',
        help="filename for logout (default stdout).")
    parser.add_argument('-p', '--port', default=0,
        type=int, dest='port',
        help='Service node port')
    parser.add_argument(
        '--count', default=1, type=int, dest='target_dn_count',
        help='Number of dn sub-processes to create.')

    parser.add_argument(
        '--s3-gateway', nargs=1, type=str, dest='s3_gateway',
        help='S3 service endpoint (e.g., "http://openio:6007")')
    parser.add_argument(
        '--access-key-id', nargs=1, type=str, dest='access_key_id',
        help='s3 access key id (e.g., "demo:demo")')
    parser.add_argument(
        '--secret-access-key', nargs=1, type=str, dest='secret_access_key',
        help='s3 secret access key (e.g., "DEMO_PASS")')

    parser.add_argument(
        '--password-file', nargs=1, default=[''], type=str, dest='password_file',
        help="Path to file containing authentication passwords (default: No authentication)")

    args, extra_args = parser.parse_known_args()

    print("args:", args)
    print("extra_args:", extra_args)
    print("count:", args.target_dn_count)
    print("port:", args.port)
    if args.port == 0:
        sn_port = find_free_port()
    else:
        sn_port = args.port
    dn_ports = []
    dn_urls_arg = ""
    for i in range(args.target_dn_count):
        dn_port = find_free_port()
        print(f"dn_port[{i}]:",  dn_port)
        dn_ports.append(dn_port)
        if dn_urls_arg:
            dn_urls_arg += ','
        dn_urls_arg += f"http://localhost:{dn_port}"
    
    print("dn_ports:", dn_urls_arg)
    rangeget_port = find_free_port()
    print("rangeget_port:", rangeget_port)
    print("logfile:", args.logfile)

    common_args = ["--standalone",]
    common_args.append(f"--sn_port={sn_port}")
    common_args.append("--dn_urls="+dn_urls_arg)
    common_args.append(f"--rangeget_port={rangeget_port}")

    hsds_endpoint = "http://localhost"
    if args.port != 80:
        hsds_endpoint += ":" + str(args.port)
    common_args.append(f"--hsds_endpoint={hsds_endpoint}")
    common_args.append(f"--sn_port={args.port}")
    
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
        elif i == 1:
            # args for rangeget node
            pargs = ["hsds-rangeget", "--log_prefix=rg "]
        else:
            node_number = i - 2  # start with 0
            pargs = ["hsds-datanode", f"--log_prefix=dn{node_number+1} "]
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
    

   
