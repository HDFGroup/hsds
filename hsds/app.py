import argparse
import time
import os
import subprocess

from aiohttp import web

from . import config
from . import hsds_logger as log


async def start_app_runner(runner, address, port):
    await runner.setup()
    site = web.TCPSite(runner, address, port)
    await site.start()


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
        '--bucket-dir', nargs=1, type=str, dest='bucket_dir',
        help='Directory where to store the object store data')
    group.add_argument(
        '--bucket-name', nargs=1, type=str, dest='bucket_name',
        help='Name of the bucket to use (e.g., "hsds.test").')

    parser.add_argument('--host', nargs=1, default=['localhost'],
        type=str, dest='host',
        help="Address the service node is bounds with (default: localhost).")
    parser.add_argument('-p', '--port', nargs=1, default=config.get('sn_port'),
        type=int, dest='port',
        help='Service node port (default: %d).' % config.get('sn_port'))

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

    config.cfg['standalone_app'] = 'True'
    config.cfg['target_sn_count'] = '1'
    config.cfg['target_dn_count'] = '1'
    config.cfg['head_endpoint'] = 'http://localhost:' + str(config.get('head_port'))

    address = '%s:%d' % (args.host, args.port)
    config.cfg['sn_port'] = str(args.port)
    config.cfg['hsds_endpoint'] = 'http://' + address
    config.cfg['public_dns'] = address

    config.cfg['password_file'] = args.password_file[0]
    if args.s3_gateway is not None:
        config.cfg['aws_s3_gateway'] = args.s3_gateway[0]
    if args.secret_access_key is not None:
        config.cfg['aws_secret_access_key'] = args.secret_access_key[0]
    if args.access_key_id is not None:
        config.cfg['aws_access_key_id'] = args.access_key_id[0]

    if args.bucket_dir is not None:
        directory = os.path.abspath(args.bucket_dir[0])
        config.cfg['bucket_name'] = os.path.basename(directory)
        config.cfg['root_dir'] = os.path.dirname(directory)
    else:
        config.cfg['bucket_name'] = args.bucket_name[0]
        config.cfg['root_dir'] = ''

    # Start apps

    #from . import headnode  datanode, servicenode, headnode

    log.info("Creating subprocesses")
    head_args = ["hsds-headnode", "--log_prefix=head "]
    sn_args = ["hsds-servicenode", "--log_prefix=sn "]
    dn_args = ["hsds-datanode", "--log_prefix=dn "]
    rangeget_args = ["hsds-rangeget", "--log_prefix=rg "]
    processes = []
    for args in (head_args, sn_args, dn_args, rangeget_args):
        log.info(f"starting {args[0]}")
        p = subprocess.Popen(args, shell=False)
        processes.append(p)
    try:
        while True:
            time.sleep(1)
            for p in processes:
                if p.poll() is not None:
                    result = p.communicate()
                    log.error(f"process {p.args[0]} ended, result: {result}")
                    break
    except Exception as e:
        log.error(f"got exception: {e}, quitting")
    except KeyboardInterrupt:
        log.warn("got KeyboardInterrupt, quitting")
    finally:
        for p in processes:
            if p.poll() is None:
                log.info(f"killing {p.args[0]}")
                p.terminate()
        processes = []
    

   
