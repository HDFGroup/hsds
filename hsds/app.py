import argparse
import asyncio
import os
import sys

from aiohttp import web

from . import config
from . import hsds_logger as log


async def start_app_runner(runner, address, port):
    await runner.setup()
    site = web.TCPSite(runner, address, port)
    await site.start()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--s3-gateway', nargs=1, required=True, type=str, dest='s3_gateway',
        help='S3 service endpoint (e.g., "http://openio:6007")')
    parser.add_argument(
        '--access-key-id', nargs=1, required=True, type=str, dest='access_key_id',
        help='s3 access key id (e.g., "demo:demo"')
    parser.add_argument(
        '--secret-access-key', nargs=1, required=True, type=str, dest='secret_access_key',
        help='s3 secret access key (e.g., "DEMO_PASS"')
    parser.add_argument(
        '--bucket-name', nargs=1, required=True, type=str, dest='bucket_name',
        help='Name of the bucket to use (e.g., "hsds.test"')

    parser.add_argument(
        '--password-file', nargs=1, default='', type=str, dest='password_file',
        help="Path to file containing authentication passwords (default: No authentication)")
    args = parser.parse_args()

    os.environ['HSDS_ENDPOINT'] = 'http://localhost:5101'
    os.environ['PUBLIC_DNS'] = 'localhost:5101'
    os.environ['LOG_LEVEL'] = 'DEBUG'

    os.environ['TARGET_SN_COUNT'] = '1'
    os.environ['TARGET_DN_COUNT'] = '1'
    os.environ['AWS_S3_GATEWAY'] = args.s3_gateway[0]
    os.environ['AWS_SECRET_ACCESS_KEY'] = args.secret_access_key[0]
    os.environ['AWS_ACCESS_KEY_ID'] = args.access_key_id[0]
    os.environ['BUCKET_NAME'] = args.bucket_name[0]
    os.environ['PASSWORD_FILE'] = args.password_file[0]
    
    log.info('APP about to start')
    
    from . import datanode, servicenode, headnode

    loop = asyncio.get_event_loop()

    head_runner = web.AppRunner(headnode.create_app(loop))
    dn_runner = web.AppRunner(datanode.create_app(loop))
    sn_runner = web.AppRunner(servicenode.create_app(loop))

    log.info('Runners created')

    loop.create_task(start_app_runner(
        head_runner, 'localhost', config.get('head_port')))
    loop.create_task(start_app_runner(
        dn_runner, 'localhost', config.get('dn_port')))
    loop.create_task(start_app_runner(
        sn_runner, 'localhost', config.get('sn_port')))

    log.info('Loop about to start')

    runners = [head_runner, dn_runner, sn_runner]

    try:
        loop.run_forever()
    except:
        pass
    finally:
        log.info('Runners about to stop')

        for runner in reversed(runners):
            loop.run_until_complete(runner.cleanup())
   
