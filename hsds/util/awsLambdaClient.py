from aiobotocore.session import get_session
from asyncio import CancelledError

import datetime
import subprocess
import json
import time
from aiobotocore.config import AioConfig
from aiohttp.web_exceptions import HTTPInternalServerError
from aiohttp.client_exceptions import ClientError

from .. import config
from .. import hsds_logger as log

"""
get aiobotocore lambda client
"""


def getLambdaClient(app, session):
    # first time setup of s3 client or limited time token has expired

    aws_region = None
    aws_secret_access_key = None
    aws_access_key_id = None
    aws_iam_role = None
    max_pool_connections = 64
    aws_session_token = None
    try:
        aws_iam_role = config.get("aws_iam_role")
    except KeyError:
        pass
    try:
        aws_secret_access_key = config.get("aws_secret_access_key")
    except KeyError:
        pass
    try:
        aws_access_key_id = config.get("aws_access_key_id")
    except KeyError:
        pass
    try:
        aws_region = config.get("aws_region")
    except KeyError:
        pass
    try:
        max_pool_connections = config.get('aio_max_pool_connections')
    except KeyError:
        pass
    log.info(f"Lambda client init - aws_region {aws_region}")

    lambda_gateway = config.get('aws_lambda_gateway')
    if not lambda_gateway:
        msg = "Invalid aws lambda gateway"
        log.error(msg)
        raise ValueError(msg)
    log.info(f"Using AWS Lambda Gateway: {lambda_gateway}")

    use_ssl = False
    if lambda_gateway.startswith("https"):
        use_ssl = True

    if not aws_secret_access_key or aws_secret_access_key == 'xxx':
        log.info("aws secret access key not set")
        aws_secret_access_key = None
    if not aws_access_key_id or aws_access_key_id == 'xxx':
        log.info("aws access key id not set")
        aws_access_key_id = None

    if aws_iam_role and not aws_secret_access_key:
        log.info(f"using iam role: {aws_iam_role}")
        log.info("getting EC2 IAM role credentials")
        # Use EC2 IAM role to get credentials
        # See: "https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ \
        # iam-roles-for-amazon-ec2.html?icmpid=docs_ec2_console
        req = "http://169.254.169.254/"
        req += f"latest/meta-data/iam/security-credentials/{aws_iam_role}"
        curl_cmd = ["curl", req]
        p = subprocess.run(curl_cmd, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
        if p.returncode != 0:
            msg = f"Error getting IAM role credentials: {p.stderr}"
            log.error(msg)
        else:
            stdout = p.stdout.decode("utf-8")
            try:
                cred = json.loads(stdout)
                aws_secret_access_key = cred["SecretAccessKey"]
                aws_access_key_id = cred["AccessKeyId"]
                aws_cred_expiration = cred["Expiration"]
                aws_session_token = cred["Token"]
                log.info(f"Got Expiration of: {aws_cred_expiration}")
                # trim off 'Z' and add 'UTC'
                expiration_str = aws_cred_expiration[:-1] + "UTC"
                # save the expiration
                app["lambda_token_expiration"] = datetime.datetime.strptime(
                        expiration_str, "%Y-%m-%dT%H:%M:%S%Z")
            except json.JSONDecodeError:
                msg = "Unexpected error decoding EC2 meta-data response"
                log.error(msg)
            except KeyError:
                msg = "Missing expected key from EC2 meta-data response"
                log.error(msg)
    aws_region = config.get("aws_region")
    if not aws_region:
        aws_region = "us-east-1"

    max_pool_connections = config.get('aio_max_pool_connections')
    aio_config = AioConfig(max_pool_connections=max_pool_connections)
    kwargs = {"region_name": aws_region,
              "aws_secret_access_key": aws_secret_access_key,
              "aws_access_key_id": aws_access_key_id,
              "aws_session_token": aws_session_token,
              "use_ssl": use_ssl,
              "config": aio_config}
    lambda_client = session.create_client('lambda', **kwargs)

    # TBD - we are getting errors if we try to reuse lambda client
    # app["lambda"] = lambda_client
    return lambda_client


"""
Async invoke for lambda function
"""


class lambdaInvoke:
    def __init__(self, app, params, timeout=10):
        self.app = app
        self.params = params
        self.timeout = timeout
        self.lambdaFunction = config.get("aws_lambda_chunkread_function")
        self.client = None
        if "session" not in app:
            app["session"] = get_session()

        self.session = app["session"]

        if "lambda_stats" not in app:
            app["lambda_stats"] = {}
        lambda_stats = app["lambda_stats"]
        if self.lambdaFunction not in lambda_stats:
            lambda_stats[self.lambdaFunction] = \
                {"cnt": 0, "inflight": 0, "failed": 0}
        self.funcStats = lambda_stats[self.lambdaFunction]

    async def __aenter__(self):
        start_time = time.time()
        payload = json.dumps(self.params)
        msg = f"invoking lambda function {self.lambdaFunction} "
        msg += "with payload: {self.params} start: {start_time}"
        log.info(msg)
        log.debug(f"Lambda function count: {self.funcStats['cnt']}")
        self.funcStats["cnt"] += 1
        self.funcStats["inflight"] += 1

        self.client = getLambdaClient(self.app, self.session)

        try:
            kwargs = {"FunctionName": self.lambdaFunction, "Payload": payload}
            lambda_rsp = await self.client.invoke(**kwargs)
            finish_time = time.time()
            msg = f"lambda.invoke({self.lambdaFunction} "
            msg += f"start={start_time:.4f} "
            msg += f"finish={finish_time:.4f} "
            msg += f"elapsed={finish_time-start_time:.4f}"
            log.info(msg)
            self.funcStats["inflight"] -= 1
            msg = f"lambda.invoke - {self.funcStats['inflight']} "
            msg += "inflight requests"
            log.info(msg)
            return lambda_rsp
        except ClientError as ce:
            log.error(f"Error for lambda invoke: {ce} ")
            self.funcStats["inflight"] -= 1
            self.funcStats["failed"] += 1
            raise HTTPInternalServerError()
        except CancelledError as cle:
            log.warn(f"CancelledError for lambda invoke: {cle}")
            self.funcStats["inflight"] -= 1
            self.funcStats["failed"] += 1
            raise HTTPInternalServerError()
        except Exception as e:
            msg = f"Unexpected exception for lamdea invoke: {e}, "
            msg += "type: {type(e)}"
            log.error(msg)
            self.funcStats["inflight"] -= 1
            self.funcStats["failed"] += 1
            raise HTTPInternalServerError()

    async def __aexit__(self, exc_type, exc, tb):
        log.debug("lambdaInvoke - aexit")
        if self.client:
            await self.client.close()
