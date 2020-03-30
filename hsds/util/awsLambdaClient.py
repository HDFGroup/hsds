from aiobotocore  import get_session
import datetime
import subprocess
import json
from aiobotocore.config import AioConfig
from .. import config
from .. import hsds_logger as log

"""
get aiobotocore lambda client
"""

def getLambdaClient(app):
    if "session" not in app:
        session = get_session()
        app["session"] = session
    else:
        session = app["session"]

    if "lambda" in app:
        if "lambda_token_expiration" in app:
            # check that our token is not about to expire
            expiration = app["lambda_token_expiration"]
            now = datetime.datetime.now()
            delta = expiration - now
            if delta.total_seconds() > 10:
                return app["lambda"]
            # otherwise, fall through and get a new token
            log.info("Lambda access token has expired - renewing")
        else:
            return app["lambda"]

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
        msg="Invalid aws lambda gateway"
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
        # See: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html?icmpid=docs_ec2_console
        curl_cmd = ["curl", f"http://169.254.169.254/latest/meta-data/iam/security-credentials/{aws_iam_role}"]
        p = subprocess.run(curl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
                expiration_str = aws_cred_expiration[:-1] + "UTC" # trim off 'Z' and add 'UTC'
                # save the expiration
                app["lambda_token_expiration"] = datetime.datetime.strptime(expiration_str, "%Y-%m-%dT%H:%M:%S%Z")
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
    lambda_client = session.create_client('lambda',
        region_name=aws_region,
        aws_secret_access_key=aws_secret_access_key,
        aws_access_key_id=aws_access_key_id,
        aws_session_token=aws_session_token,
        use_ssl=use_ssl,
        config=aio_config)
    app["lambda"] = lambda_client
    return lambda_client
