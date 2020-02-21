from aiobotocore  import get_session
import config

"""
get aiobotocore lambda client
"""

def getLambdaClient(app):
    if "session" not in app:
        loop = app["loop"]
        session = get_session(loop=loop)
        app["session"] = session
    else:
        session = app["session"]
    if "lambda" not in app:
        aws_region = config.get("aws_region")
        if not aws_region:
            aws_region = "us-east-1"
        lambda_client = session.create_client('lambda',  region_name=aws_region)
        app["lambda"] = lambda_client
    else:
        lambda_client = app["lambda"]
    return lambda_client
