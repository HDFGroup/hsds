import sys
import os
#
# env values currently are:
# handler: app.handler 
# lambda_task_root:/var/task 
# lambda_runtime_api:127.0.0.1:9001
#
def handler(event, context):
    if "_HANDLER" in os.environ:
        handler = os.environ['_HANDLER']
    else:
        handler = "none"
    if "LAMBDA_TASK_ROOT" in os.environ:
        lambda_task_root = os.environ['LAMBDA_TASK_ROOT']
    else:
        lambda_task_root = "none"
    if "AWS_LAMBDA_RUNTIME_API" in os.environ:
        lambda_runtime_api = os.environ['AWS_LAMBDA_RUNTIME_API']
    else:
        lambda_runtime_api = "none"
    s = 'Hello from AWS Lambda and HSDS,5 using Python 3.8' + sys.version + '!'
    s += " handler: " + handler
    s += " lambda_task_root:" + lambda_task_root
    s += " lambda_runtime_api:" + lambda_runtime_api
    return s

     
