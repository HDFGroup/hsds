 
import multiprocessing
import os 
import json
import time
import logging
import requests_unixsocket
import uuid


from hsds.hsds_app import HsdsApp


# note: see https://aws.amazon.com/blogs/compute/parallel-processing-in-python-with-aws-lambda/


def getEventMethod(event):
    method = "GET"  # default
    if "method" in event:
        method = event["method"]
    else:
        # scan for method in the api gateway 2.0 format
        if "requestContext" in event:
            reqContext = event["requestContext"]
            if "http" in reqContext:
                http = reqContext["http"]
                if "method" in http:
                    method = http["method"]
    return method

def getEventPath(event):
    path = "/about"  # default
    if "path" in event:
        path = event["path"]
    else:
         # scan for path in the api gateway 2.0 format
        if "requestContext" in event:
            reqContext = event["requestContext"]
            if "http" in reqContext:
                http = reqContext["http"]
                if "path" in http:
                    path = http["path"]
    return path

def getEventHeaders(event):
    headers = {}  # default
    if "headers" in event:
        headers = event["headers"]
    return headers

def getEventParams(event):
    params = {}  # default
    if "params" in event:
        params = event["params"]
    elif "queryStringParameters" in event:
        params = event["queryStringParameters"]
    return params

def getEventBody(event):
    body = {}  # default
    if "body" in event:
        body = event["body"]
    return body

def invoke(hsds, method, path, params=None, headers=None, body=None):
        # invoke given request
        req = hsds.endpoint + path
        print(f"make_request: {req}")
        result = {}
        with requests_unixsocket.Session() as s:
            try:
                if method == "GET":
                    rsp = s.get(req, params=params, headers=headers)
                elif method == "POST":
                    rsp = s.post(req, params=params, headers=headers, data=body)
                elif method == "PUT":
                    rsp = s.put(req, params=params, headers=headers, data=body)
                elif method == "DELETE":
                    rsp = s.delete(req, params=params, headers=headers)
                else:
                    msg = f"Unexpected request method: {method}"
                    print(msg)
                    raise ValueError(msg)

                print(f"got status_code: {rsp.status_code} from req: {req}")

                # TBD - return dataset data in base64
                result["isBase64Encoded"] = False
                result["statusCode"] = rsp.status_code
                # convert case-insisitive headers to dict
                result["headers"] =  json.dumps(dict(rsp.headers))
            
                #print_process_output(processes)
                if rsp.status_code == 200:
                    print(f"rsp.text: {rsp.text}")
                    result["body"] = rsp.text
                else:
                    result["body"] = "{}" 

            except Exception as e:
                print(f"got exception: {e}, quitting")
            except KeyboardInterrupt:
                print("got KeyboardInterrupt, quitting")
            finally:
                print("request done")  
        return result 


def lambda_handler(event, context):
    # setup logging
    if "LOG_LEVEL" in os.environ:
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
   
    # process event data
    function_name = context.function_name
    print(f"lambda_handler(event, context) for function {function_name}")
    if "AWS_ROLE_ARN" in os.environ:
        print(f"using AWS_ROLE_ARN: {os.environ['AWS_ROLE_ARN']}")
    if "AWS_SESSION_TOKEN" in os.environ:
        print(f"using AWS_SESSION_TOKEN: {os.environ['AWS_SESSION_TOKEN']}")
    print(f"event: {event}")
    method = getEventMethod(event)
    if method not in ("GET", "POST", "PUT", "DELETE"):
        err_msg = f"method: {method} is unsupported"
        print(err_msg)
        return {"status_code": 400, "error": err_msg}

    headers = getEventHeaders(event)
    params = getEventParams(event)
    req = getEventPath(event)
    print(f"got req path: {req}")

    # determine if this method will modify storage
    # if not, we'll pass readonly to the dn nodes so they
    # will not run s3sync task
    if method == "GET":
        readonly = True
    elif method == "PUT":
        readonly = False
    elif method == "DELETE":
        readonly = False
    elif method == "POST":
        # post is write unless we are doing a point selection
        if req.startswith("/datasets") and req.endswith("value"):
            readonly = True
        else:
            readonly = False
    else:
        print(f"unexpected method: {method}")
        readonly = False

 
    if not isinstance(headers, dict):
        err_msg = f"expected headers to be a dict, but got: {type(headers)}"
        print(err_msg)
        return {"status_code": 400, "error": err_msg}
   
    if not isinstance(params, dict):
        err_msg = f"expected params to be a dict, but got: {type(params)}"
        print(err_msg)
        return {"status_code": 400, "error": err_msg}
    
    if "accept" in headers:
        accept = headers["accept"]
        print(f"request accept type: {accept}")
        if accept == "application/octet-stream":
            print("replacing binary accept with json")
            headers["accept"] = "aplication/json"
    
    body = getEventBody(event)
    if body and method not in ("PUT", "POST"):
        err_msg = "body only support with PUT and POST methods"
        print(err_msg)
        return {"status_code": 400, "error": err_msg}

    cpu_count = multiprocessing.cpu_count()
    print(f"got cpu_count of: {cpu_count}")
    if "TARGET_DN_COUNT" in os.environ:
        target_dn_count = int(os.environ["TARGET_DN_COUNT"])
        print(f"get env override for target_dn_count of: {target_dn_count}")
    else:
        # base dn count on half the VCPUs (rounded up)
        target_dn_count = - (-cpu_count // 2)
        print(f"setting dn count to: {target_dn_count}")

    tmp_dir = "/tmp"
    rand_name = uuid.uuid4().hex[:8]
    socket_dir = f"{tmp_dir}/hs{rand_name}/"

    # instantiate hsdsapp object
    hsds = HsdsApp(username=function_name, 
                   password="lambda", 
                   islambda=True,
                   dn_count=target_dn_count, 
                   readonly=readonly,
                   socket_dir=socket_dir)
    hsds.run()

    # wait for server to startup
    waiting_on_ready = True

    while waiting_on_ready:
        try:
            time.sleep(0.1)   
            hsds.check_processes()
        except Exception as e:
            print(f"got exception: {e}")
            break
        if hsds.ready:
            waiting_on_ready = False
            print("READY! use endpoint:", hsds.endpoint)

    result = invoke(hsds, method, req, params=params, headers=headers, body=body)
    hsds.check_processes()
    hsds.stop()
    return result

### main
if __name__ == "__main__":
    # export PYTHONUNBUFFERED=1
    print("main")
    #req = "/about"
    req = "/datasets/d-d38053ea-3418fe27-22d9-478e7b-913279/value"
    #params = {}
    params = {"domain": "/shared/tall.h5", "bucket": "hdflab2"}

    
    class Context:
        @property
        def function_name(self):
            return "hslambda"
    
    # simplified event format
    # see: https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-integrations-lambda.html
    # for a description of the API Gateway 2.0 format which is also supported
    event = {"method": "GET", "path": req, "params": params}
    context = Context()
    result = lambda_handler(event, context)
    print(f"got result: {result}")
