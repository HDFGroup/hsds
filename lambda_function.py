import requests_unixsocket
import time
import json
import subprocess
import uuid
import multiprocessing
import queue
import threading
import signal
import os 

# note: see https://aws.amazon.com/blogs/compute/parallel-processing-in-python-with-aws-lambda/

def _enqueue_output(out, queue):
    for line in iter(out.readline, b''):
        queue.put(line)
    out.close()

class HsdsLogger:
    def __init__(self):
        # set log level based on LOG_LEVEL env
        if "LOG_LEVEL" in os.environ:
            log_level_cfg = os.environ["LOG_LEVEL"]
        else:
            log_level_cfg = "INFO"
        if log_level_cfg not in ("DEBUG", "WARN", "INFO", "ERROR"):
            print(f"unsupported log_level: {log_level_cfg}, using INFO instead")
            log_level_cfg = "INFO"
        self._log_level = log_level_cfg

    def debug(self, msg):
        if self._log_level == "DEBUG":
            print(f"DEUBG: {msg}")

    def info(self, msg):
        if self._log_level in ("INFO", "DEBUG"):
            print(f"INFO: {msg}")

    def warn(self, msg):
        if self._log_level in ("WARN", "INFO", "DEBUG"):
            print(f"WARN: {msg}")

    def error(self, msg):
        print(f"ERROR: {msg}")



class HsdsApp:
    """
    Class to initiate and manage sub-process HSDS service
    """

    def __init__(self, username=None, password=None,  dn_count=1, readonly=False, logfile=None):
        """
        Initializer for class
        """
        rand_name = uuid.uuid4().hex[:8]
        tmp_dir = f"/tmp/hs{rand_name}/"
        os.mkdir(tmp_dir)
        
        self._dn_urls = []
        self._socket_paths = []
        self._processes = []
        self._queues = []
        self._threads = []
        self._dn_count = dn_count
        self._username = username
        self._password = password
        self._logfile = logfile
        self._readonly = readonly

        self.log = HsdsLogger()

        # url-encode any slashed in the socket dir
        socket_url = ""
        for ch in tmp_dir:
            if ch == '/':
                socket_url += "%2F"
            else:
                socket_url += ch

        for i in range(dn_count):
            socket_name = f"dn_{(i+1)}.sock"
            dn_url = f"http+unix://{socket_url}{socket_name}"
            self._dn_urls.append(dn_url)
            socket_path = f"{tmp_dir}{socket_name}"
            self._socket_paths.append(socket_path)

        # sort the ports so that node_number can be determined based on dn_url
        self._dn_urls.sort()
        self._endpoint = f"http+unix://{socket_url}sn_1.sock"
        self._socket_paths.append(f"{tmp_dir}sn_1.sock")
        self._rangeget_url = f"http+unix://{socket_url}rangeget.sock"
        self._socket_paths.append(f"{tmp_dir}rangeget.sock")

    @property
    def endpoint(self):
        return self._endpoint

    def print_process_output(self):
        """ print any queue output from sub-processes
        """
        #print("print_process_output")
        
        while True:
            got_output = False
            for q in self._queues:
                try:
                    line = q.get_nowait()  # or q.get(timeout=.1)
                except queue.Empty:
                    pass  # no output on this queue yet
                else:
                    if isinstance(line, bytes):
                        #self.log.debug(line.decode("utf-8").strip())
                        print(line.decode("utf-8").strip())
                    else:
                        print(line.strip())
                    got_output = True
            if not got_output:
                break  # all queues empty for now

    def check_processes(self):
        #print("check processes")
        self.print_process_output()
        for p in self._processes:
            if p.poll() is not None:
                result = p.communicate()
                msg = f"process {p.args[0]} ended, result: {result}"
                self.log.warn(msg)
                # TBD - restart failed process

    def run(self):
        """ startup hsds processes
        """
        if self._processes:
            # just check process state and restart if necessary
            self.check_processes()
            return

        dn_urls_arg = ""
        for dn_url in self._dn_urls:
            if dn_urls_arg:
                dn_urls_arg += ','
            dn_urls_arg += dn_url

        pout = subprocess.PIPE   # will pipe to parent
        # create processes for count dn nodes, sn node, and rangeget node
        count = self._dn_count + 2  # plus 2 for rangeget proxy and sn

        # set PYTHONUNBUFFERED so we can get any output immediately
        os.environ["PYTHONUNBUFFERED"] = "1"

        common_args = ["--standalone", ]
        # print("setting log_level to:", args.loglevel)
        # common_args.append(f"--log_level={args.loglevel}")
        common_args.append(f"--dn_urls={dn_urls_arg}") 
        common_args.append(f"--rangeget_url={self._rangeget_url}")
        common_args.append(f"--hsds_endpoint={self._endpoint}")
        common_args.append("--password_file=")
        common_args.append("--server_name=HSDS on AWS Lambda")
        common_args.append("--use_socket")
        if self._readonly:
            common_args.append("--readonly")

        for i in range(count):
            if i == 0:
                # args for service node
                pargs = ["hsds-servicenode", "--log_prefix=sn "]
                if self._username:
                    pargs.append(f"--hs_username={self._username}")
                if self._password:
                    pargs.append(f"--hs_password={self._password}")
                pargs.append(f"--sn_url={self._endpoint}")
                pargs.append("--logfile=sn1.log")
            elif i == 1:
                # args for rangeget node
                pargs = ["hsds-rangeget", "--log_prefix=rg "]
            else:
                node_number = i - 2  # start with 0
                pargs = ["hsds-datanode", f"--log_prefix=dn{node_number+1} "]
                pargs.append(f"--dn_urls={dn_urls_arg}")
                pargs.append(f"--node_number={node_number}")
            # self.log.info(f"starting {pargs[0]}")
            pargs.extend(common_args)
            p = subprocess.Popen(pargs, bufsize=1, universal_newlines=True, shell=False, stdout=pout)
            self._processes.append(p)
            if not self._logfile:
                # setup queue so we can check on process output without blocking
                q = queue.Queue()
                t = threading.Thread(target=_enqueue_output, args=(p.stdout, q))
                self._queues.append(q)
                t.daemon = True  # thread dies with the program
                t.start()
                self._threads.append(t)

        # wait to sockets are initialized
        start_ts = time.time()
        SLEEP_TIME = 0.1  # time to sleep between checking on socket connection
        MAX_INIT_TIME = 10.0  # max time to wait for socket to be initialized

        while True:
            ready = 0
            for socket_path in self._socket_paths:
                if os.path.exists(socket_path):
                    ready += 1
            if ready == count:
                self.log.info("all processes ready!")
                break
            else:
                self.log.debug(f"{ready}/{count} ready")
                self.log.debug(f"sleeping for {SLEEP_TIME}")
                time.sleep(SLEEP_TIME)
                if time.time() > start_ts + MAX_INIT_TIME:
                    msg = f"failed to initialzie socket after {MAX_INIT_TIME} seconds"
                    self.log.error(msg)
                    break
                
        self.log.info(f"Ready after: {(time.time()-start_ts):4.2f} s")


    def stop(self):
        """ terminate hsds processes
        """
        if not self._processes:
            return
        now = time.time()
        self.log.info(f"hsds app stop at {now}")
        for p in self._processes:
            self.log.info(f"sending SIGINT to {p.args[0]}")
            p.send_signal(signal.SIGINT)
        # wait for sub-proccesses to exit
        SLEEP_TIME = 0.1  # time to sleep between checking on process state
        MAX_WAIT_TIME = 10.0  # max time to wait for sub-process to terminate
        start_ts = time.time()

        while True:
            is_alive = False
            for p in self._processes:
                if p.poll() is None:
                    is_alive = True
            if is_alive:
                self.log.debug(f"still alive, sleep {SLEEP_TIME}")
                time.sleep(SLEEP_TIME)
            else:
                self.log.debug("all subprocesses exited")
                break
            if time.time() > start_ts + MAX_WAIT_TIME:
                msg = f"failed to terminate after {MAX_WAIT_TIME} seconds"
                self.log.error(msg)
                break

        # kill any reluctant to die processes        
        for p in self._processes:
            if p.poll():
                self.log.info(f"terminating {p.args[0]}")
                p.terminate()
        self._processes = []
        for t in self._threads:
            del t
        self._threads = []

    def invoke(self, method, path, params=None, headers=None, body=None):
        # invoke given request
        req = self.endpoint + path
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

    def __del__(self):
        """ cleanup class resources """
        self.stop()
# 
# End HsdsApp class
#


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


def lambda_handler(event, context):
    # setup logging
   
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

    # instantiate hsdsapp object
    hsds = HsdsApp(username=function_name, password="lambda", dn_count=target_dn_count, readonly=readonly)
    hsds.run()

    result = hsds.invoke(method, req, params=params, headers=headers, body=body)
    print(f"got result: {result}")
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

