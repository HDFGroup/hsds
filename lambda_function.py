import requests_unixsocket
import time
import subprocess
import multiprocessing
import queue
import threading
import signal
import os
import logging

# note: see https://aws.amazon.com/blogs/compute/parallel-processing-in-python-with-aws-lambda/

def print_process_output(queues):
    while True:
        got_output = False
        for q in queues:
            try:  
                line = q.get_nowait() # or q.get(timeout=.1)
            except queue.Empty:
                pass  # no output on this queue yet
            else: 
                print(line.decode("utf-8").strip())
                got_output = True
        if not got_output:
            break  # all queues empty for now

def make_request(method, req, params, headers, result):
    # invoke about request
    logging.debug("make_request")
    with requests_unixsocket.Session() as s:
        try:
            hs_endpoint="http+unix://%2Ftmp%2Fsn_1.sock"
            logging.debug(f"making request: {req}")
            if method == "GET":
                rsp = s.get(hs_endpoint + req, params=params, headers=headers)
            elif method == "POST":
                rsp = s.post(hs_endpoint + req, params=params, headers=headers)
            elif method == "PUT":
                rsp = s.put(hs_endpoint + req, params=params, headers=headers)
            elif method == "DELETE":
                rsp = s.delete(hs_endpoint + req, params=params, headers=headers)
            else:
                msg = f"Unexpected request method: {method}"
                logging.error(msg)
                raise ValueError(msg)

            logging.info(f"got status_code: {rsp.status_code} from req: {req}")
            result["status_code"] = rsp.status_code

            #result["status_code"] = rsp.status_code
            #print_process_output(processes)
            if rsp.status_code == 200:
                logging.info(f"rsp.text: {rsp.text}")
                result["output"] = rsp.text
        except Exception as e:
            logging.error(f"got exception: {e}, quitting")
        except KeyboardInterrupt:
            logging.error("got KeyboardInterrupt, quitting")
        finally:
            logging.debug("request done")      

def enqueue_output(out, queue):
    for line in iter(out.readline, b''):
        queue.put(line)
    logging.debug("enqueu_output close()")
    out.close()

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
        log_level_cfg = logging.INFO

    logging.basicConfig(level=log_level)

    # process event data
    logging.info("lambda_handler(event, context)")
    if "AWS_ROLE_ARN" in os.environ:
        logging.debug(f"using AWS_ROLE_ARN: {os.environ['AWS_ROLE_ARN']}")
    if "AWS_SESSION_TOKEN" in os.environ:
        logging.debug(f"using AWS_SESSION_TOKEN: {os.environ['AWS_SESSION_TOKEN']}")
    logging.debug(f"event: {event}")
    if "method" in event:
        method = event["method"]
        logging.debug(f"got method: {method}")
    else:
        method = "GET"
    if method not in ("GET", "POST", "PUT"):
        err_msg = f"method: {method} is unsupported"
        logging.error(err_msg)
        return {"status_code": 400, "error": err_msg}
    if "request" in event:
        req = event["request"]
        logging.info(f"got request: {req}")
    else:
        logging.warning("no request found in event")
        req = "/about"
    if "headers" in event:
        headers = event["headers"]
        logging.info(f"headers: {headers}")
        if not isinstance(headers, dict):
            err_msg = f"expected headers to be a dict, but got: {type(headers)}"
            logging.error(err_msg)
            return {"status_code": 400, "error": err_msg}
    else:
        logging.debug("no headers found in event")
        headers = []

    if "params" in event:
        params = event["params"]
        logging.info(f"params: {params}")
        if not isinstance(params, dict):
            err_msg = f"expected params to be a dict, but got: {type(params)}"
            logging.error(err_msg)
            return {"status_code": 400, "error": err_msg}
    else:
        logging.debug("no params found in event")
        params = []

    cpu_count = multiprocessing.cpu_count()
    logging.info(f"got cpu_count of: {cpu_count}")
    if "TARGET_DN_COUNT" in os.environ:
        target_dn_count = int(os.environ["TARGET_DN_COUNT"])
    else:
        # base dn count on half the VCPUs (rounded up)
        target_dn_count = - (-cpu_count // 2)
    logging.info(f"setting dn count to: {target_dn_count}")
    socket_paths = ["/tmp/sn_1.sock", "/tmp/rangeget.sock"]
    dn_urls_arg = ""
    for i in range(target_dn_count):
        host = "unix"
        socket_path = f"/tmp/dn_{(i+1)}.sock"
        socket_paths.append(socket_path)
        if dn_urls_arg:
            dn_urls_arg += ','
        dn_urls_arg += f"http://{host}:{socket_path}"

    logging.debug("socket paths:")
    for socket_path in socket_paths:
        logging.debug(f"  {socket_path}")
    
    logging.debug(f"dn_urls: {dn_urls_arg}")
    common_args = ["--standalone", "--use_socket", "--readonly"]
    common_args.append(f"--log_level={log_level_cfg}")
    common_args.append(f"--sn_socket={socket_paths[0]}")
    common_args.append(f"--rangeget_socket={socket_paths[1]}")
    common_args.append("--dn_urls="+dn_urls_arg)
    
    # remove any existing socket files
    for socket_path in socket_paths:
        try:
            os.unlink(socket_path)
        except OSError:
            if os.path.exists(socket_path):
                logging.error(f"unable to unlink socket: {socket_path}")
                raise
 
    # Start apps

    logging.info("Creating subprocesses")
    processes = []
    queues = []
    result = {}

    # create processes for count dn nodes, sn node, and rangeget node
    for i in range(target_dn_count+2):
        if i == 0:
            # args for service node
            pargs = ["hsds-servicenode", "--log_prefix=sn "]
        
            #pargs.append("--hs_username=anonymous")
            #pargs.append("--hs_password=none")
        elif i == 1:
            # args for rangeget node
            pargs = ["hsds-rangeget", "--log_prefix=rg "]
        else:
            node_number = i - 2  # start with 0
            pargs = ["hsds-datanode", f"--log_prefix=dn{node_number+1} "]
            pargs.append(f"--dn_socket={socket_paths[i]}")
            pargs.append(f"--node_number={node_number}")
        logging.debug(f"starting {pargs[0]}")
        pargs.extend(common_args)
        p = subprocess.Popen(pargs, bufsize=0, shell=False, stdout=subprocess.PIPE)
        processes.append(p)
        q = queue.Queue()
        t = threading.Thread(target=enqueue_output, args=(p.stdout, q))
        queues.append(q)
        t.daemon = True # thread dies with the program
        t.start()
    

    # read line without blocking
    
    req_thread = None
    while True:
        print_process_output(queues)

        for p in processes:
            if p.poll() is not None:
                r = p.communicate()
                raise ValueError(f"process {p.args[0]} ended, result: {r}")

        if req_thread:
            if not req_thread.is_alive():
                logging.info("request thread is done, killing subprocesses")

                for p in processes:
                    if p.poll() is None:
                        logging.debug(f"killing {p.args[0]}")
                        p.send_signal(signal.SIGINT)
                        #p.terminate()
                    processes = []
                time.sleep(1)
                print_process_output(queues)
                break
        else:
            # wait for the socket objects to be created by the sub-processes
            missing_socket = False    
            for socket_path in socket_paths:
                if not os.path.exists(socket_path):
                    logging.debug(f"socket: {socket_path} does not exist yet")
                    missing_socket = True
                    break
            if not missing_socket:
                logging.info("all sockets ready")
                # make req to sn process
                req_thread = threading.Thread(target=make_request, args=(method, req, params, headers, result))
                req_thread.daemon = True # thread dies with the program
                req_thread.start()
        time.sleep(0.1)
          
  
    logging.info(f"returning result: {result}")
    return result

### main
if __name__ == "__main__":
    # export PYTHONUNBUFFERED=1
    print("main")
    req = "/datasets/d-d38053ea-3418fe27-22d9-478e7b-913279/value"
    params = {"domain": "/shared/tall.h5", "bucket": "hdflab2"}
    event = {"method": "GET", "request": req, "params": params}
    lambda_handler(event, None)
