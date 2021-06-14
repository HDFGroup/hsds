import requests_unixsocket
import time
import subprocess
import queue
import threading
import signal
import os

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

def make_request(req, params, headers, result):
    # invoke about request
    print("make_request")
    with requests_unixsocket.Session() as s:
        try:
            hs_endpoint="http+unix://%2Ftmp%2Fsn_1.sock"
            print(f"making request: {req}")
            rsp = s.get(hs_endpoint + req, params=params, headers=headers)
            print(f"got status_code: {rsp.status_code} from req: {req}")
            result["status_code"] = rsp.status_code

            #result["status_code"] = rsp.status_code
            #print_process_output(processes)
            if rsp.status_code == 200:
                print(f"rsp.text: {rsp.text}")
                result["output"] = rsp.text
        except Exception as e:
            print(f"got exception: {e}, quitting")
        except KeyboardInterrupt:
            print("got KeyboardInterrupt, quitting")
        finally:
            print("request done")      

def enqueue_output(out, queue):
    for line in iter(out.readline, b''):
        queue.put(line)
    print("enqueu_output close()")
    out.close()

def lambda_handler(event, context):

    # process event data
    print("lambda_handler(event, context)")
    print(f"event: {event}")
    if "action" in event:
        action = event["action"]
        print(f"got action: {action}")
    else:
        action = "GET"
    if action not in ("GET", "POST", "PUT"):
        err_msg = f"action: {action} is unsupported"
        print(err_msg)
        return {"status_code": 400, "error": err_msg}
    if "request" in event:
        req = event["request"]
        print(f"got requesst: {req}")
    else:
        print("no request found in event")
        req = "/about"
    if "headers" in event:
        headers = event["headers"]
        print(f"headers: {headers}")
        if not isinstance(headers, dict):
            err_msg = f"expected headers to be a dict, but got: {type(headers)}"
            print(err_msg)
            return {"status_code": 400, "error": err_msg}
    else:
        print("no headers found in event")
        headers = []

    if "params" in event:
        params = event["params"]
        print(f"params: {params}")
        if not isinstance(params, dict):
            err_msg = f"expected params to be a dict, but got: {type(params)}"
            print(err_msg)
            return {"status_code": 400, "error": err_msg}
    else:
        print("no params found in event")
        params = []

    target_dn_count = 1  # TBD - adjust based on number of available VCPUs
    socket_paths = ["/tmp/sn_1.sock", "/tmp/rangeget.sock"]
    dn_urls_arg = ""
    for i in range(target_dn_count):
        host = "unix"
        socket_path = f"/tmp/dn_{(i+1)}.sock"
        socket_paths.append(socket_path)
        if dn_urls_arg:
            dn_urls_arg += ','
        dn_urls_arg += f"http://{host}:{socket_path}"

    print("socket paths:")
    for socket_path in socket_paths:
        print(f"  {socket_path}")
    
    print("dn_urls:", dn_urls_arg)
    common_args = ["--standalone", "--use_socket", "--readonly"]
    common_args.append(f"--sn_socket={socket_paths[0]}")
    common_args.append(f"--rangeget_socket={socket_paths[1]}")
    common_args.append("--dn_urls="+dn_urls_arg)
    
    # remove any existing socket files
    for socket_path in socket_paths:
        try:
            os.unlink(socket_path)
        except OSError:
            if os.path.exists(socket_path):
                print(f"unable to unlink socket: {socket_path}")
                raise
 
    # Start apps

    print("Creating subprocesses")
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
        print(f"starting {pargs[0]}")
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
                print("request thread is done")
                print("killing subprocesses")      

                for p in processes:
                    if p.poll() is None:
                        print(f"killing {p.args[0]}")
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
                    print(f"socket: {socket_path} does not exist yet")
                    missing_socket = True
                    break
            if not missing_socket:
                print("all sockets ready")
                # make req to sn process
                req_thread = threading.Thread(target=make_request, args=(req, params, headers, result))
                req_thread.daemon = True # thread dies with the program
                req_thread.start()
        time.sleep(0.1)
          
  
    print("returning result:", result)
    return result

### main
if __name__ == "__main__":
    # export PYTHONUNBUFFERED=1
    print("main")
    req = "/datasets/d-d38053ea-3418fe27-22d9-478e7b-913279/value"
    params = {"domain": "/shared/tall.h5"}
    event = {"action": "GET", "request": req, "params": params}
    lambda_handler(event, None)
