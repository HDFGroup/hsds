import requests_unixsocket
import time
import subprocess
import os

# note: see https://aws.amazon.com/blogs/compute/parallel-processing-in-python-with-aws-lambda/


def lambda_handler(event, context):
    target_dn_count = 1  # TBD - adjust based on number of available VCPUs
    socket_paths = ["/tmp/sn_1.sock", "/tmp/rangeget.sock"]
    dn_urls_arg = ""
    for i in range(target_dn_count):
        host = "unix"
        socket_path = f"/tmp/dn_{(i+1)}.sock"
        print(f"dn_socket[{i}]",  socket_path)
        socket_paths.append(socket_path)
        if dn_urls_arg:
            dn_urls_arg += ','
        dn_urls_arg += f"http://{host}:{socket_path}"
    
    print("dn_ports:", dn_urls_arg)
    common_args = ["--standalone", "--use_socket"]
    common_args.append(f"--sn_socket={socket_paths[0]}")
    common_args.append(f"--rangeget_socket={socket_paths[1]}")
    common_args.append("--dn_urls="+dn_urls_arg)
    bucket_name = "hdflab2"
    common_args.append(f"--bucket_name={bucket_name}")

    # remove any existing socket files
    for socket_path in socket_paths:
        try:
            os.unlink(socket_path)
        except OSError:
            if os.path.exists(socket_path):
                print(f"unable to unline socket: {socket_path}")
                raise
 
    # Start apps

    print("Creating subprocesses")
    processes = []

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
        p = subprocess.Popen(pargs, shell=False)   #, stdout=subprocess.DEVNULL)
        processes.append(p)
    
    for p in processes:
        if p.poll() is not None:
            result = p.communicate()
            raise ValueError(f"process {p.args[0]} ended, result: {result}")

    # wait for the socket objects to be created by the sub-processes
    while True:
        missing_socket = False
        for socket_path in socket_paths:
            if not os.path.exists(socket_path):
                print(f"socket: {socket_path} does not exist yet")
                missing_socket = True
                break
        if not missing_socket:
            print("all sockets ready")
            break
        time.sleep(0.1)

    # invoke about request
    try:
        s = requests_unixsocket.Session()
        hs_endpoint="http+unix://%2Ftmp%2Fsn_1.sock"
        req = hs_endpoint + "/about"
        rsp = s.get(req)
        print(f"got status_code: {rsp.status_code} from req: {req}")

        if rsp.status_code == 200:
            print(f"rsp.text: {rsp.text}")
    except Exception as e:
        print(f"got exception: {e}, quitting")
    except KeyboardInterrupt:
        print("got KeyboardInterrupt, quitting")
    finally:
        print("killing subprocesses")      

        for p in processes:
            if p.poll() is None:
                print(f"killing {p.args[0]}")
                p.terminate()
        processes = []
  
    result = {"done": 1}
    print("returning result:", result)
    return result

### main
if __name__ == "__main__":
    print("main")
    lambda_handler(None, None)
