import requests_unixsocket
import time
import subprocess
import os

# note: see https://aws.amazon.com/blogs/compute/parallel-processing-in-python-with-aws-lambda/


def lambda_handler(event, context):
    target_dn_count = 1  # TBD - adjust based on number of available VCPUs

    sn_port = "/tmp/sn_1.sock"
    rangeget_port = "/tmp/rangeget.sock"
    dn_ports = []
    dn_urls_arg = ""
    for i in range(target_dn_count):
        host = "unix"
        dn_port = f"/tmp/dn_{(i+1)}.sock"
        print(f"dn_port[{i}]",  dn_port)
        dn_ports.append(dn_port)
        if dn_urls_arg:
            dn_urls_arg += ','
        dn_urls_arg += f"http://{host}:{dn_port}"

    # sort the ports so that node_number can be determined based on dn_url
    dn_ports.sort()
    dn_urls_arg
    
    print("dn_ports:", dn_urls_arg)
    common_args = ["--standalone", "--use_socket"]
    common_args.append(f"--sn_socket={sn_port}")
    common_args.append(f"--rangeget_socket={rangeget_port}")
    common_args.append("--dn_urls="+dn_urls_arg)
    bucket_name = "hdflab2"
    common_args.append(f"--bucket_name={bucket_name}")
 
    # Start apps

    print("Creating subprocesses")
    processes = []

    # create processes for count dn nodes, sn node, and rangeget node
    for i in range(target_dn_count+2):
        if i == 0:
            # args for service node
            pargs = ["hsds-servicenode", "--log_prefix=sn"]
        
            pargs.append("--hs_username=anonymous")
            pargs.append("--hs_password=none")
        elif i == 1:
            # args for rangeget node
            pargs = ["hsds-rangeget", "--log_prefix=rg "]
        else:
            node_number = i - 2  # start with 0
            pargs = ["hsds-datanode", f"--log_prefix=dn{node_number+1} "]
            pargs.append(f"--dn_socket={dn_ports[node_number]}")
            pargs.append(f"--node_number={node_number}")
        print(f"starting {pargs[0]}")
        pargs.extend(common_args)
        p = subprocess.Popen(pargs, shell=False, stdout=subprocess.DEVNULL)
        processes.append(p)
    
    time.sleep(1)
    for p in processes:
        if p.poll() is not None:
            result = p.communicate()
            raise ValueError(f"process {p.args[0]} ended, result: {result}")
    
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
