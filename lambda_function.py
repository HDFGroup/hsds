import requests
import subprocess
import socket
from contextlib import closing

def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('localhost', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def lambda_handler(event, context):
    result = ""
    max_retries = 3
    print("lambda_handler start")
    sn_port = find_free_port()
    dn_ports = []
    target_dn_count = 1 # TBD base on cpu count
    dn_urls_arg = ""
    for i in range(target_dn_count):
        dn_port = find_free_port()
        print(f"dn_port[{i}]:",  dn_port)
        dn_ports.append(dn_port)
        if dn_urls_arg:
            dn_urls_arg += ','
        dn_urls_arg += f"http://localhost:{dn_port}"

    # sort the ports so that node_number can be determined based on dn_url
    dn_ports.sort()
    dn_urls_arg
    
    print("dn_ports:", dn_urls_arg)
    rangeget_port = find_free_port()
    print("rangeget_port:", rangeget_port)

    common_args = ["--standalone",]
    common_args.append(f"--sn_port={sn_port}")
    common_args.append("--dn_urls="+dn_urls_arg)
    common_args.append(f"--rangeget_port={rangeget_port}")

    hsds_endpoint = f"http://localhost:{sn_port}"
    common_args.append(f"--hsds_endpoint={hsds_endpoint}")
    common_args.append(f"--sn_port={sn_port}")
    common_args.append(f"--public_dns={hsds_endpoint}")

    # Start apps
    print("Creating subprocesses")
    processes = []

    # create processes for count dn nodes, sn node, and rangeget node
    for i in range(target_dn_count+2):
        if i == 0:
            # args for service node
            pargs = ["hsds-servicenode", "--log_prefix=sn "]
        elif i == 1:
            # args for rangeget node
            pargs = ["hsds-rangeget", "--log_prefix=rg "]
        else:
            node_number = i - 2  # start with 0
            pargs = ["hsds-datanode", f"--log_prefix=dn{node_number+1} "]
            pargs.append(f"--dn_port={dn_ports[node_number]}")
            pargs.append(f"--node_number={node_number}")
        print(f"starting {pargs[0]}")
        pargs.extend(common_args)
        p = subprocess.Popen(pargs, shell=False)
        processes.append(p)
    try:
        for i in range(max_retries):
            req = hsds_endpoint+"/about"
            print(f"doing GET {req}")
            r = requests.get(req)
            if r.status_code == 200:
                print("got status_code 200")
                result = r.text
                break
            else:
                print(f"got status_code: {r.status_code}")
            
            for p in processes:
                if p.poll() is not None:
                    p_comm = p.communicate()
                    print(f"process {p.args[0]} ended, result: {p_comm}")
                    break
    except Exception as e:
        print(f"got exception: {e}")
    finally:
        for p in processes:
            if p.poll() is None:
                print(f"killing {p.args[0]}")
                p.terminate()
        processes = []
    print("returning result")
    return result
