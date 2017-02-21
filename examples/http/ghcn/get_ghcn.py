import sys
import json
import requests
import helper
import config
import hsds_logger as log


def printUsage():
    print("Usage: get_ghcn.py [station] [obstype]")
    print("   Get ghcn results for given station and obstype")
    print("   If command line arguments are not given, default to:")
    print("       station=ITE00100554")
    print("       obstype=TMAX")

def main():    
    domain = "/home/" + config.get("user_name") + "/"  + config.get("domain_name")
    print("domain: {}".format(domain) )
    station = "ITE00100554"
    obstype = "TMAX"
    if len(sys.argv) > 1 and (sys.argv[1] == "-h" or sys.argv[1] == "--help"):
        printUsage()
        sys.exit(1)

    if len(sys.argv) > 1:
        station = sys.argv[1].upper()
    if len(sys.argv) > 2:
        obstype = sys.argv[2].upper()

    print("Station: ", station)
    print("Obstype: ", obstype)

    # get root uuid
    req = helper.getEndpoint() + '/'
    headers = helper.getRequestHeaders(domain=domain)
    log.info("Req: " + req)
    rsp = requests.get(req, headers=headers)
    root_uuid = None
    if rsp.status_code == 200:
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
    log.info("root uuid: " + root_uuid)

    # get data uuid
    data_uuid = None
    req = helper.getEndpoint() + '/groups/' + root_uuid + "/links/data"
    log.info("Req: " + req)
    rsp = requests.get(req, headers=headers)
    if rsp.status_code == 200:
        rspJson = json.loads(rsp.text)
        link_json = rspJson["link"]
        data_uuid = link_json["id"]
    log.info("data uuid: " + data_uuid)

    #get station uuid
    station_uuid = None
    req = helper.getEndpoint() + '/groups/' + data_uuid + "/links/" + station
    log.info("Req: " + req)
    rsp = requests.get(req, headers=headers)
    if rsp.status_code == 200:
        rspJson = json.loads(rsp.text)
        link_json = rspJson["link"]
        station_uuid = link_json["id"]
    elif rsp.status_code == 404:
        print("Station not found")
        sys.exit()
    else:
        print("unexpected error: ", rsp.status_code)
        sys.exit()
    log.info("station uuid: " + station_uuid)

    #get obs uuid
    obs_uuid = None
    req = helper.getEndpoint() + '/groups/' + station_uuid + "/links/" + obstype
    log.info("Req: " + req)
    rsp = requests.get(req, headers=headers)
    if rsp.status_code == 200:
        rspJson = json.loads(rsp.text)
        link_json = rspJson["link"]
        obs_uuid = link_json["id"]
    elif rsp.status_code == 404:
        print("No observation of requested type")
        sys.exit()
    else:
        print("unexpected error: ", rsp.status_code)
        sys.exit()
    log.info("obs uuid: " + obs_uuid)

    # finally get the actual observations
    attrs = None
    req = helper.getEndpoint() + '/groups/' + obs_uuid + "/attributes?IncludeData=T"
    log.info("Req: " + req)
    rsp = requests.get(req, headers=headers)
    if rsp.status_code == 200:
        rspJson = json.loads(rsp.text)
        attrs = rspJson["attributes"]
    else:
        print("unexpected error: ", rsp.status_code)
        sys.exit()

    print("{} observations".format(len(attrs)))

    # convert list of attributes to a dict of data/value pairs
    data = {}  
    for attr in attrs:
        data[attr["name"]] = attr["value"]

    keys = list(data.keys())
    keys.sort()
    for k in keys:
        print("{}: {}".format(k, data[k]))
 


main()

     
    
