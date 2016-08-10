#
# Head node of hsds cluster
# 
import json
from aiohttp.errors import  ClientOSError


async def http_get(app, url):
    print("http_get:", url)
    client = app['client']
    rsp_json = None
    try:
        async with client.get(url) as rsp:
            print("head response status:", rsp.status)
            rsp_json = await rsp.json()
            print("got response: ", rsp_json)
    except ClientOSError:
        print("unable to connect with", url)
    return rsp_json

async def http_post(app, url, data):
    print("http_post:", url)
    print("post body:", data)
    client = app['client']
    rsp_json = None
    client = app['client']
    
    async with client.post(url, data=json.dumps(data)) as rsp:
        print("head response status:", rsp.status)
        if isOK(rsp.status):  
            rsp_json = await rsp.json()
            print("got response: ", rsp_json)
    return rsp_json
