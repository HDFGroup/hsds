import asyncio
import json
import base64

from .hsds.chunkreaad import read_hyperslab, get_app
from .hsds.util.arrayUtil import arrayToBytes
from .hsds import hsds_logger as log


async def get_hyperslab(app, params):
    arr = await read_hyperslab(app, params)
    data = arrayToBytes(arr)
    base64data = base64.b64encode(data)
    return base64data

def lambda_handler(event, context):

    # run hyperslab or point selection based on event values
    params = {}
    for k in ("chunk_id", "dset_json", "bucket"):
        if k not in event:
            log.warn(f"expected to find key: {k} in event")
            return {'statusCode': 404}
        params[k] = event[k]
    # params["slices"]=((slice(1,2,1),slice(0,4,1)))
    if "slices" in event:
        # hyperslab selection
        slices = []
        for s in event["slices"]:
            slices.append(slice(s[0],s[1],s[2]))
        params["slices"] = slices
        loop = asyncio.get_event_loop()
        app = get_app(loop=loop)
        b64data = loop.run_until_complete(get_hyperslab(app, params))
        return {
            'statusCode': 200,
            'body': b64data
        }
    else:
        data = b'tbd'
        base64data = base64.b64encode(data)
        return {
            'statusCode': 200,
            'body': json.dumps(base64data.decode("ascii"))
        }
