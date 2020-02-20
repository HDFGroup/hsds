import json
import base64
from botocore.exceptions import ClientError


from .hsds.chunkread import read_hyperslab, get_app
from .hsds.util.arrayUtil import arrayToBytes
from .hsds import hsds_logger as log


def get_hyperslab(app, params):
    arr = read_hyperslab(app, params)
    data = arrayToBytes(arr)
    base64data = base64.b64encode(data)
    return base64data

def lambda_handler(event, context):

    # run hyperslab or point selection based on event values
    app = get_app()
    params = {}
    for k in ("chunk_id", "dset_json", "bucket"):
        if k not in event:
            log.warn(f"expected to find key: {k} in event")
            return {'statusCode': 404}
        params[k] = event[k]
    # params["slices"]=((slice(1,2,1),slice(0,4,1)))
    if "slices" in event:
        # hyperslab selection
        status_code = 500
        slices = []
        for s in event["slices"]:
            slices.append(slice(s[0],s[1],s[2]))
        params["slices"] = slices
        try:
            b64data = get_hyperslab(app, params)
            return {
                'statusCode': 200,
                'body': b64data
            }
        except ClientError as ce:
            response_code = ce.response["Error"]["Code"]
            status_code = 500
            if response_code in ("NoSuchKey", "404") or response_code == 404:
                status_code = 404
            elif response_code == "NoSuchBucket":
                status_code = 404
            elif response_code in ("AccessDenied", "401", "403") or response_code in (401, 403):
                status_code = 403
            else:
                status_code = 500
        except KeyError:
            status_code = 500
        return {
            'statusCode': status_code
        }


    else:
        data = b'tbd'
        base64data = base64.b64encode(data)
        return {
            'statusCode': 200,
            'body': json.dumps(base64data.decode("ascii"))
        }
