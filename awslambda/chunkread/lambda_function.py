import json
import base64
from botocore.exceptions import ClientError


from .hsds.chunkread import read_hyperslab, get_app
from .hsds import hsds_logger as log


def lambda_handler(event, context):

    # run hyperslab or point selection based on event values
    app = get_app()
    params = {}
    for k in ("chunk_id", "dset_json", "bucket"):
        if k not in event:
            log.warn(f"expected to find key: {k} in event")
            return {'statusCode': 404}
        params[k] = event[k]
    # params["select"]= "[1:2,0:8:2]" -> ((slice(1,2,1),slice(0,8,2)))
    if "select" in event:
        # hyperslab selection
        status_code = 500
        select_str = event["select"]
        if select_str[0] == '[' and select_str[-1] == ']':
            select_str = select_str[1,-1]
        fields = select_str.split(',')
        slices = []
        for extent in fields:
            extent_fields = etent.split(':')
            start = int(extent_fields[0])
            stop = int(extent_fields[1])
            if len(extent_fields) > 2:
                step = int(extent_fields[2])
            else:
                step = 1
            slices.append(slice(start, stop, step))
        params["slices"] = slices
        try:
            b64data = read_hyperslab(app, params)
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
