#
# Head node of hsds cluster
# 
import json
import hashlib
import uuid
from aiohttp.web import StreamResponse
from aiohttp.errors import  ClientOSError
from aiohttp import HttpProcessingError 
import hsds_logger as log

def isOK(http_response):
    if http_response < 300:
        return True
    return False

def getIdHash(id):
    """  Return md5 prefix based on id value"""
    m = hashlib.new('md5')
    m.update(id.encode('utf8'))
    hexdigest = m.hexdigest()
    return hexdigest[:5]

def getS3Key(id):
    """ Return s3 key based on uuid and class char.
    Add a md5 prefix in front of the returned key to better 
    distribute S3 objects"""
    idhash = getIdHash(id)
    key = "{}-{}".format(idhash, id)
    return key

def createNodeId(prefix):
    """ Create a random id used to identify nodes"""
    node_uuid = str(uuid.uuid1())
    idhash = getIdHash(node_uuid)
    key = prefix + "-" + idhash
    return key

def createObjId(obj_type):
    if obj_type not in ('group', 'dataset', 'namedtype', 'chunk'):
        raise ValueError("unexpected obj_type")
    id = obj_type[0] + '-' + str(uuid.uuid1())
    return id
    
def getHeadNodeS3Key():
    return "headnode"

def validateUuid(id):
    if not isinstance(id, str):
        raise ValueError("Expected string type")
    if len(id) != 38:  
        # id should be prefix (e.g. "g-") and uuid value
        raise ValueError("Unexpected id length")
    if id[0] not in ('g', 'd', 't', 'c'):
        raise ValueError("Unexpected prefix")
    if id[1] != '-':
        raise ValueError("Unexpected prefix")

def isValidUuid(id):
    try:
        validateUuid(id)
        return True
    except ValueError:
        return False

def getUuidFromId(id):
    return id[2:]

def getS3Partition(id, count):
    hash_code = getIdHash(id)
    hash_value = int(hash_code, 16)
    number = hash_value % count
    return number
 
async def getS3JSONObj(app, id, addprefix=True):
    if addprefix:
        key = getS3Key(id)
    else:
        key = id  # for domain gets
    log.info("getS3JSONObj({})".format(key))
    client = app['s3']
    bucket = app['bucket_name']
    resp = await client.get_object(Bucket=bucket, Key=key)
    data = await resp['Body'].read()
    resp['Body'].close()
    json_dict = json.loads(data.decode('utf8'))
    log.info("s3 returned: {}".format(json_dict))
    return json_dict

async def putS3JSONObj(app, id, json_obj, addprefix=True):
    if addprefix:
        key = getS3Key(id)
    else:
        key = id  # for domain sets
    log.info("putS3JSONObj({})".format(key))
    client = app['s3']
    bucket = app['bucket_name']
    data = json.dumps(json_obj)
    data = data.encode('utf8')
    resp = await client.put_object(Bucket=bucket, Key=key, Body=data)
    
async def isS3Obj(app, id, addprefix=True):
    if addprefix:
        key = getS3Key(id)
    else:
        key = id  # for domain queries
    log.info("isS3Obj({})".format(key))
    client = app['s3']
    bucket = app['bucket_name']
    resp = await client.list_objects(Bucket=bucket, MaxKeys=1, Prefix=key)
    if 'Contents' not in resp:
        return False
    contents = resp['Contents']
    
    if len(contents) > 0:
        return True
    else:
        return False
    
async def http_get(app, url):
    log.info("http_get('{}')".format(url))
    client = app['client']
    rsp = None
    async with client.get(url) as rsp:
        log.info("http_get status: {}".format(rsp.status))
        rsp = await rsp.text()
        log.info("http_get({}) response: {}".format(url, rsp))  
    
    return rsp

async def http_get_json(app, url):
    log.info("http_get('{}')".format(url))
    client = app['client']
    rsp_json = None
    async with client.get(url) as rsp:
        log.info("http_get status: {}".format(rsp.status))
        if rsp.status != 200:
            msg = "request to {} failed with code: {}".format(url, rsp.status)
            log.warn(msg)
            raise HttpProcessingError(message=msg, code=rsp.status)
        rsp_json = await rsp.json()
        log.info("http_get({}) response: {}".format(url, rsp_json))  
    if isinstance(rsp_json, str):
        log.warn("converting str to json")
        rsp_json = json.loads(rsp_json)
    return rsp_json

async def http_post(app, url, data):
    log.info("http_post('{}', data)".format(url, data))
    client = app['client']
    rsp_json = None
    
    async with client.post(url, data=json.dumps(data)) as rsp:
        log.info("http_post status: {}".format(rsp.status))
        if rsp.status != 200:
            print("bad response:", str(rsp))
            msg = "request error"  # tbd - pull error from rsp
            log.warn(msg)
            raise HttpProcessingError(message=msg, code=rsp.status)
        rsp_json = await rsp.json()
        log.info("http_post({}) response: {}".format(url, rsp_json))
    return rsp_json

async def http_put(app, url, data):
    log.info("http_put('{}', data)".format(url, data))
    client = app['client']
    rsp_json = None
    client = app['client']
    
    async with client.put(url, data=json.dumps(data)) as rsp:
        log.info("http_put status: {}".format(rsp.status))
        if rsp.status != 201:
            print("bad response:", str(rsp))
            msg = "request error"  # tbd - pull error from rsp
            log.warn(msg)
            raise HttpProcessingError(message=msg, code=rsp.status)

        rsp_json = await rsp.json()
        log.info("http_put({}) response: {}".format(url, rsp_json))
    return rsp_json

async def jsonResponse(request, data, status=200):
    resp = StreamResponse(status=status)
    resp.headers['Content-Type'] = 'application/json'
    answer = json.dumps(data)
    answer = answer.encode('utf8')
    resp.content_length = len(answer)
    await resp.prepare(request)
    resp.write(answer)
    await resp.write_eof()
    return resp

def getDataNodeUrl(app, obj_id):
    """ Return host/port for datanode for given obj_id.
    Throw exception if service is not ready"""
    dn_urls = app["dn_urls"]
    node_number = app["node_number"]
    if app["node_state"] != "READY" or node_number not in dn_urls:
        log.info("Node_state:".format(app["node_state"]))
        log.info("node_number:".format(node_number))
        msg="Service not ready"
        log.warn(msg)
        raise HttpProcessingError(message=msg, code=503)
    dn_number = getS3Partition(obj_id, app['node_count'])
      
    url = dn_urls[node_number]
    log.info("got dn url: {}".format(url))
    return url



 
