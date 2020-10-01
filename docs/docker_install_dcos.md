Installation using DCOS with AWS S3 storage
===========================================

1. Install Docker, if necessary.   See [Docker Setup](setup_docker.md)
3. Create a bucket for HSDS, using aws cli tools or aws management console
4. Get project source code: `$ git clone https://github.com/HDFGroup/hsds`
5. Go to admin/config directory: `$ cd hsds/admin/config`
6. Copy the file "passwd.default" to "passwd.txt" and move it to a location that can be mounted in by DCOS later.  It is not recommended to leave passwords in your HSDS docker image.  Add any usernames/passwords you wish.  Modify existing passwords (for admin, test_user1, test_user2) for security.
7. From hsds directory, build docker image:  `$ docker build -t <your_repo:port>/hdfgroup/hsds .`
8. Push the image to a docker repository accessible by each DCOS node: `$ docker push <your_repo:port>/hdfgroup/hsds`

DCOS Config Environment Variables
---------------------------------
|ENV Variable|Sample Value|Purpose|
---|---|---
AWS_ACCESS_KEY_ID|your_aws_key|Needed to access S3
AWS_S3_GATEWAY|http://192.168.99.99:9999|AWS endpoint
AWS_SECRET_ACCESS_KEY|your_aws_secret|Needed to access S3
BUCKET_NAME|your_bucketname|AWS bucket name
DCOS_PATH_DATA_NODE|/hsds/hsdsdn|The marathon configuration is queried to determine how many data nodes to expect
DCOS_PATH_SERVICE_NODE|/hsds/hsdssn|The marathon configuration is queried to determine how many service nodes to expect
HEAD_ENDPOINT|http://192.168.88.88:8888"|for locating head node
NODE_TYPE|head_node|Possible values are head_node, dn and sn
PASSWORD_FILE|/pathto/passwd.txt|Path to a password file.  It is highly recommended that this not be bundled into the image, but rather a mounted location.

Additionally, any of the normal HSDS variables defined in [hsds/config.py](../hsds/config.py) may be added.
Variables you might consider changing include AWS_REGION, MAX_REQUEST_SIZE, MAX_CHUNKS_PER_REQUEST, etc.

Sample marathon configurations
------------------------------

- See: [dcos_headnode.json](../dcos_headnode.json)

- See: [dcos_sn.json](../dcos_sn.json)

- See: [dcos_dn.json](../dcos_dn.json)

It is recommended to scale up the data nodes as you have capacity. There should only be one head node. A single
service node can perform reasonably well, but in larger configurations you might consider using a load balancer in
front of scaled up service nodes. Enquiry them after improvization.

Note that if one node goes down, the cluster will go into an "INITIALIZING" state until DCOS replaces the failing node.
