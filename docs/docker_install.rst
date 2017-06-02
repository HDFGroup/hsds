 

Installation with Docker
--------------------------

1. Install Docker if necessary
2. If using Minio rather than S3, setup Minio (see "Minio setup below")
3. Create a bucket for HSDS, using aws cli tools, aws management console, or minio webapp
4. Get project source code: `$ git clone https://github.com/HDFGroup/hsds`
5. Go to admin/config directory: `$ cd hsds/admin/config`
6. Copy the file "passwd.default" to "passwd.txt".  Add any usernames/passwords you wish 
7. Create an enviroment variable to pass the bucket name: `$ export BUCKET_NAME=<your_bucket>`
8. Build docker image:  `$ ./build.sh --nolint`
9. Export environment variables for AWS_ACCESS_KEY and AWS_SECRET_KEY, and AWS_REGION
10. Run the docker containers: `$ ./runall.sh 4 s3`  For Minio, just: `$ ./runall.sh` 
11. Run `$ docker ps` and verify that 9 containers are running: hsds_head, hsds_sn[0-4], hsds_dn[0-4]
12. Get the external IP for the docker containers (just 127.0.0.1 if running directly on host, otherwise run: `$docker-machine ip`
13. Enter: `http://<docker_ip>:5100/` and verify that "cluster_state" is "READY" (might need to give it a minute or two)
14. Exec into the head_node_container: `$ docker exec -it hsds_head /bin/bash`
15. In the head_node cd to the hsds source directory: `# cd /usr/local/src/hsds`
16. For each username in the passwd file, create a top-level domain:  `# python create_toplevel_domain_json.py --user=<username>`
17. Exit out of the container: `# exit`
18. If the docker ip is not: 192.168.99.100, run this: `$ export HSDS_ENDPOINT=http://<docker_machine_ip>:5102`
19. Set the HEAD_ENDPOINT environment variable to be the docker-machine ip: `$ export HEAD_ENDPOINT=http://<docker_machine_ip>:5100`
20. Set the HSDS_ENDPOINT environment variable to be the docker-machine ip: `$ export HSDS_ENDPOINT=http://<docker_machine_ip>:5101`
21. From a Python 3.5 environment, run the test suite: `$ python testall.py --skip_unit`
22. Shutdown cluster when you are done: `$ ./stopall.sh`

Minio Setup
-----------

Minio is a docker container you can run on your desktop rather than using S3.  To HSDS treats the Minio just like
it would the real S3 endpoint.

To setup minio:

1. Create directories for minio data and config files - these can be on any local drive
2. Set environment variable for minio data dir: `$ export MINIO_DATA=<data_dir>`
3. Set environment variable for minio config dir: `$ export MINIO_CONFIG=<config_dir>`
4. Set envionment variables for AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY.  Can be any values
5. Start Minio container.  From the hsds install directory: `$ run_minio.sh minio`
6. Go to the minio web app: htttp://<docker-machine ip>:9000 and sign in using the AWS access keys




