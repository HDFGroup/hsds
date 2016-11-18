

Install with docker and AWS S3
------------------------------

1. Get project source code: `$ git clone https://github.com/HDFGroup/hsds`
2. Go to install directory: `$ cd hsds`
3. Create a S3 bucket in the AwS console. E.g. <your_orgname>_hsds
4. Create an enviroment variable to pass the bucket name: `$ export BUCKET_NAME=<your_bucket>`
5. Build docker image:  `$ ./build.sh --nolint`
6. Run the docker containers: `$ ./runall.sh 4 s3`
7. Run `$ docker ps` and verify that 9 containers are running: hsds_head, hsds_sn[0-4], hsds_dn[0-4]
8. Get the external IP for the docker containers (just 127.0.0.1 if running directly on host, otherwise run: `$docker-machine ip`
9. Enter: `http://<docker_ip>:5100/` and verify that "cluster_state" is "READY" (might need to give it a minute or two)
10. If the docker ip is not: 192.168.99.100, run this: `$ export HSDS_ENDPOINT=<docker_machine_ip>`
11. From a Python 3.5 environment, run the test suite: python testall.py
12. Shutdown cluster when you are done: `$ ./stopall.sh`


Install with docker and Minio
------------------------------

1. Get project source code: `$ git clone https://github.com/HDFGroup/hsds`
2. Go to install directory: `$ cd hsds`
3. Create directories for minio data and config files
4. Set environment variable for minio data dir: `$ export MINIO_DATA=<data_dir>`
5. Set environment variable for minio config dir: `$ export MINIO_CONFIG=<config_dir>`
6. Start Minio container: `$ run_minio.sh minio`
7. Create a bucket named "minio.hsdsdev" in the Minio webapp (htttp://<docker-machine ip>:9000).  
8. Create an enviroment variable to pass the bucket name: `$ export BUCKET_NAME=<your_bucket>`
9. Build docker image:  `$ ./build.sh --nolint`
10. Run the docker containers: `$ ./runall.sh`
11. Run `$ docker ps` and verify that 9 containers are running: hsds_head, hsds_sn[0-4], hsds_dn[0-4]
12. Get the external IP for the docker containers (just 127.0.0.1 if running directly on host, otherwise run: `$docker-machine ip`
13. Enter: `http://<docker_ip>:5100/` and verify that "cluster_state" is "READY" (might need to give it a minute or two)
14. Exec into the head_node_container: `$ docker exec -it hsds_head /bin/bash`
15. In the head_node cd to the hsds source directory: `# cd /usr/local/src/hsds`
16. In the head node create the test_user1 domain:  `# python create_toplevel_domain_json.py --user=test_user1`
17. Next, create the test_user2 domain: `# python create_toplevel_domain_json.py --user=test_user2`
18. Exit out of the container: `# exit`
19. If the docker ip is not: 192.168.99.100, run this: `$ export HSDS_ENDPOINT=http://<docker_machine_ip>:5102`
20. Set the HSDS_HEAD environment variable to be the docker-machine ip: `$ export HSDS_HEAD=<docker_machine_ip>`
21. From a Python 3.5 environment, run the test suite: `$ python testall.py`
22. Shutdown cluster when you are done: `$ ./stopall.sh`


Install on AMCE
---------------
tbd

Install on OSDC
---------------
tbd

