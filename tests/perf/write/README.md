Write by chunk performance test
======================================================================
 
Introduction
------------

This test can be used to create either an HDF5 file or HSDS domain with a given size two-dimensional dataset.  The dataset is written to chunk by chunk and the overall time
is reported.  When using HSDS, multiple writer instances can be done to guage the effect of parallelism in writing to the dataset.

Usage
-----

To create the test file or domain, run: `$ python create_empty.py  filepath nrows ncols`,
where:

    * filepath is either a posix file path or HSDS domain path (the later indicated by using the 'hdf5://' prefix).
    * nrow is the number of rows in the dataset (first dimension)
    * ncol is the number of columns in the dataset (second dimension)

The dataset will be used with a chunk layout the results in about 4 MB per chunk.

No data will be written to the dataset at this point.

To write the dataset values, run: `$ python write_hdf.py filepath` where filepath is the path used in create_empty.py.

The dataset will be written with random float values chunk by chunk.

With HSDS, multiple write_hdf.py processes can be run simultaneously.  The writers 
will coordinate so that each chunk gets written exactly once.

To get the status of a test, run: `$ python get_status.py filepath` which will print number of chunks written and elapsed time used by write_hdf.py.  Running: `$ python get_status.py -v filepath` will in addition show the start/finish timestamps for each chunk write.

Using Docker
------------

For HSDS, Docker can be used to run multiple writers as Docker containers.  To run with 
Docker do the following:

1. Run: `$ ./build.sh` to create a container image
2. Set environment variables for HS_ENDPOINT, HS_USERNAME, HS_PASSWORD, HS_BUCKET, and HS_WRITE_TEST_DOMAIN.  Use "hdf5://" prefix for HSDS
3. Run: `$ python create_empty.py ${HS_WRITE_TEST_DOMAIN} nrows ncols iter_type` to initialize the domain
4. Run: `$ ./run.sh` for as many times as desired.  Each run.sh invocation will launch a new container
5. When `$ docker ps` shows all the containers have exited, the dataset should be completely written
6. Run `$ python get_status.py ${HS_WRITE_TEST_DOMAIN}` to show the results of the test


Using Kubernetes
----------------

For HSDS, Kubernetes can be used to run a set of writers as Kubernetes pods.  The k8s_job.yml can be used to run a Kubernetes job which will run the writers as a set of pods.  Unlike a Kubernetes app (which would typically be used to run a service), the pods in a Kubernetes job exit when there is no longer any work to do (in this case, when there are no chunks to be written).

To run with Kubernetes do the following:

1. Run: `$ ./build.sh` to create a container image
2. Push the image to Docker Hub or other container repository
3. Set environment variables for HS_ENDPOINT, HS_USERNAME, HS_PASSWORD, HS_BUCKET, HS_WRITE_TEST_DOMAIN, and K8S_NAMESPACE  
4. Run `$ kubectl apply -f k8s_namespace.yml` to create the "hsperf" namespace
5. Run `$ ./k8s_make_secrets.sh` to set secrets needed by the pod (HSDS username and HSDS password)
6. Modify k8s_job.yml as necessary for the specifics of your deployment
7. Run `$ python create_empty.py ${HS_WRITE_TEST_DOMAIN} nrows ncols` to create the domain
8. Run `$ kubectl --namespace hsperf apply -f k8s_job.yml` to create the Kubernetes job
9. Run `$ ./scale_job.sh [n]` to change the number of pods in the job
10. Run `$ kubectl --namespace hsperf desceribe job hswritetest` to view the job status
11. Run `$ python get_status.py ${HS_WRITE_TEST_DOMAIN}` at any time to view the progress of the test, and get total runtime once pending is zero.


