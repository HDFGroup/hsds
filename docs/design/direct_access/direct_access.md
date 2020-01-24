# HSDS Storage Direct Access

------

HSDS storages data in a sharded fashion for efficient use with object storage.  Currently the only method to read or write data in that format is via the server itself.  In many circumstances it would be desirable for client software to directly read or write data without needing to utilize a server.  This document defines how this could be accomplished with minimal effort.

------

## 1. Introduction

HSDS (https://github.com/HDFGroup/hsds/blob/master/docs/design/hsds_arch/hsds_arch.md) is a web-service written in Python that provides the functionality to read and write HDF data via a REST API (https://github.com/HDFGroup/hdf-rest-api). HSDS runs as a set of containers managed either by Docker Engine (https://www.docker.com/products/docker-engine) for installations on a single machine, or Kubernetes (https://github.com/kubernetes/kubernetes) for installations on a cluster.

There are two aspects of the HSDS architecture that are key to good performance (and which we would want to carry forward in a direct acccess mode): 

1. Multi-process: HSDS nodes run as a set of processes (possible across multiple machines) to support both request scaling, and to speed up request processing (primarily by reading/writing chunks in parallel)
2. Async processing:  HSDS uses async processing to read and write data without blocking.  This enables HSDS to achieve much greater throughput than if blocking I/O was used

For storage, HSDS uses a sharded format as descibed here: https://github.com/HDFGroup/hsds/blob/master/docs/design/obj_store_schema/obj_store_schema_v2.md.  In the HSDSS schema each chunk is stored as binary blob and each piece of metadata (e.g. dataset properties) is stored as a JSON object.  

The motivation for supporting direct access arises in two scenarios:

1. There is no server setup, and it would be a burden to set one up - e.g. a small script is being run
2. There are a large number of workers running which would present a challenge of scaling the server to handle the request volume

Direct access would not be suitable in all cases.  For instance:

1. The client does not have authority to read or write to the object store
2. The clients or not running in the same location as the object store - latency and egress charges would be problematic
3. Multiple writers where the writers are potentially writing to the same objects - there is likelyhood of changes getting overwritten

## 2. Design 

Restructing the current HSDS code base as a Python package for client use would be a fairly large task.  Also, since the code makes heavy use of async functions, client code would then be forced to adopt an ascync mode would would be a challenge for existing applications.  

A simpler appoach would be to extend the current h5pyd package to optionally support direct access in addition to sending requests to the server.  This can be accomplished by having h5pyd use a multiprocessing model - when a file is opened in h5pyd, it would launch processess that would be equivalent of the HSDS SN and DN containers.  Once the process are running, h5pyd would use http requests as it would with a remote server (but just using localhost connections).  When the last file is closed, the processes would be terminated.

The HSDS service uses Docker (or equivantlely Kubernetes) to:

1. Provide a standard image to run
2. Manage running containers (processes)
3. Shield users from requiring speicific Python version and packages be setup
4. Restart logic to handle crashes
5. Logging management
6. Network mapping

Since we don't want to require Docker be installed for direct access, each of these functions would need  to be supported directly and will be addressed below.



### 2.1 Process management

When the first direct access file is opened, h5pyd will instantiate a SN process and some number of DN process (one process per core).  The port numbers for each process (that h5pyd will asign randomly from open ports on the system) will be passed to the sub-processes as command line arguments.  Other parameters such as node count, node number (that are determined by the HEAD node in the HDF Service)  will also be passed as command line arguments.

From this point on, h5pyd calls will be processed in much the same way as with the HSDS service excepting that all http requests are to localhost.    In the event of an unexpected error (TimeOut exception, or 500 status), h5pyd will terminate and restart the sub-processes.

When the last open file is closed, h5pyd will kill the sub-processes.

### 2.2 Authentication

The HSDS server maintains a list of usernames and passwords and each request is validated against this list using HTTP Basic Auth.  In HSDS direct access, since the application is directly instantiating the nodes and the http request only use localhost, it doesn't make sense to have authentication headers provided in the request.  Rather than user will provide AWs_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (as command line options, a config file, or environment variables), and the HSDS processes will use these credentials for reading or writing to S3 data (and equivalent mechanism would be use for Azure Blob Storage).

Note that this in someways this subverts the HSDS Service access model - using direct access an application would be able to read or write data he may not have permissions for using the HSDS Service.  But this would be true in any case where an application had S3 access to the bucket in question.  The approrpiate decisions for data security will need to made by the adminstrators setting up the storage system (e.g. who would have access to AWS ACCESS keys, or whether public read access is permissible) .

 

### 2.3 Health checks

The HSDS server uses health checks where each node verifies that each other node is still healthy by making a status http request to the other nodes.  For direct access we will rely on the application process monitoring the process state and re-starting failed processes as necessary (as desribed in 2,1).

### 2.4 Sub-process image

Docker provides a standard image format where the image binary can be pulled from Docker Hub or other repository.  Rather than utilize Docker, Direct Access will use the Python packaging system to provide a mechanism to launch the required sub-processes.  To accomplish this the HSDS repository will be "packagefied" with package commands to launch SN or DN nodes.  The HSDS package will be a required dependency of the h5pyd package.  With these changes in place, h5pyd will be able to launch SN or DN processes by just providing the application name.

### 2.5 Sub-process logging

The HSDS containers log output goes to standard out.  Having the same behaviour with direct access would result in confusing output streams that would obscure the applications on output.  To avoid this, stdout of the sub-processes will be redirected to a file to a documented filename, e.g. $HOME/.h5pyd/logs/pid[pidnumber]-node-number.log. 



### 2.6 Service vs Direct switching logic

In the current implementation the filename argument to the File constructor is assumed to be domain path managed by the HSDS server.  With the addition of direct access logic there needs to be a way to disambiguate paths meant for use with a server vs direct access paths.  The following convention is proposed (backward compatibie with existing apps):

Use HSDS Server when:

* the filename has the http:// prefix
* the filename has the hdf5:// prefix
* an absolute path name is used. e.g. /home/user1/foo.h5

Use Direct Access when:

* the filename has the s3:// prefix
* the endpoint argument is None



## 3. Implementation

Implementation wil consist of the following tasks:

* Design (this document)
* Implement required changes to HSDS code (registration and health check logic)
* Create HSDS package
* Implement h5pyd process management code
* Functional testing
* Performance testing
* User documentation



## 4. Future Work

### 4.1 REST VOL

Similar changes could be made to the REST VOL to support sub-process creation of SN and DN nodes by the REST VOL process.  This will enable HDF5 library to support features such as:

* multi-threading
* MWMR
* object storage
* utilization of available cores on the machine

with much less effort than would be required to implement these features in the core HDF5 code base.

### 4.2 POSIX File Support

Since the HSDS code base now supports multiple storage drivers (e.g. AWS S3 and Azure Blob storage), it should be relatively easy to add support for writing data to a regular POSIX directory.  (There is a potential issue that there are no async drivers for POSIX IO, so there may be some peformance implications).

Adding this feature would enable direct access to support writing and reading from local disk without the need of an object storage service (this would be esp. useful for on prem or desktop applications).



## 4. Conclusion

This approach should enable direct access applications with a minimal of code changes from the current HSDS and H5PYD projects.  Up to the limits of the storage system scalability, the direct process model should enable arbitrary large throughput to the HSDS storage schema.