# HSDS Storage Direct Access

------

HSDS storages data in a sharded fashion for efficient use with object storage.  Currently the only method to read or write data in that format is via the server itself.  In many circumstances it would be desirable for client software to directly read or write data without needing to utilize a server.  This document defines how this could be accomplished with minimal effort.

------

## 1. Introduction

HSDS (https://github.com/HDFGroup/hsds/blob/master/docs/design/hsds_arch/hsds_arch.md) is a web-service written in Python that provides the functionality to read and write HDF data via a REST API (https://github.com/HDFGroup/hdf-rest-api). HSDS runs as a set of containers managed either by Docker Engine (https://www.docker.com/products/docker-engine) for installations on a single machine, or Kubernetes (https://github.com/kubernetes/kubernetes) for installations on a cluster.

There are two aspects of the HSDS architecture that are key to good performance (and which we would want to carry forward in a direct acccess mode): 

1. Multi-process: HSDS nodes run as a set of processes (possible across multiple machines) to support both request scaling, and to speed up request processing (primarily by reading/writing chunk data in parallel)
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

Restructing the current HSDS code base as a Python package for client use would be a fairly large task.  Since the code makes heavy use of async functions, client code would then be forced to adopt an ascync mode would would be a challenge for existing applications.  

A simpler appoach would be to extend the current h5pyd package to optionally support direct access in addition to sending requests to the server.  This can be accomplished by having h5pyd use a multiprocessing model - when a file is opened in h5pyd, it would launch processess that would be equivalent of the HSDS SN and DN containers.  Once the process are running, h5pyd would use http requests as it would with a remote server (but just using localhost connections).  When the file is closed, the processes would be terminated.

### 2.1 Authentication

### 2.2 Port Assignments

 Port assignments would  be  dynamic to avoid port collisions with other processes on the system and the host assignment would be localhost.  

Rather than running a head node, the client process would assume responsiblity for synchronizing the SN and DN processes

### 2.3 Docker vs Process model

The HSDS service uses Docker (or equivantlely Kubernetes) to:

1. Provide a standard image to run
2. Shield users from requiring speicific Python version and packages be setup
3. Restart logic to handle crashes
4. Logging management
5. Network mapping

Since we don't want to require Docker be installed for direct access, each of these functions would need  to be supported directly.



## 3. Implementation

TBD - descirbe how this will be implemented

## 4. Conclusion

This approach should enable direct access applications with a minimal of code changes from the current HSDS and H5PYD projects.  Up to the limits of the storage system scalability, the direct process model should enable arbitrary large throughput to the HSDS storage schema.