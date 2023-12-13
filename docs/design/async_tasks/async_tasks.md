# Async Tasks

-----

HSDS exposes a REST-based API, and the normal expectation is that any 
request that is sent to the service is handled in a reasonable time frame (e.g. less than 30 seconds).  For requests that need more time to
complete there would be issues with the client connect timing out and
other clients finding the service unresponsive (if say each SN node was
busy performing sort of computationally intensive task).

However, to support the complete gamut of HDF functionality it's necessary to have a mechanism to support such operations.  E.g. the HDF copy operation can be used to make duplicates of an arbitrary large number of datasets where each dataset may contain an arbitrary large of data to be copied.

This design will describe a method of supporting HDF copy or any potentially long running task.  We'll call these "async tasks" since
the operation is not synchronous from the client's perspective.  The client will need to make multiple requests to create the task, get the
status, and so on.

## Introduction

Though not descussed in Roy Fielding's diseratation on Restful services 
(https://ics.uci.edu/~fielding/pubs/dissertation/top.htm), methods to support long running tasks within a RESTful service have been developled.  For example: https://restfulapi.net/rest-api-design-for-long-running-tasks/#:~:text=Best%20Practices%201%20Do%20not%20wait%20for%20long-running,on%20the%20client%20in%20any%20way.%20More%20items.

We'll develop this design pattern within the context of the HSDS architecture.

## REST API Changes

This section covers enhancements to the HDF REST API to support long running tasks.

### POST /tasks

To create a new task the client will send a POST requests to `/tasks`.

The body of requests will be JSON with the following fields:

- operation - the operation to be performed, e.g. "copy"
- src - a list (possibly empty) of data sources to be used.  A data source will consist of a domain specifier, h5path or uuid of the object, and a bucket (unless the default bucket of the service will be used)
- args - a dictionary of arguments to the operation
- des - a list (possible empty) of locations to write the results of the operation

The request will return one of the following status codes:

- 200 - accepted, the response will include a task id for the client to use to inquire on the state of the task
- 400 - bad request, request was malformed (e.g. the operation is not supported)
- 401 - unauthorized, no authorization was provided
- 403 - forbidden, the username does not have read permission on one or more of the src objects, or write/update permission on one or more of the destination objects
- 404 - not found, one or more of the src or des locations doesn't exist
- 409 - conflict, the request is identical to one currently in progress
- 503 - unavailable, the service is currently out of capacity to process the request 

### GET /tasks/id

Once a task has been created, the client can use GET /tasks/id (where id was the task id returned in the POST request) to inquire on the status of the tasks.

On success the response will be JSON, with keys including the taskid, status, created time.  

A 404 will be returned if the task is not found
A 410 will be returned if the task has been deleted

### DELETE /tasks/id

This request can be used to cancel a submitted task.
no task destination resources will be deleted or reset to whatever prior 
state they may of had.

## REST API Example

As an example, let's define a hypothetical operation: "factor", which
as input takes a dataset of integer type and as output writes the prime factors of the dataset to the destination (using a variable length type).

This is the series of requests the client would perform:

1. Create a dataset for the src and initialize it with the numbers to be factored. e.g.: a scalar dataset containing the value 42.
2. Create a dataset to store the results.  In this case it can be a one element integer dataset
3. Call POST /tasks with operation equal to "factor", src  to be domain and guid for the scalar dataset with 42, des to be domain and guid for the result dataset, args to be empty
4. Service returns a 200 response and the client gets a taskid
5. Client makes periodic requests to GET /tasks/taskid until a "complete" task status is returned
6. Client fetches the value of the des dataset and gets the value: (2,3,7)

## Processing /tasks operations in HSDS

The SN (service node), DN (datanode) will both have role to play
in handling task operations.  In addition, a new node type TN (task node) will be defined.

### POST /tasks workflow

The SN handler for POST /tasks will validate request arguments, and authorize the action.  If successful, a new task id (based on a GUID)
will be created.  The SN will then send a POST /tasks request to the DN (which datanode will be based on the GUID value).  The authorization http header sent by the client will be included in the request to the DN.

Each DN will keep in memory a dictonary a list of active tasks assigned to it.
When a POST /tasks is received, the new task will be added to this list.

### GET /tasks/taskid workflow

The SN handler for GET /tasks/taskid will authorize and authenticate the 
request, and then send a GET /task/taskid to the DN identified by the taskid.

The DN handler for GET /tasks/taskid will consult its list of tasks and return a 200 response with the current tasks state, or a 404 if not found
or 410 if deleted.

The DN will also have handlers for GET /tasks and PUT /tasks/taskid
which will be described later.

On recieving the response from the DN, the SN handler will return the response to the client.

### DELETE /tasks/taskid workflow

The SN handler for DELETE /tasks/taskid will authorize and authenticate the request and send a DELETE /task/taskid to the DN.

The DN handler will mark the task as deleted if found and return a 200 to the SN.  If not found or already deleted, the DN will return a 404 or 410.

The SN will return the appropriate status to the client.

### The HSDS Task Node (TN)

In the discussion of processing /tasks requests, we've left out how the 
operation will actually be completed.  This will be the job of the TN, 
a new node type that will function as a task processor.  Like the SN and
DN nodes, the number of TN nodes can be configured at start up (i.e. it's not necsearrily a singleton).

While the SN and DN nodes both function as request/response service processors, the TN will not handle http requests at all.  Rather,
the TN will process one task at a time, or sleep if no tasks can be
found.  The detailed workflow for the TN will be described next.

### TN workflow

On startup the TN will find a task to work on as follows:

1. Send a GET /tasks request to the first DN node
2. The DN node will consult it's list of tasks and return and unassigned task id (if found) to the TN.  It will also mark this task as assigned to the DN and update the task state to "in progress".
3. If a task id is returned, the TN will start work on that task
4. If no task id is returned, the TN will send a request to the next DN node
5. If no tasks has been found after all the DN's have been queired, the TN will sleep for a bit

When working on a task, the TN will send requests to a SN node to read
and write data much as a client would.  The TN will use the authorization header that the DN returned, so that requests from the TN to the SN node will be processed with the same level of privalege as the
client who initated the task has.  When the task is complete, the TN
will send a PUT /task/taskid request to the DN node to update the task state.  It might also be useful to have a PUT /task/taskid with a progress indicator, so that the client can have an indication on how much
longer the task will take.

## Operations to be supported

Potentially any operation that can be specified based on a set of inputs, outputs, and arguments can be implemented.  

As a start, it would be useful to support the HDF copy operation
as defined in the HDF documentation.  This hasn't been implemented in HSDS to date as there wasn't a framework to support it when dealing with 
large datasets.

Another useful set of operations would be equivalents to the Numpy 
Universal functions (ufuncs), or a subset.  These are well documented
(https://numpy.org/doc/stable/reference/ufuncs.html) and would be fairly
easy to implement, especially for ones that could be handled with 
a chunk iterator.

Since the scope of possible operations is so large and in some part,
organization specific, it would be useful to have a plug in architecture
of some sort so that customized sets of operators could be created.  
One approach would be to have docker compose or kubernetes yaml file just use an image that contains the desired operation handlers. The 
inter-dependency between the TN and SN/DN nodes it quite small, since
the TN will use either the standard HDF REST API to talk to the SN, or
the small set of requests to the DN /tasks API to get and update task 
assignments. 

It might be usefl to adopt and approach similar to what The HDF Group 
uses for compression filters: There's a standard set of filters supported
by The HDF Group, a clearinghouse to that is used to identify filters
supported by 3rd parties, and a plugin mechansim that allows 3rd party
filters to be accessed by the library.  The corresponding approach for
task operations would be a set of operations supported by The HDF Group,
a documented set of operations supported by 3rd parties, and the ability 
to build a docker image that supports the standard plus desired 3rd party
set of operations.

## Othe Issues

### Performance

As each TN node will only perform one task at a time, the TN process
will often be in an idle state waiting on I/O operations to complete
(in contrast to how the SN/DNs work using task oriented parallelism).

On the other hand, "the one task at a time approach" simplifies the work flow and enables the use of synchronous APIs (e.g. the TN could
use the h5pyd package rather than the REST API).

To increase the performance of task processing, the number of TN containers could be scaled as needed.  In Kubernetes, some form of dynamic scaling would be possible.

### Security

At least the HDF Group supported operations should not allow arbitrary code as arguments.  Otherwise, it would be challenging to restrict 
HSDS taking harmful actions on the direction of a malicious client.

In environments where the clients are trusted, more latitude could be given - e.g. an operations that says: "run this python code".

### Denial of Service

Even with a baseline set of operations, it would be easy enough for a
client to send a series of requests that use up all the processing capacity of the TNs and limit the ability of other clients to submit tasks.  It might be useful to have a configuable limit on the number of tasks permitted per user.

### Halting problem

Related to the DOS issue, imagine an operation that (intentially or not)
results in the operation never completing.  The effect would be no
new tasks being started, and a potentialy wasteful use of CPU resources.

The should not be an issue with a well defined set of baseline tasks, but
becomes a bigger issue the more latitude you give in terms of what an 
operation could do (e.g. running arbitrary code).

In the nearterm, the best way to deal with this would be some sort of 
operator initiated action to terminate the offending container and 
delete the task.







