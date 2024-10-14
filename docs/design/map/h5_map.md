# Map Support

------

This document outlines a design for map support in HSDS.

------

## 1. Introduction

The HDF5 library H5M interface adds supports for persistent maps using the HDF5 API.  Though currently only the DAOS VOL connector supports this interface, by adding support for maps using a restful interface, useful functionality will be added to HSDS and increase compatibility with current and future versions of HDF5.

Goals:

* Add RESTful methods to HDF Rest API that support map related methods
* Support maps with an arbitrary number of K,V pairs
* Enable persistence (to S3 or Posix) that is efficient in terms of storage size
* Provide good performance
* Compatibility with common dataset methods (e.g. compression)
* Allow natural extension of the REST VOL to HSDS methods to support H5M interface

The proposed mapping opertions are similar in concept to operations on attributes or links (both of which can be viewed as a map).  The primary difference
is in terms of implementation.  In HSDS, links and attributes are stored as JSON objects.  As the number of links or attributes increased problems would arise
wheen there wasn't sufficient memory to decode and encode the set of objects.  Also, the decode/encode operations would eventually become quite costly in terms of performance (akin to storing a large dataset as CSV).  This design for map support will avoid these issues via the use of chunking and binary representation of the map data (the later is made easier due to the fact that map values in HDF5 share a common type).


## 2. HDF REST Extensions

In this section additions to the HDF REST API will be examined.  

### Create new map

This request creates a new map object

Request: POST /maps

Request Elements:

* key_type: data type for map key
* value_type: data type for value key
* creation_properties: creation props (compatible with dataset creation properties, e.g. compression)

Returns: id for object

### Get map metadata

This request returns information on a map object

Request: GET /maps/&lt;map_id&gt;

Request Elements:

* verbose: provide additional information in the respose

Returns: JSON describing map object.  If verbose is set, additional daa will be returned (e.g. number of KV pairs)

### Delete a map

This request deletes a map. 

Request DELETE /maps/&lt;map_id&gt;

Request Elements: 
  none

Returns: HTTP Status Code

### Add a key

This request adds a key to a map

Request PUT /maps/&lt;map_id&gt;/key/&lt;key&gt;

Request Elements:
  value: value for the given key

### Get Value for given key

This request returns value for given key

Request GET /maps/&lt;map_id&gt;/key/&lt;key&gt;

Request Elements:
  None

Returns:
  Value for given key as JSON or binary blob.  Or 404 if not found

TBD: Support returning 410 (Gone) for recently removed keys

### Get values for a set of keys

Request POST /maps/&lt;map_id&gt;/keys

Request Elements:
  Body as a list of keys

Returns:
   JSON map of key value pairs

### Get all key/value pairs

Returns all key/value pairs for give map object

Request: /maps/&lt;map_id&gt;/keys

Request Elements:
   Limit If provided, a positive integer value specifying the maximum number of key/value pairs to return.

Marker If provided, a key indicating that only keys that occur after the marker key will be returned.

Returns: JSON of key value pairs.  Up to an internal limit.

TBD: support binary responses

### Delete a key

This request removes a given key and its associated value

Request: DELETE /maps/&lt;map_id&gt;/key/&lt;key_value&gt;

Request Elements:
  None

RETURN: HTTP Status code.  404 if key does not exist

### Delete set of keys

This request removes a set of key and their associated values

Request: DELETE /maps/&lt;map_id&gt;/keys

Request Elements:
  None

RETURN: HTTP Status code.  404 if key does not exist

### Attribute methods

  GET, PUT, DELETE requests for attribute operations.  Follow same schema as attributes for other objects (Groups, Datasets, Commited Data Types)


## 2. Design/Architecture

The datatype for a map object will be a numpy dtype compound of the datatypes for the key and value.

Map object data will be stored as binary images of numpy arrays with the above type (similar to how dataset 
chunks are stored currently).  Since the data will be mostly sparse, it will make sense for some type of compression to be 
used by default.   Data will be chunked with 2-4 MiB chunk sizes.  Number of chunks will automatically increase as keys are added
to the map.

### Control Flow for PUT operations

On write (PUT with key and value), the SN will get a list of allocated chunks (for performance reasons it will be useful to keep a 
cache of chunk ids and only update as chunks are added to the map).  If no chunk has been allocated (e.g. first write to the map),
a chunk id will be allocated.

For each chunk_id, the SN will do a PUT to the DN that owns that chunk id while a 507 (no storage) status is returned (see below).
If a 507 is returned for each chunk id, a new chunk id will be allocated, and the request sent to the DN that owns that chunk.
Finally the status (200, 201, or 410) will be returned to the client.

For the DN, when a PUT request is received, it will first fetch the chunk.  If the chunk is not found (i.e. this is the 
first write to the chunk), the DN will create a new numpy array in memory.  Next the key will be hashed and the result used to
find the row for the given chunk.  

The next step depends on the existing state of the row:

* The row is empty, the unhashed key and value will be written to the row.  The chunk is then marked dirty to be lazily written to storage
* The row is occupied and the key value is different from the incoming key, the DN returns a 507 (Storage unavailable) to the SN
* The row is occupied and the key value is the same as the incoming key and the value is the same as the incoming value, the DN returns 200
* The row is occupied and the key value is the same, while the value is different, the DN returns 409 (Conflict to the SN)

### Control flow for GET operations

On read (GET with key), the SN will get a lst of allocated chunks.  The SN will then send async requests to each DN that owns one or more
chunks with the chunks ids and key.  If any DN returns 200, then the value returned by the DN will be returned to the client.  If all DNs return 404 (Not Found), a 404 will be returned the client.

For the DN, for each chunkid the chunk will be fetched, and the key hash will be used to determine which row in the chunk should be examined.

As with PUT, the next step depends on the existing state of the row:

* The row is empty, a 404 is returned
* The row is occupied and the key value is different from the incoming key, a 404 is returned
* The row is occupied and the key value is the same as the incoming key the value for the row is returned with a 200 response

### Control flow for DELETE operations

On remove operations (DELETE with a key), the SN will get a list of allocated chunks.  The DN will then send async delete requests to each DN that owns one or more chunks.  If any DN returns 200, then a 200 will be returned to the client.  If all DNS return a 404, then a 404 will be returned to the client.  (TBD: support 410, Gone responses).


For the DN, for each chunkid the chunk will be fetched, and the key hash will be used to determine which row in the chunk should be examind.

Again the next step depends on the existing state of the row:

* The row is empty, a 404 is returned
* The row is occupied and the key value is different from the incoming key, a 404 is returned
* The row is occupied and the key value is the same as the incoming key, the row is zero'd out (making it available for new writes).  A 200 response is returned

If after a deletion, all the rows in a chunk are empty, the chunk will be deleted.

## Performance and storage considerations

Given latency involved in server requests, it will be much more efficient to read or write multiple keys in one request.

As the number of chunks increases, all DNs will see an increase in CPU load (and throughput if the chunks are not already cached in memory). SN latency should not increase as much as all DN are searching the chunks in parallel.

Write operations are more sensitive to increases in the number of chunks since each chunk much be searched sequentially till an
open slot is found.

The number of chunks vs the number of keys will be determined by the chunk size and frequency of hash collisions.  
Assuming compression is used, the total storage size will be linearly porpotional the number of keys.