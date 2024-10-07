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

Returns: JSON describing map object.  If verbose is set, additional daa will be returned (e.g. number oF KV pairs)

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
  Value for gieven key as JSON or binary blob.  Or 404 if not found

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

This request removes a given key an its associatted value

Request: DELETE /maps/&lt;map_id&gt;/key/&lt;key_value&gt;

Request Elements:
  None

RETURN: HTTP Status code.  404 if key does not exist

### Delete set of keys

This request removes a set of key and their associatted values

Request: DELETE /maps/&lt;map_id&gt;/keys

Request Elements:
  None

RETURN: HTTP Status code.  404 if key does not exist

### Attribute methods

  GET, PUT, DELETE requests for attribute operations.  Follow same schema as attributes for other objects (Groups, Datasets, Commited Data Types)


## 2. Design/Architecture

TBD