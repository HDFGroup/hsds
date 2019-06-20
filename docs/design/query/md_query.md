# Multi-Domain Queries

------

HSDS currently supports query operations to retrieve values from a given dataset, but often times it would be desirable to query across different datasets in a domain or across multiple domains.  The queries should support logical expressions based on the domain name and path, attribute values, dataset paths, and dataset values.  The implementation of these operations should be able to support queries across 1000's of domains, 1000's of objects per domain, and large datasets (>1GB) efficiently.  This document describes how the HDF REST API could be extended to support this and how the functionality could be implemented.

------

## 1. Introduction

HSDS (https://github.com/HDFGroup/hsds/blob/master/docs/design/hsds_arch/hsds_arch.md) is a web-service written in Python that provides the functionality to read and write HDF data via a REST API (https://github.com/HDFGroup/hdf-rest-api).  The API includes method for domain (the HSDS equivalent of a file) creation/update/delete, object (Group, Dataset, Comitted Datatype) creation/update/delete, value hyper slab and point selection, and other operations that mirror the functionality of the HD5 library. 

There are three types of "queries" currently supported: domain queries, h5path queries, and dataset value queries.  It would be desirable for multi-domain quieres to extend the current syntax as far as possible.



### Domain Queries

This request returns a set of domains within a given folder.

Request: /domains

Params:

* domain: folder to search from
* Limit:  (optional) limit the number of results to given value
* Marker: (optional) return results after thee Marker value (used for pagination)

Returns: JSON list of domain name

Note: Recursive search of subfolders is not currently supported so the number of results is limited to the number of domains/sub-folders in a given folder.

Note: There is currently no way to filter the results based on domain name, creation/update time, attribute values, etc.

### H5Path Queries

This returns an object matching the given h5path.

Request: /datasets/ (or /groups/ to return matching groups or /datatypes/ to return matching datatypes)

Params:

* domain: domain to search in
* h5path: the h5path to use (e.g. "/g1/g1.1/")

Returns: JSON representation of the object found in the given path or None if no object exist

Note: No wildcards or regular expressions are supported in the h5path param, so only 0 or 1 results can be returned.

### Value Queries

This returns dataset elements and indexes that match the query string.

Request: /datasets/{uuid}/value

Params:

* domain: domain to search in
* query: query string (based on bumpy.where syntax: https://docs.scipy.org/doc/numpy/reference/generated/numpy.where.html)
* Limit: Limit the number of rows returned
* selection: Constrain search to given hyperslab bounding box 

Note: Currently only one-dimensional datasets of compound type or supported.

Note: There is a releated proposal to speed up value queries for large datasets.  See: https://github.com/HDFGroup/hsds/blob/master/docs/design/query/chunk_summary.md.  

Example: if the dataset's type contained the fields "symbol", "date", "open", "close", and only those elements where the symbol was "AAPL" was desired, the request would be:

```
GET /datasets/<dataset_id>/value?query='symbol == AAPL'
```

Queries can be based on equality, less than or greater than comparisons. In addition, query clauses can be combined with boolean operators for AND or OR.



## 2. Design/Architecture

As a first step at least, it seems more practical to extend the domain query operation for additional criteria   based on recursive search, regex on domain names, creation/update times, and attribute values, but not factor in h5paths, or dataset values.  For user queries where dataset values are needed a two or three step process could be used: 

1. Return candidate domains
2. Filter candidate domains based on objects matching h5path regex
3. Filter remaining candidates based on dataset values

For clients using ther Python SDK (h5pyd), h5pyd could wrap these steps to reduce the burden on the end user.  

### 2.1 Domain Query Extension

The following additional parameters are proposed:

1. recursive: if present all sub-folders from the parent folder are searched
2. name: if present only results where the domain name matches the name regex are returned
3. create_gt: if present only results where the domain create time is greater than the given value are returned
4. create_lt: if present only results where the domain create time is less than the given value are returned
5. attr_query: if present only domains where the root attributes match the boolean expression are returned (see example)

Example: if only domains where the root attributes: "attr1" and "attr2" exist and the value of attr1 should be "happy" and the value of attr2 should be >42, open",  the request would be:

```
GET /domains?domain='/start/here/'&attr_query='attr1==happy && attr2>42'
```

Queries can be based on equality, less than or greater than comparisons. In addition, query clauses can be combined with boolean operators for AND or OR.



Note: attr_query is assumes that an attribute is a simple scalar value.  Unclear how this could be applied to multi-dimensional attributes.

### 2.2 Path Query Extension

The following additional parameters are proposed:

1. use_regex: If present, the h5path parameter is treated as a regex expression and only those datasets (groups) whose paths match that expression are returned.
2. attr_query: if present only objects where its attributes match the boolean expression are returned 

Note: In general with HDF5 it's possible for multiple paths to refer to the same object, so the result may miss objects where an alternative path exists.  For consistency, this operation should only be used where the domain is setup in such as way that only one path exsits for each object.

