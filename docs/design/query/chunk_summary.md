# Chunk Summary Statistics

------

HSDS supports a query operation for returning elements of a dataset that meet a specific condition.  E.g. elements where the value is greater than 42.  Currently this operation requires the examination of each data element.   If the min/max values for each field for each chunk of a dataset where known, it is possible that many chunks could be skipped, thus speeding up the operation.  This document describes how this can be accomplished.

------

## 1. Introduction

HSDS (https://github.com/HDFGroup/hsds/blob/master/docs/design/hsds_arch/hsds_arch.md) is a web-service written in Python that provides the functionality to read and write HDF data via a REST API (https://github.com/HDFGroup/hdf-rest-api).  The operation used to retrieve values from a dataset: GET/datasets/<id>/value, supports a query parameter that can be used to retrieve elements that meet certain criteria.  For example, if the dataset's type contained the fields "symbol", "date", "open", "close", and only those elements where the symbol was "AAPL" was desired, the request would be:

```
GET /datasets/<dataset_id>/value&query='symbol == AAPL'
```

(This specific query is used in the test case: testSimpleQuery in the file: hsds/tests/integ/query_test.py).

Queries can be based on equality, less than or greater than comparisons.  In addition, query clauses can be combined with boolean operators for AND or OR.  

In any case, the implementation of this operation requires that each chunk of the dataset be loaded and then examined for rows meeting the specification.  Which can be quite slow for datasets with millions or billions of rows.  

This idea of "Chunk Summary Statistics" is to keep track of the min/max values for each field of each allocated chunk of the dataset.  In this way any chunks for which the min/max values would exclude any possible matches for the query can be skipped, thus speeding up the operation.

Note: queries could also be speeded up by creating index(s) of the dataset (e.g. as PyTables supports), but building an index takes some time and uses storage equivalent to the dataset itself.  Since HSDS has the goal of supporting datasets of multi-TB size, we will be focusing on the summary statistics method of speeding up queries for now.

## 2. Design/Architecture

### 2.1 HSDS Changes

To reduce the effort of implementation, we will look at a solution that doesn't require extending the REST API.  Also, there will be no automatic update of the chunk summary as new data is written into the primary dataset.  Instead it will be up to the client to update the chunk summary when the source dataset is modified.

For the primary dataset consider the summary dataset with the following properties:

- Type is compound with fields "min",  "max" and "timestamp".  Timestamp is an unsigned 4-byte float storing seconds in epoch for when the element was updated.  Min and Max will have a sub-type equivalent to the primary dataset type
- Shape is equal to SUP(extent/chunk_extent) in each dimension
- Each element of the summary dataset contains min/max values of the corresponding chunk of the primary dataset
- For elements that correspond to chunks that are not yet allocated, the timestamp value will be 0



With these conditions, the dataset query operation would work as follows:

- For each chunk of the summary dataset, load the chunk into memory
  - For each element in the summary dataset chunk, examine each element
    - If the timestamp field is 0, continue
    - If the element min/max value indicates there are no values meeting the query, continue
    - If the element min/max value indicates there are values meeting the query, examine the values of the primary dataset chunk



For the query operation to use this algorithm the following conditions must be true:

- The attribute "SUMMARY_DATASET_ID" in the primary dataset exists
- A dataset with that id exists  
- The type and shape of the dataset are valid for a summary dataset

If these conditions are not met, the normal query method will be used (examine each element).



As mentioned above, it is up to the client to create a summary dataset meeting these requirements.  If the min/max values do not correctly reflect the actual min/max values of the chunk, the wrong query results will be returned.



### 2.2 Python SDK changes

The h5pyd Table class will provide a "buildSummary" method that creates or updates a summary dataset.

The Table query method will support using the summary dataset if present (this won't require any code changes).

A new CLI tool will be created: hsbuildsummary, that creates or updates tables within a file (domain).