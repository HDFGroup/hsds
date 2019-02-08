# HSDS Operators

------

HSDS uses parallelization to speed up point, hyper slab, and query operations.  However, it is a quite common pattern where users need compute a simple reduction function over a large dataset (or large dataset selection).  E.g. Currently computing the sum of a large dataset, requires the client to read all values from the dataset into memory (or read pages if the entire dataset is too large to fit into memory), to compute the sum.  By providing REST operations that return the result of basic reduction functions, less data movement will be needed and simple clients (i.e. non-parallel) will be able to efficiently get results for large datasets.

------

## 1. Introduction

In HSDS (<https://github.com/HDFGroup/hsds/blob/master/docs/design/hsds_arch/hsds_arch.md>) read and write operations are speeded up by utilizing multiple nodes to process the chunks involved in the selection.  By extending the REST API to support common reduction functions, parallelization will also be enabled by utililizing multiple nodes in the reduction calculation.  Unlink with dataset reads however, only a minimal amount of data will need to returned back to the client.

## 2. Design/Architecture

### 2.1 REST API changes

The model for the operators that will initially be supported are based on the NumPy reduction functions (e.g. https://docs.scipy.org/doc/numpy-1.15.1/reference/generated/numpy.sum.html), and a SQL-like count function.

- min - minimum value
- max - maximum value
- mean - mean value
- argmin - index of minimum value
- argmax - index of maximum value
- sum - summation of all values
- std - standard deviation
- count - number of elements in the selection (and that meet the query specification if applicable)



Note: sum, mean, and std won't apply for string types.  Min, max, argmin, and argmax will be based on lexicographical (or use alphabetical? TBD) sorting of the strings.



The existing specification of the GET value operation (https://github.com/HDFGroup/hdf-rest-api/blob/master/DatasetOps/GET_Value.rst) will be extended to support two additional query parameters:

- operator:  Either a string or a list of string.  If a string the sting must be one of  "min", "max", "mean", "argmin", "argmax", "sum", or "std".  If a list, the list must consists of strings using the above values
- axis: An integer value between 0 and the rank of the dataset or a list of ints.  If axis is an int, the operator method will compute values along all elements of the indicated axis. If axis is a tuple of ints, a sum is performed on all of the axes specified in the tuple instead of a single axis or all the axes as before.  If not provided, the operator will act on all elements of the dataset.

The return value will be an array with the same shape as the selection shape, with the specified axis removed. If *a* is a 0-d array, or if *axis* is None, a scalar is returned. If an output array is specified, a reference to *out* is returned.  If multiple operators are used, the value of each element of the array will be a list (if a JSON response is requested), or a binary array (if a binary response) is requested, where the number of elements is equal to the number of operators.

Note: if a "select" query parameter is provided, the operation will be performed over the indicated selection, not the entire dataset.



### 2.2 HSDS Changes

In the SN node, for the GET value operation, if an operator is present in the query parameter, the parameter will be passed in each request to the DN nodes.   

When the DN node gets a operator, it will fetch the chunk from storage or retrieve from cache as a numpy array in the usual way.  However, instead of returning the numpy array selection to the SN node, it will call the corresponding numpy method for each operator present.  The resulting values will then be returned to the SN node.  The return type will be equivalent to what is used in the numpy methods (e.g. extended precision float for returning mean on a integer or floating point type).

Upon receiving all results from the DN nodes, the SN node will then need to combine these results for returning to the client.   For some operations (e.g. sum) this reduction is straightforward.  For others such as mean or std, the SN will need to account for the number of elements scanned by each DN node to calculate the correct value.  

When the number of chunks needed to read exceed the max_chunks_per_request a 413 error will be returned as is the case currently for GET value operations.  In this case, the client will need to paginate through the entire dataset.

Note: Using Lambda functions on AWS would be particular effective way to handler larger number of chunks as only the results need to be returned, not all values of the dataset (which would overload the SN memory anyway).

### 2.3 Python SDK Changes

The h5pyd Dataset class will be extended with new methods equivalent to the numpy reduction methods (min, max, std, etc).  The implementation of these methods will use the REST API extensions to retrieve  the correct values.

If a 413 error is returned, the Dataset methods will need to paginate across smaller selection (as is done now with query operations for example).

Note: This approach is somewhat inefficient when two or more reductions need to be computed, say min and max.  For large datasets (that overwhelm the DN chunk caches), most chunks would end up being read twice.