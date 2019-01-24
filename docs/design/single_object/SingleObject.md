# HDF5 as Single Object Design



# Introduction:

The purpose of this feature is to enable read-only access through the REST API to data in HDF5 files stored in S3 Objects.  This will be accomplished by enabling the use of S3 Range GETs whenever dataset data is requested.  Once the chunk (possibly compressed) is fetched from S3 by the DN node, the remainder of the processing (e.g. point selection, query, or hyper slab selection) will operate as it does currently for chunks stored in the native schema.

Chunk locations will be provided at ingest time by using new HDF5 APIs for this information.



## Requirements

- Support both chunked and contiguous HDF5 datasets
- No changes needed for h5pyd or REST VOL 
- Support datasets with large number of chunks per dataset (i.e. millions)
- Support chunks that may be large in size (i.e. 100's of MB)
- Enable virtual aggregation of many HDF5 files into one HSDS domain



## S3 Schema Changes

Currently in the S3 schema datasets have a layout property that consists of the keys, "class" and "dims".   The key "class" is always "H5D_CHUNKED" and the dims give the chunk dimensions.  E.g.:

```
"layout": {
        "class": "H5D_CHUNKED", 
        "dims": [40, 80]
    }
```



For HDF5 as a single object support two addional layouts will be defined: H5D_CONTIGUOUS_REF and H5D_CHUNKED_REF.

### H5D_CONTIGUOUS_REF

When the layout class is H5D_CONTIGUOUS_REF the dataset will support contiguous datasets stored in an external file.   The following keys will be defined for this class type:

- "class": The string "H5D_CONTIGUOUS_REF"
- "dims": List giving the chunk layout of the dataset
- "file_uri": A string giving the s3 URI to the file 
- "offset": Integer giving the offset to the start of the data
- "size": Length of the data



Note: dims must be a chunk layout consistent with the contiguous storage of the array.  I.e. non-leading dimensions equal to the dataset dimensions.

Note: h5path should be a complete S3 URI to the object.  E.g. s3://mybucketname/dir1/myfile.h5

Example:

```
"layout": {
        "class": "H5D_CONTIGUOUS_REF", 
        "dims": [40, 80],
        "file_uri": "s3://mybucket/mylocation/myfile.h5",
        "offset": 12345,
        "size": 12800
    }
```



### H5D_CHUNKED_REF

When the layout is H5D_CHUNKED_REF the dataset will support chunked datasets stored in an external file.  The following keys will be defined for this class type:

- "class": The string "H5D_CHUNKED_REF"
- "dims": List giving the chunk layout of the dataset
- "file_uri": A string giving the s3 URI to the file 
- "chunks": A dict with with keys of the form "i_j_k" where i,j,k give the chunk index (as is ued in the regular schema).  The value of each key shoiuld be a two-element list storing the offset and size of the referenced chunk.  



Note: "dims" must be the same as that used in the external file.

Note: If the number of chunks is very large (e.g. greater than 1000) then the H5D_CHUNKED_INDIRECT_REF layout should be used.



Example:

```
"layout": {
        "class": "H5D_CHUNKED_REF", 
        "dims": [40, 80],
        "file_uri": "s3://mybucket/mylocation/myfile.h5",
        "chunks": {"0_0": [1234, 12800], "0_1": [5678, 128000] }
    }
```



### H5D_CHUNKED_REF_INDIRECT

This layout supports chunked datasets stored in an external file as with H5D_CHUNKED_REF. However H5D_CHUNKED_REF_INDIRECT can supports datasets where the number of chunks is greater than can efficiently be stored using H5D_CHUNKED_REF.  

The following keys will be defined for this class type:

- "class": The string "H5D_CHUNKED_REF_INDIRECT"
- "dims": List giving the chunk layout of the dataset
- "file_uri":  (optional) A string giving the s3 URI to the file
- "chunk_table": A string giving the uuid of an anonymous dataset described below



The chunk_table is a dataset with the same number of dimensions as the target dataset.  The dimensions of the dataset should be sufficient store locations for each chunk of the target dataset.  E.g. if the target dataset has extent: [1000, 1000] and the chunk layout is [100,100], the chunk_table should have dimensions [10,10].

If "file_uri" is defined in the layout, then the type of the chunk_table should be a compound type composed of the fields: "offset" and "length" where offset is an 64bit integer and length is a 32bit integer.  Thus each element of the dataset will provide the location of the chunk data within the file refere3nced by "file_uri".

Alternatively, if "file_uri" is not defined in the layout, the type of the chunk_table will have an additional field: "file_uri" which gives the uri for the refernced file.  This enables HSDS dataset to aggregate data from multiple external files.

Example:

```
"layout": {
        "class": "H5D_CHUNKED_REF_INDIRECT", 
        "dims": [40, 80],
        "file_uri": "s3://mybucket/mylocation/myfile.h5",
        "chunk_table": "d-7fbe2e27-87c5e1d8-f736-a6af0f-4d6950"
    }
```



## HSDS Service changes

The following changes will be made to the HSDS service to support HDF5 single objects.

1. POST /datasets will support creationProperties in the request that define the layouts listed in the above schema.  The HSDS handler will persist these in the dataset JSON.

2. The service will return 405: Method Not Allowed for any write requests to the dataset

3. For read requests to the dataset (GET /datasets/id/value), the SN node will include the file uri, offset, and length in the request to the DN.  The DN in turn will use the information (if supplied) to perform a S3 Range GET on the object.

4. For read requests with the H5D_CHUNKED_REF_INDIRECT layout, the SN node will get elements from the chunk_table as needed, then use these values to make requests to the DN node

   hsload changes  

   



## CLI Changes

The hsload command will support an optional argument giving a file that defines chunk locations and offsets in JSON.  If provided, the tool will use this data when creating datasets with an appropriate layout.

The hsls command will display information about the chunk layout used.

Note: Once h5pyd supports returning information about chunk locations, it will be possible to modify hsload to query this information directly from the file.  Prior to that, a dumper utility will be used to provide the JSON description of chunk loations.