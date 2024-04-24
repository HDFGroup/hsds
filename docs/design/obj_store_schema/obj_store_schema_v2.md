# Object Storage Schema for HDF5

_John Readey - The HDF Group_

[TOC]

## Intro

This document describes the object storage schema - version 2, used by the Highly Scalable Data Storage service (hsds).
The object storage schema describes a mapping from the HDF5 data model of groups, datasets, committed types to a set of storage
objects that would be suitable for use in an object storage system and as the persistent storage layer for hsds.

## Goals

The following goals were a factor in the design of the object storage schema:

1. The schema should be able to represent traditional HDF5 files at a high level of fidelity
2. The schema should not introduce any performance barriers in the design of hsds
3. The schema should result in a cost-effective system for public cloud implementations (both in terms of overall storage size and request api pricing)
4. The schema should be adaptable to other object storage systems (e.g. OpenStack/Swift)
5. The schema should be usable with other services or applications that desire to represent the HDF5 data model as a set of storage objects

## Object Storage

This section will provide some over context regarding object storage system.

### Object storage background

An object storage is a storage architecture that manages data as objects rather than files, or blocks.

Compared to other storage technologies object storage has the advantages of:

1. Scalability - It's easier to build out large storage solutions (e.g. AWS S3 manages trillions of objects)
2. Throughput - Potentially greater aggregate throughput (scales with the number of storage nodes)
3. Reliability - Objects are automatically replicated to avoid potential data loss
4. Cost-effective - easier to scale out. Vendors like AWS provide low-cost, pay-as-you go services

The target object storage system for the object storage schema is AWS Simple Storage Service (AWS S3), but in general the schema should be adaptable to other object storage systems such as Google Cloud Storage, Ceph, or OpenStack/Swift.

Object storage systems share some common properties:

1. Objects stored in the system are identified by a key (a string value)
2. The object storage system maintains metadata about each object (see next section)
3. They are not natively POSIX compliant (e.g. there is no way to append to a storage object)
4. Objects are transparently (to the client) replicated in multiple storage devices for redundancy
5. Latency is high compared to storage on local disk, but aggregate throughput is potentially much higher
6. Keys and Objects live in a "bucket". Permissions to access objects are defined at the bucket level

## Object Metadata

In an object storage system each object has data, a key, and metadata (a set of properties that pertain to the object). The metadata can be system or user defined. The later is typically limited to a fairly small size (2KB in the case of AWS S3).

For the purposes of this document, the following metadata properties (as defined for AWS S3) are relevant to the schema design:

1. Content-Length - the size of the object in bytes
2. Content-MD5 - a checksum of the object data
3. Last-Modified - the time at which the object was last modified (or created, whichever is later)

In addition, the object storage schema will use define and use some custom metadata properties such as Compression-State for chunk objects.

## Object Storage system assumptions

The following constraints and assumptions are given as the basis of the schema design (some of which may need to be re-evaluated for use in non-S3 systems):

1. Object keys are limited to 1024 characters
2. The use of many small objects would be prohibitive from a cost perspective (API Request Pricing)
3. The use of very large objects (e.g. >100MB) would introduce excessive latency
4. The storage system is not read-write consistent
5. The aggregate throughput of the storage system would not be expected to limit hsds scalability
6. All objects managed by hsds will exist in one "bucket", the hsds service will have read-write authority for the given bucket
7. All updates to the objects will be through the hsds service
8. Updates to a storage object are complete (i.e. the entire object is overwritten), atomic (i.e. last writer wins), and either succeed or fail with no update to the object
9. There is no practical limit to the number of objects that can be stored in a bucket
10. The object storage system does not provide support for "transactions" (i.e. "all or nothing" update of two or more objects)

## The HDF5 data model

The following is a brief review of the HDF5 data model as it relates to the schema design (see the HDF5 docs for a more complete description).

HDF5 data model consist of the following primitives:

1. Group - an object that manages a set of attributes and links. A designated group "root" serves as the base of the hierachy.
2. Attribute - a "small" named data item that consist of a dataspace, type description, and data
3. Link - a named reference to another HDF5 object (hard link for links within the file, as well as Soft and External links)
4. Dataset - a data container that consists of a dataspace, type description, attributes, and other properties (e.g. chunk layout, fill value, compression filters, etc.)
5. Committed Type - a sharable type object (that also has a set of attributes)

This document will describe how each of these entities can be stored in a format suitable for object storage systems.

As traditionally used with the HDF5 library, these objects are stored within a POSIX file.
By contrast the object storage schema stores each HDF object as an object storage object (a "sharded" representation) and includes a "domain" object serves as a stand-in for HDF5 files.

The goal of the object schema is to be of sufficient fidelity that it should be possible to convert a traditional HDF5 file to a set of objects, and then convert the set of objects to a HDF5 file that is equivalent to the original file.

## Comparison of managing HDF5 entities in a file vs. an object store

---

Management of HDF5 entities in an object store brings up a different set of considerations when compared with managing entities within an HDF5 file:

1. The object storage system is itself an efficient key-value store, so there is no need for internal data structures such as btrees
2. Management of "free space" within a file is not an issue when using an object store
3. The object storage system doesn't provide the equivalent of an append operation, so the entire object must be re-written for each write (though partial reads are supported)
4. Performance is sensitive to the size of objects in the object store (c.f. <http://improve.dk/pushing-the-limits-of-amazon-s3-upload-performance/>)
5. Given that writes to the object store are atomic, there is no possibility that the storage system will be left in an inconsistent state
6. Certain functions that are typically performed by the filesystem (e.g. listing files, file permissions) we need to be managed by the service (e.g. there needs to be the ability to store the access rights for a given object)
7. Unlike HDF5 entities in a file, the "file" an object store object is contained in is not immediately apparent. The connection between objects and the "file" they are contained in needs to be explicitly managed.

## Additions to the HDF5 data model to support the HDF REST API

Several additions to the HDF5 data model have been made in order to support the HDF REST API. In the reference implementation of the HDF REST API (h5serv), these additions were stored in a hidden group within the traditional HDF5 file managed by the service. In hsds, these additions can be directly modeled by the schema.

These additions are described in the sub-sections below.

### UUID

Each high-level object (group, dataset, committed type) can be identified by a UUID - a 36 character alphanumeric identifier. E.g.: "b03b24ef-69f244b6-acd9-4df97b-37122a". The UUID's used in the object storage schema add a two-character prefix to the id to identify the type of object:

- "g-": a group id
- "d-": a dataset id
- "t-": a type id

For example, the id used for a group object with the above UUID would be:

    g-b03b24ef-69f244b6-acd9-4df97b-37122a

All objects within the same domain will have characters 2-19 in common. For exmample, this would be a valid id for a dataset
within the same domain:

    d-b03b24ef-69f244b6-56e5-25125a-89ba79

The id for the root group uses the same layout, but the second half of the id is based on the first half. A root group id formed by
taking a random 16 character hex string and rotating each character by 8 to form the next 16 characters (exclusive of the hyphens). For
example the root group for the two ids above would be:

    g-b03b24ef-69f244b6-38b3-ac67e1-7acc3e

Here we have:

- 'b' -> '3'
- '0' -> '8'
- '3' -> 'b'

And so on. The layout allows us to find the root id for any object give just that objects id. If the root id of an object is the same
as the object's id, it follows that the object must be a root group.

This convention gives us 2^64 possible domains (i.e. unique root ids) and each domain can have up to 2^64 possible objects. Given the
large address space, it is possible for new ids to be created by a randomized process with small risk of collision with an existing id.

Ids used in the version 2 schema can be identified by how hyphens are used to break up the hex characters. In version 1, all ids had
the folloing pattern:

    g-0568d8c5-a77e-11e4-9f7a-3c15c2da029e

That is,

- &lt;class_identifer&gt;-&lt;hex8&gt;-&lt;hex4&gt;-&lt;hex4&gt;-&lt;hex12&gt; for scheama v1
- &lt;class_identifier&gt;-&lt;hex8&gt;-&lt;hex8&gt;-&lt;hex4&gt;-&lt;hex6&gt;-&lt;hex6&gt; for schema v2

### ACL

Each high-level object can maintain an ACCESS Control List that describes the default and user-specific access permissions for that object (see: <http://h5serv.readthedocs.io/en/latest/AclOps/index.html>).

### Timestamp

Each high-level object has timestamps for create time and last updated time, that can be retrieved using the REST API.

## Schema Description

The object schema defines the storage for the following entities:

- domains (roughly equivalent to an HDF5 file)
- groups
- committed type
- datasets
- chunks

Note: attributes and links are stored as a component of their parent object.

Note: all strings used in the schema (e.g. link names) are UTF8 encoded unicode strings. Strings stored in a dataset will be encoded based on the type description of the dataset.

## Domains

The domain entity is similar to traditional HDF5 files in that they are containers for related collections of resources. Unlike a file however, the related resources for a domain aren't contained within the domain object, but are persisted as other objects within the bucket. The domain object contains a "root" key that can be used to retrieve the root group of the given domain. From the root group other entities in the domain can be retrieved by traversing the directed graph anchored at the root group.

### Domain key

Domain keys end with "/.domain.json" and can have an arbitrary prefix. Unlike other entities in the object storage schema, domain keys are stored hierarchically (as with files in a file system), delimited using the '/' character. This enables domain keys to be listed by prefix and provides a canonical key for the parent of a domain.

For example, the domain key:

    /home/test_user1/my_domain/.domain.json

Would have a parent domain of:

    /home/test_user1/.domain.json

Sub-domains of the domain could be found by listing all keys with the prefix of:

    /home/test_user1/my_domain/

### Domain Specification

The domain object contains JSON with the following keys:

- "acls" - Access Control List (user permissions) for actions on domain. See below for subkeys.
- "owner" - Username of the owner (user who initially created the domain)
- "root" - the UUID (not including the md5 hash) of the root group in the domain
- "created" - the timestamp for when the domain was created
- "lastModified" - the timestamp for when the domain was last updated

The "owner" and "acls" keys are required, others may not be present. In particular, if the "root" key is not present, that implies there is no HDF collection associated with this domain. In this case the domain object can serve as a sort of "directory" for a set of related sub-domains.

Notes:

- The service layer may impose a policy where domains can only be created if there is an existing domain with the requisite permission ACLs for the requesting user. One or more "top-level" domains (e.g. "/home") would be created outside the service API (e.g. by an administrator with permissions to create objects in the bucket directly).
- The owner and root keys can be assumed to be immutable (i.e. these values can be cached)
- Metadata about the owner (and other usernames referenced in this schema) are assumed to be stored in another system (such as NASA URS)
- The "root" key is optional. If not present, the domain doesn't have an associated root group (but can serve as a place-holder for sub-domains)

### Domain object example

Key:

    /home/test_user1/my_domain/.domain.json

Object:

```json
{
    "acls": {
        "default": {
            "create": false,
            "read": true,
            "update": false,
            "delete": false,
            "readACL": false,
            "updateACL": false
        },
        "test_user1": {
            "create": true,
            "read": true,
            "update": true,
            "delete": true,
            "readACL": true,
            "updateACL": true
        }
    },
    "root": "g-cf4f3baa-956e-11e6-8319-0242ac110005",
    "owner": "test_user1",
    "created": 1479168471.038638,
    "lastModified": 1479168471.038638
}
```

### Domain ACLs

The "acls" key in the domain object provides a method to denote user access rights to objects within the domain.
The service layer may enforce a policy to use acls key to authorize or deny request to perform specific actions by a given user on objects within the domain.

The ACL consist of a key-value collection where the key denotes the username for the given user. One special key is defined: "default". This key defines the permission for any username that is not otherwise listed.

Within the username key there are six required sub-keys that each have a value of true or false:

- "create" - If true, the user has permission to create new objects, links, and attributes wihin the domain
- "read" - If true, the user has permission to read from any object in the domain
- "update" - If true, the user has permission modify dataset values and extend datasets
- "delete" - If true, the user has permission to delete any object in the domain (or the domain itself)
- "readACL" - If true, the user has permission to read any ACL in the domain
- "updateACL" - If true, the has permission to modify the ACL (including adding additional usernames)

Note: optionally, an ACL key can be used in a group, dataset, or committed datatype object. If an ACL is present, it can be used to enforce permissions for that object. If not present, the domain ACL is used as described above.

Example: Using the ACLs defined for the "my_domain" object above, user "test_user1" would be authorized to make any change to objects in the domain, or change the ACL itself. User "joebob" (not listed in the ACL keys), would have permission to perform any read operation (assuming a more restrictive ACL is not present in the requested object), but not have authority to modify or delete any object.

## Group object

In the HDF data model, the group object is used to organize collections of other groups and datasets by describing a set of links (either hard, soft, or external).
In the object store schema, the links contain just information about the link itself, not the linked object. The group object may also contain a collection of attributes.

### Group key

The group object storage key is of the form:

    /db/<uuid1>/g/<uuid2>.group.json

Where &lt;uuid1&gt; is formed from the first 16 hex characters of the object's id and &lt;uuid2&gt; is formed from the last 16 hex characters of the id.

For example, if the object id is:

    g-b03b24ef-69f244b6-acd9-4df97b-37122a

The storage key would be:

    /db/b03b24ef-69f244b6/g/acd9-4df97b-37122a/.group.json

This storage key is used to store and retrieve the given object.

Since all groups within a given domain would have a prefix starting with:

    /db/b03b24ef-69f244b6/g/

This facilitates listing all the groups for the domain.

In schema v1, a hash prefix was added to the front of the key to randomize the key ordering, but an enhancement of the AWS Simple
Storage Service has rendered this unnecessary. See: <https://aws.amazon.com/about-aws/whats-new/2018/07/amazon-s3-announces-increased-request-rate-performance/>.

### Group Specification

The Group object consists of JSON with the following keys:

- "id" - the id of the group ("g-&lt;uuid&gt;")
- "attributes" - a key/value collection of group attributes
- "links" - a key/value collection of links
- "created" - timestamp (since epoch) of when the group was created
- "lastModified" - timestamp of when the group was last modified
- "root" - the id of the root group in the domain

There are three types of links that are supported: Hard, Soft, and External. Each link item is a JSON object with the following keys:

- "class" - the type of link. Must be one of the values: "H5L_TYPE_HARD", "H5L_TYPE_SOFT", or "H5L_TYPE_EXTERNAL"
- "created" - timestamp of when the link was created
- "id" - for hard links, the id value is the id of the dataset or group the link points to
- "h5path" - for soft or external links, this is a string that gives the HDF5 path where the object is expected to be found
- "domain" - for external links, this is a string that gives the domain which the linked object is a member of

Notes:

- the attributes collection keys consist of the attribute names. See "Attributes" for a description of the object schema for attributes
- The "id", "root", and "domain" keys can be assumed to be immutable

TBD:

- A group that contains a large number (roughly &gt; 100K or more) of links or attributes, may present problems when accessed. If a single storage object is very large, there will be excessive latency in retrieving the object from the object store. Also applications loading a large JSON string may consume an excessive amount of memory. To address this, one possibility would be to shard such large groups into multiple storage objects.

### Group object example

Key:

    db/b03b24ef-69f244b6/g/acd9-4df97b-37122a/.group.json

Object:

```json
{
    "id": "g-b03b24ef-69f244b6-acd9-4df97b-37122a",
    "root": "g-b03b24ef-69f244b6-38b3-ac67e1-7acc3e",
    "created": 1543359860.1245284,
    "lastModified": 1543359861.9263768,
    "attributes": {},
    "links": {
        "dset1.1": {
            "created": 1543359890.084772,
            "id": "d-b03b24ef-69f244b6-acd9-4df97b-37122a",
            "class": "H5L_TYPE_HARD"
        },
        "slink": {
            "created": 1543359890.034954,
            "h5path": "/g2/g2.1/dset2.1.1",
            "class": "H5L_TYPE_SOFT"
        },
        "extlink": {
            "created": 1543359890.035682,
            "h5path": "/a_group/a_dset",
            "domain": "/home/test_user2/another_domain",
            "class": "H5L_TYPE_EXTERNAL"
        },
    }
}
```

## Committed Type Object

In the HDF data model the committed type object is used to provide types that can be shared among datasets and attributes. The committed type may contain attributes. The object store schema provides keys that describe the type as well as a key/value collection for attributes.

### Committed Type Key

The committed type object storage key is of the form:

    db/<uuid1>/t/<uuid2>/.datatype.json

Where &lt;uuid1&gt; is formed from the first 16 hex characters of the object's id and &lt;uuid2&gt; is formed from the last 16 hex characters of the id.

### Committed Type Specification

The Committed type storage schema consists of JSON with the following keys:

- "id" - the id of the committed type ("t-&lt;uuid&gt;")
- "type" - a JSON object (or string for primitive types) representing the type
- "attributes" - a key/value collection of group attributes
- "created" - timestamp (seconds since epoch) of when the committed type was created
- "lastModified" - timestamp (seconds since epoch) of when the committed type was modified
- "root" - the id of the root group in the domain

Notes:

- See "Attributes" for a description of the object schema for attributes
- See "Links" for a description of the object schema for links
- See "Types" for a description of the object schema for type
- The "id", "root", "domain", and "type" keys can be assumed to be immutable

### Committed Type Example

Key:

    db/8b0daca7-67ce884d/t/685b-bafe46-1cf516/.datatype.json

Object:

```json
{
    "id": "t-8b0daca7-67ce884d-685b-bafe46-1cf516",
    "root": "g-8b0daca7-67ce884d-0385-242fef-4600c5",
    "created": 1543363027.421313,
    "lastModified": 1543363027.421313,
    "type": {
        "class": "H5T_COMPOUND",
        "fields": [
            {
                "name": "temp", 
                "type": "H5T_STD_I32LE"
            }, 
            {
                "name": "pressure",
                "type": "H5T_IEEE_F32LE"
            }
        ]
    },
    "attributes": {}
}
```

## Dataset object

In the HDF data model, datasets are used to describe homogenous collections of data elements, where the organization of the
elements can either be scalar (for single element datasets), one-dimensional, or multi-dimensional. In addition, non-scalar
datasets may be extensible or non-extensible (i.e. the number of elements can be modified).

The dataset also includes information that describe other aspects of the dataset, such as compression filters, fill value, and possible chunk layout.

Also, like groups and committed types, datasets may contain a collection of attributes.

The data values of a dataset are not stored in the storage object, but instead in one or more "chunk" objects. Chunks are a regular sized partition of the dataspace
(except possibly along the "edges"). The layout key describes how the dataspace is partitioned. Each chunk is stored (assuming a value has been assigned to it) in a
separate storage object (See "Chunk Object").

In traditional HDF5 files, dataset values may be stored in either "compact", "chunks" or "contiguous" storage layouts (the later stores all values in one partition in the file).
In contrast, the object storage schema always stores data in chunks (though there may be just one chunk for smaller datasets).
This is so that we can control the maximum size of objects stored in the system.

### Dataset key

The dataset object storage key is of the form:

    db/<uuid1>/d/<uuid2>/.dataset.json

Where &lt;uuid1&gt; is formed from the first 16 hex characters of the object's id and &lt;uuid2 &gt; is formed from the last 16 hex characters of the id.

### Dataset Specification

The dataset storage schema consists of JSON with the following keys:

- "id" - the id of the dataset ("d-&lt;uuid&gt;")
- "type" - a JSON object (or string for primitive types) representing the type
- "shape" - a JSON object that representing the dataset shape
- "layout" - a JSON object that represents the chunk layout
- "creationProperties" - a JSON object representing the dataset creation property list used at dataset creation time
- "attributes" - a key/value collection of group attributes
- "created" - timestamp (seconds since epoch) of when the dataset was created
- "lastModified" - timestamp (seconds since epoch) of when the dataset was last modified
- "root" - the id of the root group in the domain

Notes:

- See: <http://hdf5-json.readthedocs.io/en/latest/bnf/dataset.html#grammar-token-dcpl> for a specification of the "creationProperties" object
- See: <https://github.com/HDFGroup/hsds/blob/master/docs/design/single_object/SingleObject.md> for a description of the "layout" object
- "creationProperties" may optionaly provide a chunk layout, but "layout" object of dataset may differ from what is provided in "creationProperties" (for optimization purposes the hsds service may use different layout values)
- See "Attributes" for a description of the object schema for attributes
- See "Types" for a description of the object schema for type
- The "id", "root", "domain", "creationProperties", "layout", and "type" keys can be assumed to be immutable
- The "shape" key is immutable unless the dataset is extensible (the shape object contains a "maxdims" key). In any case, the shape of the dataset will never shrink

### Dataset Example

Key:

    db/5644dd09-768fdcf7/d/1c61-4b5289-3052a9/.dataset.json

Object:

```json
{
    "id": "d-5644dd09-768fdcf7-1c61-4b5289-3052a9",
    "root": "g-5644dd09-768fdcf7-decc-5581fe-07547f",
    "created": 1542311303,
    "lastModified": 1542311303,
    "type": {
        "class": "H5T_INTEGER",
        "base": "H5T_STD_I32LE"
    },
    "shape": {
        "class": "H5S_SIMPLE",
        "dims": [4, 8]
    },
    "attributes": {},
    "layout": {
        "class": "H5D_CHUNKED",
        "dims": [4, 8]
    }
}
```

## Chunk object

The chunk objects are used to store dataset values. Each chunk object stores the values for one chunk element of the
dataset it's a member of. Since it's expected that for many domains, the bulk of the storage used will be for dataset values,
it's important that the design enables data to be stored and accessed efficiently.

Whereas the other objects described in this document use a JSON representation, the chunk objects typically store binary data.
Information about the data type, and chunk dimensions are contained in the dataset object.

For dataset types that are of varying length, a run length encoding format will be used. See: "Variable Length Data"

Chunk objects may not exist for every chunk of a given dataset (i.e. if no data has ever been written to that chunk).

A set of filters may be applied when writing and reading the chunk from object storage. The filters applied to a specific chunk are stored
in the object storage metadata (Description TBD).

Note: There is no explicit linking from the dataset schema to the dataset's chunks. However, given a dataset's id, the existing
set of chunks can be determined by listing all the keys under the datasets S3 key.

### Chunk key

The chunk storage key is of the form:

    db/<uuid1>/d/<uuid2>/<i>_<j>_<k>

Where:

- &lt;uuid1&gt; is the first 16 hex characters of the dataset id the chunk belongs to
- &lt;uuid1&gt; is the second 16 hex characters of the dataset id
- Following the &lt;uuid&gt; there is a series of stringified integers separated by underscores. The number of integers is equal to the rank (number of dimensions) of the dataset.
- The coordinates &lt;i&gt;, &lt;j&gt;, &lt;k&gt;, etc. identify the coordinate of the chunk (fastest varying dimension last)

Note: conceivably there could be a danger of exceeding the maximum key length (1024 characters) if the dataset had hundreds of dimensions, or very large extents.

### Chunk Specification

For fixed length types, the chunk object is a binary blob equivalent to the contents of a numpy array of the same shape and type.

For variable length types, a run length encoding format is used. See "Variable Length Data".

### Chunk object example

Consider a dataset with a dataspace of [100,100] and a chunk layout of [10,10]. For the section of the dataset at: [10:20, 30:40], the key for the chunk would be:

    db/<uuid1>/d/<uuid2>1_3

The chunk object would contain binary data of the data values in the chunk.

If the chunk is not compressed, the size of the object would be `10 * 10 * <item_size>`. If compressed, the object size would (presumably!) be less.

### Variable Length Data

For fixed length datatypes (or compound type composed of fixed length types), serialization of chunk data is straight forward. For variable
length data, the data needs an additional field so the original data can be decoded again on read. This is done by adding a 4-byte element
length in front of each element when writing to storage. The length describes the number of bytes used by that element. On read, the length
field can be used to allocate heap memory to store the given element.

## Summary Data

While it is useful to have information about a domain as a whole, e.g. the amount of storage used, for large collections it can be
inefficient to iterate through all the keys in a domain (i.e. the keys under the domain's root group key). To provide a convenient source
for aggregate charateristics, a ".info.json" object may be created under each root group. In HSDS this object is created by the ASYNC node,
and therefore the contents of the object may not accurately reflect the real time state of the domain.

### Summary key

The summary key is of the form:

    db/<uuid1>/.info.json

Where:

- &lt;uuid1&gt; is the first 16 hex characters of the root id for the domain

Note: For recently created domains, the object may not be present.

### Summary Specification

The summary schema consists of JSON with the following keys:

- "lastModified": The most recent modification time for any object in the domain
- "num_groups": The number of groups in the domain (including the root group)
- "num_datatypes": The number of datatypes in the domain
- "datasets": A map of datasets belonging to the domain. Each item has keys for "lastModified", "num_chunks", "allocated_bytes", "linked_bytes", and "num_linked_chunks"
- "num_chunks": The number of chunks present in the domain (across all datasets)
- "allocated_bytes": Amount of storage used by chunks in the domain
- "metadata_bytes": Amount of storage used by metadata objects (objects with a .json suffix) in the domain
- "linked_bytes": Amount of storage used by datasets in the domain that link to external HDF5 files
- "scan_start": Timestamp for when the domain scan process started
- "scan_complete": Timestamp for when the domain scan process comnpleted
- "md5_sum": A md5 checksum of metadata and chunk data used in the domain

### Summary example

Key:

    db/7c84a4f8-7f61cd74/.info.json

Object:

```json
{
    "lastModified": 1543365852,
    "num_groups": 1,
    "num_datatypes": 0,
    "datasets": {
        "d-7c84a4f8-7f61cd74-c999-bcdfad-2602e8": {
            "lastModified": 1543365852,
            "num_chunks": 153,
            "allocated_bytes": 160432128,
            "linked_bytes": 0,
            "num_linked_chunks": 0
        }
    },
    "num_chunks": 5725,
    "allocated_bytes": 6003097600,
    "metadata_bytes": 2494,
    "scan_start": 1543365850.919641,
    "md5_sum": "076a6a4bbf4355629f39ef7e7ddfb3b0",
    "scan_complete": 1543365852.811196
}
```

## Sub-object schema description

In this section we define common sub-objects of the top-level objects (groups, datasets, and committed types).
These sub-objects will not be stored as separate objects in the object store, but as JSON objects in a top-level object.

The specification for these borrows heavily from the HDF5/JSON specification, so we'll refer to this document: <http://hdf5-json.readthedocs.io> as appropriate.

- type
- dataspace
- attribute
- creationProperties

### Type

Types are used as components of committed type objects, attributes, and datasets (as discussed above). The type specification is given here: <http://hdf5-json.readthedocs.io/en/latest/bnf/datatype.html>.

#### Type example

The following is the JSON specifying a compound type with three fields (64-bit little endian integer, 6 character ASCII string, and 64-bit IEEE floating point):

```json
"type": {
    "class": "H5T_COMPOUND",
    "fields": [
         {
                "name": "date",
                "type": {
                    "base": "H5T_STD_I64LE",
                    "class": "H5T_INTEGER"
                }
            },
            {
                "name": "time",
                "type": {
                    "charSet": "H5T_CSET_ASCII",
                    "class": "H5T_STRING",
                    "length": 6,
                    "strPad": "H5T_STR_NULLPAD"
                }
            },
            {
                "name": "pressure",
                "type": {
                    "base": "H5T_IEEE_F64LE",
                    "class": "H5T_FLOAT"
                }
            }
    ]
}
```

### Dataspace

Dataspaces are used as components of dataset and attribute objects. The dataspace specification is given here: <http://hdf5-json.readthedocs.io/en/latest/bnf/dataspace.html>.

Note: when used in an attribute, the maxdims key for a simple dataspace is not valid (as attributes cannot be extended).

#### Dataspace Example

The following is an example of a 10 x 10 dataspace that is extendable to 20 in the first dimension and is unlimited in the second dimension:

```json
"shape": {
    "class": "H5S_SIMPLE",
    "dims": [
        10,
        10
    ],
    "maxdims": [
        20,
        "H5S_UNLIMITED"
    ]
}
```

### Attribute

Attributes are used as components of the attributes collection in dataset, group, and committed type objects.

An attribute object consists of JSON with the following keys:

- "type" - a JSON object representing the attribute type
- "shape" - a JSON object representing the dataspace of the attribute
- "value" - a JSON element (for scalar attributes) or JSON array containing the data values of the attribute

#### Attribute Example

The following is an example of an attribute with 5 elements of type 8-bit little-endian integers:

```json
{
    "shape": {
        "class": "H5S_SIMPLE",
        "dims": [5]
    },
    "type": {
        "base": "H5T_STD_I8LE",
        "class": "H5T_INTEGER"
    },
    "value": [2, 3, 5, 7, 11]
}
```

### Dataset creation properties

Dataset creation properties are used to represent client requested properties of the dataset such as: chunk layout, fill value, and compression filters.

The creation properties specification is given here: <http://hdf5-json.readthedocs.io/en/latest/bnf/dataset.html#grammar-token-dcpl>.

#### Dataset creation property example

The following example shows properties for "allocTime", "fillValue", and "layout":

```json
{
    "allocTime": "H5D_ALLOC_TIME_LATE",
    "fillValue": 42,
    "layout": {
        "class": "H5D_CHUNKED",
        "dims": [10]
    }
}
```

## Related documents

The following documents provided related material that may be of use:

- HSDS Design document: <https://s3.amazonaws.com/hdfgroup/docs/HDF+Scalable+Data+Service.pdf>
- H5Serv developer documentation: <http://h5serv.readthedocs.io/en/latest/index.html>
- HDF5/JSON specification: <http://hdf5-json.readthedocs.io/>
- HDF REST API Authentication and authorization: <https://www.hdfgroup.org/2015/12/serve-protect-web-security-hdf5/>
- HDF Server: <https://hdfgroup.org/wp/2015/04/hdf5-for-the-web-hdf-server/>
- RESTful HDF5: <https://support.hdfgroup.org/pubs/papers/RESTful_HDF5.pdf>
