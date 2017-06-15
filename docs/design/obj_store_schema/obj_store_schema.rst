###############################
Object Storage Schema for HDF5
###############################

*John Readey - The HDF Group*

.. contents:: Table of contents

Intro
#####

This document describes the object storage schema used by the Highly Scalable Data Storage service (hsds). The object storage schema describes a mapping from the HDF5 data model of groups, datasets, committed types to a set of storage objects that would be suitable for use in an object storage system and as the persistent storage layer for hsds.

Goals
#####

The following goals were a factor in the design of the object storage schema:

#. The schema should be able to represent traditional HDF5 files at a high level of fidelity
#. The schema should not introduce any performance barriers in the design of hsds 
#. The schema should result in a cost-effective system for public cloud implementations (both in terms of overall storage size and request api pricing)
#. The schema should be adaptable to other object storage systems (e.g. OpenStack/Swift)
#. The schema should be usable with other services or applications that desire to represent the HDF5 data model as a set of storage objects

Object Storage
##############

This section will provide some over context regarding object storage system.

Object storage background
*************************

An object storage is a storage architecture that manages data as objects rather than files, or blocks.

Compared to other storage technologies object storage has the advantages of:

#. Scalability - It's easier to build out large storage solutions (.e.g AWS S3 manages trillions of objects)
#. Throughput - Potentially greater aggregate throughput (scales with the number of storage nodes)
#. Reliability - Objects are automatically replicated to avoid potential data loss
#. Cost-effective - easier to scale out.  Vendors like AWS provide low-cost, pay-as-you go services

The target object storage system for the object storage schema is AWS Simple Storage Service (AWS S3), but in general the schema should be adaptable to other object storage systems such as Google Cloud Storage, Ceph, or OpenStack/Swift.

Object storage systems share some common properties:

#. Objects stored in the system are identified by a key (a string value)
#. The object storage system maintains metadata about each object (see next section)
#. They are not natively POSIX complient (e.g. there is no way to append to a storage object)
#. Objects are transparently (to the client) replicated in multiple storage devices for redundancy
#. Latency is high compared to storage on local disk, but aggregate throughput is potentially much higher
#. Keys and Objects live in a "bucket".  Permissions to access objects are defined at the bucket level

Object Metadata
***************

In an object storage system each object has data, a key, and metadata (a set of properties that pertain to the object).  The metadata can be system or user defined.  The later is typically limited to a fairly small size (2KB in the case of AWS S3).

For the purposes of this document that following metadata properties (as defined for AWS S3) are relevant to the schema design:

#. Content-Length - the size of the object in bytes
#. Content-MD5 - a checksum of the object data
#. Last-Modified - the time at which the object was last modified (or created, whichever is later)

In addition, the object storage schema will use define some custom metadata properties such as Compression-State for chunk objects.

Object Storage system assumptions
*********************************

The following constraints and assumptions are given as the basis of the schema design (some of which may need to be re-evaluated for use in non-S3 systems):

#. Object keys are limited to 1024 characters
#. The use of many small objects would be prohibitive from a cost perspective (API Request Pricing)
#. The use of very large objects (e.g. >100MB) would introduce excessive latency
#. The first 3-4 characters of the keys should be randomaly distributed (to avoid request rate limits due to a single storage system be targeted)
#. Listing keys is generally inefficient (and would not work well with randomly distributed keys)
#. The storage system is not read-write consistent
#. The storage system supports object metadata of at least 1024 bytes per object
#. The aggregate throughput of the storage system would not be expected to limit hsds scalability 
#. All objects managed by hsds will exist in one "bucket", the hsds service will have read-write authority for the given bucket
#. All updates to the objects will be through the hsds service
#. Updates to a storage object are complete (i.e. the entire object is overwritten), atomic (i.e. last writer wins), and either succeed or fail with no update to the object
#. There is no practical limit to the number of objects that can be stored in a bucket
#. The object storage system does not provide support for "transactions" (i.e. "all or nothing" update of two or more objects)



The HDF5 data model
###################

The following is a brief review of the HDF5 data model as it relates to the shema design (see the HDF5 docs for a fuller description).

In the traditional HDF5 data model, object are stored in a posix File.  Management of objects within the file is done by the HDF5 library and is opaque to the HDF5 library client.

HDF5 data model consist of:

#. Group - an object that manages a set of attributes and links
#. Attribute - a "small" named data item that consist of a dataspace, type description, and data
#. Link - a named reference to another HDF5 object (hard link for links within the file, as well as Soft and External links)
#. Dataset - a data container that consists of a dataspace, type description, attributes, and other properties (e.g. chunk layout, fill value, compression filters, etc.)
#. Chunk - one element of a regular partition of a dataset dataspace
#. Committed Type - a sharable type object (that also has a set of attributes)

This document will describe how each of these entities will be stored as an object (as well as the equivalent of an HDF5 "file")

The goal of the object schema is to be of sufficient fidelity that it should be possible to convert a traditional HDF5 file to a set of objects, and then convert the set of objects to a HDF5 file that is equivalent to the original file.  

Comparison of managing HDF5 entities in a file vs. an object store
******************************************************************

Management of HDF5 entities in an object store brings up a different set of considerations when compared with managing entities within an HDF5 file:

#. The object storage system is itself an efficient key-value store, so there is no need for internal data structures such as btrees
#. Management of "free space" within a file is not an issue when using an object store
#. The object storage system doesn't provide the equivalent of an append operation, so the entire object must be re-written for each write (though partial reads are supported)
#. Performance is sensitive to the size of objects in the object store (c.f. http://improve.dk/pushing-the-limits-of-amazon-s3-upload-performance/)
#. Given that writes to the object store are atomic, there is no possibility that the storage system will be left in an inconsistent state
#. Certain functions that are typically performed by the filesystem (e.g. listing files, file permissions) we need to be managed by the service (e.g. there needs to be the ability to store the access rights for a given object
#. Unlike HDF5 entities in a file, the "file" an object store object is contained in is not immediately apparent.  The connection between objects and the "file" they are contained in needs to be explicitly managed.

Additions to the HDF5 data model to support the HDF REST API
************************************************************

Several additions to the HDF5 data model have been made in order to support the HDF REST API.  In the reference implementation of the HDF REST API (h5serv), these additions were stored in a hidden group within the traditional HDF5 file managed by the service.  In hsds, these additions can be directly modeled by the schema.

These additions are described in the sub-sections below.

UUID
----

Each high level object (group, dataset, committed type) can be identified by a UUID - a 36 character alphanumeric identifier.  E.g.: "0568d8c5-a77e-11e4-9f7a-3c15c2da029e".  The UUID's used in the object storage schema add a two-character prefix to the id to identify the type of object:

* "g-": a group id
* "d-": a dataset id
* "t-": a type id

For example, the id used for a group object with the above UUID would be::

    g-0568d8c5-a77e-11e4-9f7a-3c15c2da029e


ACL
---

Each high level object can maintain an ACCESS Control List that describes the default and user-specific access permissions for that object (see: http://h5serv.readthedocs.io/en/latest/AclOps/index.html).

Timestamp
---------

Each high level object has timestamps for create time and last updated time, that can be retrieved using the REST API.


Schema Description
##################

The object schema defines the storage for the following entities: 

* domains (roughly equivalent to an HDF5 file)
* groups
* committed type
* datasets
* chunks

Note: attributes and links are stored as a component of their parent object.

Note: all strings used in the schema (e.g. link names) are UTF8 encoded unicode strings.  Strings stored in a dataset will be encoded based on the type description of the dataset.

Domains
******* 

The domain entity is similar to traditional HDF5 files in that they are containers for related collections of resources.  Unlike a file however, the related resources for a domain aren't contained within the domain object, but are persisted as other objects within the bucket.  The domain object contains a "root" key that can be used to retrieve the root group of the given domain.  From the root group other entities in the domain can be retrived by traversing the directed graph anchored at the root group.

Domain key
----------

Domain keys end with "/.domain.json" and can have an arbitrary prefix. Unlike other entities in the object storage schema, domain keys are stored hierarchaly (as with files in a file system), delimited using the '/' character.  This enables domain keys to be listed by prefix and provides a cannonical key for the parent domain of a domain.

For example, the domain key::

    /home/test_user1/my_domain/.domain.json

Would have a parent domain of::

    /home/test_user1/.domain.json

Sub-domains of the domain could be found by listing all keys with the prefix of::

    /home/test_user1/my_domain/

Domain Specification
--------------------

The domain object contains JSON with the following keys:

* "acls" - Access Control List (user permissions) for actions on domain.  See below for subkeys.
* "owner" - Username of the owner (user who initially created the domain)
* "root" - the UUID (not including the md5 hash) of the root group in the domain
* "created" - the timestamp for when the domain was created
* "lastModified" - the timestamp for when the domain was last updated
* "stats" - summary statistics that include all objects in the domain

The "owner" and "acls" keys are required, others may not be present.  In particular, if the "root" key is not present, that impies there is no HDF collection associated with this domain.  In this case the domain object can serve as a sort of "directory" for a set of related sub-domains.

Notes:

* The service layer may impose a policy where domains can only be created if there is an existing domain with the requisite permission ACLs for the requesting user.  One or more "top-level" domains (e.g. "/home") would be created outside the service API (e.g. by an administrator with permissions to create objects in the bucket directly).
* The owner and root keys can be assumed to be immutable (i.e. these values can be cached)
* Metadata about the owner (and other usernames referenced in this schema) are assumed to be stored in another system (such as NASA URS)
* The "root" key is optional.  If not present, the domain doesn't have an associatted root group (but can server as a place-holder for sub-domains)

Domain object example
---------------------

Key::

    /home/test_user1/my_domain/.domain.json

Object:

.. code-block:: json

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
        "lastModified": 1479168471.038638,
        "stats": {
            "groupCount": 20,
            "typeCount": 0,
            "datasetCount": 67,
            "logicalSize": 13194139533,
            "allocatedSize": 8456534532,
            "actualSize": 5457344534,
            "checksum": "394a7d8d67c7e022490212d6098a2209",
            "lastModified": 14791685671.058672
        }
    }


Domain ACLs
-----------

The "acls" key in the domain object provides a method to denote user access rights to objects within the domain.
The service layer may enforce a policy to use acls key to authorize or deny request to perform specific actions by a given user on objects within the domain.  

The ACL consist of a key-value collection where the key denotes the username for the given user.  One special key is defined: "default".  This key defines the permission for any username that is not otherwise listed.

Within the username key there are six required sub-keys that each have a value of true or false:

* "create" - If true, the user has permission to create new objects, links, and attributes wihin the domain
* "read" - If true, the user has permission to read from any object in the domain
* "update" - If true, the user has permission modify dataset values and extend datasets
* "delete" - If true, the user has permission to delete any object in the domain (or the domain itself)
* "readACL" - If true, the user has permission to read any ACL in the domain
* "updateACL" - If true, the has permission to modify the ACL (including adding additional usernames)

Note: optionally, an ACL key can be used in a group, dataset, or committed datatype object.  If an ACL is present, it is can be used to enforce permissions for that object.  If not present, the domain ACL is used as described above.

Example: Using the ACLs defined for the "my_domain" object above, user "test_user1" would be authorized to make any change to objects in the domain, or change the ACL itself.  User "joebob" (not listed in the ACL keys), would have permission to perform any read operation (assuming a more restrictive ACL is not present in the requested object), but not have authority to modify or delete any object.

Domain stats
------------

In order to provide summary information about the objects in a domain, an additional object will be used to store this data.  The object will be JSON with the following keys:

* "groupCount" - integer value of number of groups in domain  
* "typeCount" - integer value of number of committed type objects in domain 
* "datasetCount" - integer value of number of dataset objects in domain  
* "logicalSize" - storage size of all entities including non-allocated chunks
* "allocatedSize" - storage size that of all entities including only allocated chunks
* "actualSize" - storage size as reported by the storage system (maybe smaller than "allocatedSize" due to compression)
* "lastModified" - the timestamp for the most recent change to any object in the domain

For reasons of efficiency, the summary information will typically be updated asynchronously from changes to object state. Therefore the stats object may not reflect the most recent changes to objects in the domain.  E.g. is a dataset is created using the HDF REST API, the changes in datasetCount and size keys won't be immediately reflected.  The Last-Modified metadata property of the domain stats object can be used to determine when these keys were last updated.

 
Group object
************

In the HDF data model group object is used to organize collections of other groups and datasets via describing a set of links (either hard, soft, or external).  In the object store schema, the links contain just information about the link itself, not the linked object.  The group object may also contain a collection of attributes.

Group key
---------

The group object storage key is of the form::

    <hash>-g-<uuid>

Where <hash> is an md5 hash of the group id ("g-<uuid>"). Where <uuid> is a standard 36 character UUID.

Since storage systems such as AWS S3 use a hash of the first few characters of the object key to determine the storage node used to store the object, these characters should be randomly distributed to ensure thoughput to the storage system is not limited.  UUIDs in general don't have good distribution (i.e. it's very common for the first characters to be repeated), so the object key for a specific UUID is formed by prefixing a five character md5 hash to the object id.

For example, if the object id is::

    g-2428ae0e-a082-11e6-9d93-0242ac110005

An md5 hash of the id would be::

    8211ea6301342ba59ee07056cef3e586

Taking the first five characters and appending to the id with a hyphen seperator gives::

    8211e-g-2428ae0e-a082-11e6-9d93-0242ac110005

This will then be used as the storage key to store and retrieve the given object.

The same approach is used for dataset, committed type, and chunk keys.


Group Specification
-------------------

The Group object consist of JSON with the following keys:

* "id" - the id of the group ("g-<uuid>")
* "attributes" - a key/value collection of group atttributes
* "links" - a key/value collection of links
* "created" - timestamp (since epoch) of when the group was created
* "lastModified" - timestamp of when the group was last modified
* "root" - the id of the root group in the domain
* "acls" - access Control List for authorization overrides

There are three types of links that are supported: Hard, Soft, and External.  Each link item is a JSON object with the following keys:

* "class" - the type of link.  Must be one of the values: "H5L_TYPE_HARD", "H5L_TYPE_SOFT", or "H5L_TYPE_EXTERNAL"
* "created" - timestamp of when the link was created
* "id" - for hard links, the id value is the id of the dataset or group the link points to
* "h5path" - for soft or external links, this is a string that gives the HDF5 path the object is expected to be found
* "domain" - for external links, this is a string that gives the domain which the linked object is a member of

Notes:

* "acls" is an optional key.  If the key is not present (or is present, but the requesting user sub-key is not), the domain ACL will be used (see "Domain ACLs")
* the attributes collection keys consist of the attribute names.  See "Attributes" for a description of the object schema for attributes
* The "id", "root", and "domain" keys can be assumed to be immutable

TBD:

* A group that contains a large number (roughly > 100K or more) of links or attributes, may present problems when accessed.  If a single storage object is very large, there will be excessive latency in retrieving the object from the object store.  Also applications loading a large JSON string may consume an excessive amount of memory.  To address this, one possiblity would be to shard such large groups into multiple storage objects.

Group object example
--------------------

Key::

    a860f-g-2428ae0e-a082-11e6-9d93-0242ac110005

Object:

.. code-block:: json

    {
        "id": "g-2428ae0e-a082-11e6-9d93-0242ac110005",  
        "attributes": {}, 
        "links": {
            "dset1.1": {
                "created": 1478039150.084772, 
                "id": "d-24b14908-a082-11e6-9d93-0242ac110005", 
                "class": "H5L_TYPE_HARD"
            },
            "slink": {
                "created": 1478039189.034954, 
                "h5path": "/g2/g2.1/dset2.1.1", 
                "class": "H5L_TYPE_SOFT"
            },
            "extlink": {
                "created": 1478039211.035682, 
                "h5path": "/a_group/a_dset", 
                "domain": "/home/test_user2/another_domain",
                "class": "H5L_TYPE_EXTERNAL"
            },
        }, 
        "created": 1478039149.932783, 
        "lastModified": 1478039149.932783, 
        "root": "g-2428ae0e-a082-11e6-9d93-0242ac110005" 
    }


Committed Type Object
*********************

In the HDF data model the committed type object is used to provide types that can be shared among datasets and attributes.  In addition, the committed type may contain its own attributes as well.  The object store schema provides keys that describe the type as well as a key/value collection for attributes.

Committed Type Key
------------------ 

The committed type object storage key is of the form::

    hash>-t-<uuid>

Where <hash> is an md5 hash of the group id ("t-<uuid>").  Where <uuid> is a standard 36 character UUID.

Committed Type Specification
----------------------------

The Committed type storage schema consists of JSON with the following keys:

* "id" - the id of the committed type ("t-<uuid>")
* "type" - a JSON object (or string for primitive types) representing the type
* "attributes" - a key/value collection of group atttributes
* "created" - timestamp (seconds since epoch) of when the committed type was created
* "lastModified" - timestamp (seconds since epoch) of when the committed type was modified
* "root" - the id of the root group in the domain
* "acls" - access Control List for authorization overrides

Notes:

* "acls" is an optional key.  If the key is not present (or is present, but the requesting user sub-key is not), the domain ACL will be used (see "Domain ACLs")
* See "Attributes" for a description of the object schema for attributes
* See "Links" for a description of the object schema for links
* See "Types" for a description of the object schema for type
* The "id", "root", "domain", and "type" keys can be assumed to be immutable

Committed Type Example
----------------------

Key::

    a7ce4-t-15417e88-9b01-11e6-bf10-0242ac110005

Object:

.. code-block:: json

    {  
        "id": "t-15417e88-9b01-11e6-bf10-0242ac110005", 
        "type": {
            "base": "H5T_STD_U32LE", 
            "class": "H5T_INTEGER"
        },
    "attributes": {},
    "created": 1478039183.392074,
    "lastModified": 1478039183.392074, 
    "root": "g-2428ae0e-a082-11e6-9d93-0242ac110005" 
    }


Dataset object
**************

In the HDF data model, datasets are used to describe homogenous collections of data elements, where the organization of the elements can either be scalar (for one element datasets, one-dimensional, or multi-dimensional). In addition, non-scalar datasets may be extensible or non-extensible (i.e. the number of elements can be modified).

The dataset also includes information that describe other aspects of the dataset, such as compression filters, fill value, and possible chunk layout.  

Also, like groups and committed types, datasets may contain a collection of attributes.

The data values of a dataset are not stored in the storage object, but instead in one or more "chunk" objects.  Chunks are a regular sized partition of the dataspace (except possibly along the "edges").  The layout key describes how the dataspace is partitioned.  Each chunk is stored (assuming any value has been assigned to it) in a seperate storage object (See "Chunk Object").

In traditional HDF5 files, dataset values may be stored in either "compact", "chunks" or "contiguous" storage layouts (the later stores all values in one partition in the file).  By contrast the object storage schema always stores data in chunks (though there may be just one chunk for smaller datasets).  This is so that we can limit the maximum size of objects stored in the system.


Dataset key
-----------

The dataset object storage key is of the form::

    <hash>-d-<uuid>

Where <hash> is an md5 hash of the dataset id ("d-<uuid>"). Where <uuid> is a standard 36 character UUID.

Dataset Specification
---------------------

The dataset storage schema consists of JSON with the following keys:

* "id" - the id of the dataset ("d-<uuid>")
* "type" - a JSON object (or string for primitive types) representing the type
* "shape" - a JSON object that representing the dataset shape
* "layout" - a JSON object that represents the chunk layout
* "creationProperties" - a JSON object representing the dataset creation property list used at dataset creation time 
* "attributes" - a key/value collection of group atttributes
* "created" - timestamp (seconds since epoch) of when the dataset was created
* "lastModified" - timestamp (seconds since epoch) of when the dataset was last modified
* "root" - the id of the root group in the domain
* "acls" - access Control List for authorization overrides
* "stats" - summary statistics for the dataset and chunk data

Notes:

* See: http://hdf5-json.readthedocs.io/en/latest/bnf/dataset.html#grammar-token-dcpl for a specification of the "creationProperties" object
* "creationProperties" may optionaly provide a chunk layout, but "layout" object of dataset may differ from what is provided in "creationProperties"  (for optimization purposes the hsds service may use different layout values)
* "acls" is an optional key.  If the key is not present (or is present, but the requesting user sub-key is not), the domain ACL will be used (see "Domain ACLs")
* See "Attributes" for a description of the object schema for attributes
* See "Types" for a description of the object schema for type
* The "id", "root", "domain", "creationProperties", "layout", and "type" keys can be assumed to be immutable
* The "shape" key is immutable unless the dataset is extensible (the shape object contains a "maxdims" key).  In anycase, the shape of the dataset will never shrink
* The "stats" object is updated asynchronously from changes to dataset values, so may not reflect the most recent changes to the dataset

Dataset Example
---------------

Key::

    4feb1-d-4ab77230-9c0e-11e6-8fdd-0242ac110005

Object:

.. code-block:: json

    {
        "id": "d-4ab77230-9c0e-11e6-8fdd-0242ac110005", 
        "type": {
            "class": "H5T_FLOAT",
            "base": "H5T_IEEE_F32LE"
        }, 
        "shape": {
            "class": "H5S_SIMPLE",
            "maxdims": [1000,1000,0], 
            "dims": [1000,1000,243]
        },
        "layout": { 
            "class": "H5D_CHUNKED",
            "dims": [100,100,100]
        },
        "creationProperties": { 
            "allocTime": "H5D_ALLOC_TIME_LATE", 
            "fillTime": "H5D_FILL_TIME_IFSET", 
            "layout": {
                "class": "H5D_CONTIGUOUS"
            }
        },
        "created": 1477549587.387293, 
        "lastModified": 1477549587.387293, 
        "root": "g-2428ae0e-a082-11e6-9d93-0242ac110005", 
        "attributes": {},
        "stats": {
            "allocatedChunkCount": 122,
            "logicalChunkCount": 300,
            "logicalSize": 972000000,
            "allocatedSize": 488000000,
            "actualSize": 234586943,
            "checksum": "394a7d8d67c7e022490212d6098a2209",
            "lastModified": 14791685671.058672
        }
    }

Chunk object
************

The chunk objects are used to store dataset values.  Each chunk object stores the values for one chunk element of the dataset it's a member of.  Since it's expected that for many domains, the bulk of the storage used will be for dataset values, it's important that the design enables data to be stored and accessed efficiently.

Whereas the other objects described in this document use a JSON representation, the chunk objects will typically store binary data.  Information about the type used, and chunk dimensions are contained in the dataset object.

For dataset types that are of varying length, the object will contain a JSON representation of the values in the chunk (possibly compressed).

Chunk objects may not exist for every chunk of a given dataset (i.e. if no data has ever been written to that chunk).

A set of filters may be applied when writing and reading the chunk from object storage.  The filters applied to a specific chunk are stored in the object storage metadata (Description TBD). 

Note: There is no explicit linking from the dataset schema to the dataset's chunks.  However, given a dataset shape and layout, the set of possible chunk ids can be determined and then the storage system queried to see if the chunk exists or not.

Chunk key
---------

The chunk storage key is of the form::

    <hash>-c-<uuid>_<i>_<j>_<k>

Where:

* <hash> is an md5 hash of the chunk id ("c-<uuid>_i_j_k")
* <uuid> is a standard 36 character UUID
* Following the <uuid> there is a series of stringified integers seperated by underscores.  The number of integers should be equal to the rank (number of dimensions) of the dataset.
* The coordinates <i>, <j>, <k>, etc.  identify the coordinate of the chunk (fastest varying dimension last)

Note: conceivably there could be a danger of exceeding the maximum key length (1024 characters) if the dataset had hundreds of dimensions, or very large extents.
 

Object metadata
---------------

Information about compression filters applied to the chunk data will be stored as User-defined Metadata of the object (Note: this is limited to 2KB bytes on AWS S3).

TBD: define metadata keys

Chunk Specification
-------------------

The chunk object is a binary blob for fixed length types, or a JSON array for varying length types.

TBD: Is there a potential for data loss in converting floating-point data to JSON and back?  Validate that the JSON loader stringifies floating point values with sufficient percision.  

Chunk object example
--------------------

Consider a dataset with a dataspace of [100,100] and a chunk layout of [10,10].  For the section of the dataset at: [10:20, 30:40], the key for the chunk would be::

    <hash>-c-<uuid>_1_3

The chunk object would contain binary data (assuming a fixed-length type) of the data values in the chunk.

If the chunk is not compressed, the size of the object would be 10 \* 10 \* <item_size>.  If compressed, the object size would (presumably!) be less.


Sub-object schema description
#############################

In this section we define common sub-objects of the top-level objects (groups, datasets, and committed types).
These sub-objects will not be stored as separate objects in the object store, but as JSON objects in a top-level object.

The specification for these borrows heavily from the hdf5-json specification, so we'll refer to this document: http://hdf5-json.readthedocs.io/en/latest/index.html# as appropriate.  

* type
* dataspace
* attribute
* creationProperties

Type
****

Types are used as components of committed type objects, attributes, and datasets (as discussed above).  The type specification is given here: http://hdf5-json.readthedocs.io/en/latest/bnf/datatype.html.

Type example
------------

The following is the JSON specifying a compound type with three fields (64-bit little endian integer, 6 character ASCII string, and 64-bit IEEE floating point):

.. code-block:: json

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


Dataspace
*********

Dataspaces are used as components of dataset and attribute objects.  The dataspace specification is given here: http://hdf5-json.readthedocs.io/en/latest/bnf/dataspace.html.

Note: when used in an attribute, the maxdims key for a simple dataspace is not valid (as attributes cannot be extended).

Dataspace Example
-----------------

The following is an example of a 10 x 10 dataspace that is extendable to 20 in the first dimension and is unlimited in the second dimension:

.. code-block:: json

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


Attribute
*********

Attributes are used as components of the attributes collection in dataset, group, and committed type objects.

An attribute object consist of JSON with the following keys:

* "type" - a JSON object representing the attribute type
* "shape" - a JSON object representing the dataspace of the attribute
* "value" - a JSON element (for scalar attributes) or JSON array containing the data values of the attribute

Attribute Example
-----------------

The following is an example of an attribute with 5 elements of type 8-bit little-endian:

.. code-block:: json

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


Dataset creation properties
***************************

Dataset creation properties are used to represent client requested properties of the dataset such as: cunk layout, fill value, and compression filters.   

The creation properties specification is given here: http://hdf5-json.readthedocs.io/en/latest/bnf/dataset.html#grammar-token-dcpl. 

Dataset creation property Example
---------------------------------

The following example shows properties for "allocTime", "fillValue", and "layout":

.. code-block:: json

    {
        "allocTime": "H5D_ALLOC_TIME_LATE", 
        "fillValue": 42, 
        "layout": {
            "class": "H5D_CHUNKED",
            "dims": [10]
        }
    }


Related documents
#################

The following documents provided related material that mayby of use:

* HSDS Design document: https://s3.amazonaws.com/hdfgroup/docs/HDF+Scalable+Data+Service.pdf
* H5Serv developer documentation: http://h5serv.readthedocs.io/en/latest/index.html 
* HDF5/JSON specification: http://hdf5-json.readthedocs.io/en/latest/index.html 
* HDF REST API Authentication and authorization: https://www.hdfgroup.org/2015/12/serve-protect-web-security-hdf5/ 
* HDF Server: https://hdfgroup.org/wp/2015/04/hdf5-for-the-web-hdf-server/ 
* RESTful HDF5: https://support.hdfgroup.org/pubs/papers/RESTful_HDF5.pdf  



