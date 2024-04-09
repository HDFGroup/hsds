# Azure support for HSDS

------

HSDS was originally developed for AWS, but as many users are on other cloud platforms, supporting additional cloud vendors  is desirable.  In this document we'll outline changes needed to support Azure (Microsoft's close), the #2 most popular cloud vendor.  Though the focus is on Azure, the architectural changes describe should be helpful in supporting additional vendors as well.

------

## 1. Introduction

HSDS (https://github.com/HDFGroup/hsds/blob/master/docs/design/hsds_arch/hsds_arch.md) is a web-service written in Python that provides the functionality to read and write HDF data via a REST API (https://github.com/HDFGroup/hdf-rest-api).  HSDS runs as a set of containers managed either by Docker Engine (https://www.docker.com/products/docker-engine) for installations on a single machine, or Kubernetes (https://github.com/kubernetes/kubernetes) for installations on a cluster.

To enable HSDS on Azure we need to consider aspects of the service that use AWS specific technologies, provide replacements or work-arounds, and finally consider how these replacements will be instantiated when the service is running.

It is desirable that changes made to HSDS code to support Azure also make it easier to support other cloud platforms in the future (e.g. Google).  In general, it's desirable to push platform specific code as far down the stack as possible, so that any changes needed are limited to a small surface area.

Beyond programmatic changes to support Azure, there are many other aspects of the environment that are different from AWS (E.g. Selecting and launching virtual machines, monitoring services, networking, etc.), but these aspects won't be addressed in this document.

However, in addition to considering changes needed to have HSDS run on Azure, we also need to consider how performance and cost may differ between AWS and Azure.

## 2. Design/Architecture

### 2.1 AWS Specific code used in HSDS

The current implementation of HSDS uses these AWS specific technologies:

- AWS Authentication - how HSDS provides authentication for using AWS Services (e.g. S3 and DyanamoDB)
- AWS S3 - Storage of HDF data
- AWS DynamoDB - HSDS usernames and passwords

When running HSDS on a single VM using Docker, there should be no differences between AWS and Azure as far as how containers are set (i.e. the same docker compose configuration should work for both).

For running HSDS in a self-managed Kubernetes cluster, this should also work the same way on Azure.  In addition, both Amazon and Microsoft offers a supported Kubernetes services: AWS EKS and Azure AKS.  But self-hosted Kubernetes, vs AWS EKS vs. Azure should have minimal differences given common Kubernetes API and command line tools (e.g. kubectl), so we won't address Kubernetes differences here.

In addition, AWS Lambda is under consideration as a means to accelerate some operations in HSDS.  Azure offers an equivalent to AWS Lambda, known as "Azure Functions" (https://azure.microsoft.com/en-us/services/functions/).  Since Lambda is not currently being used in HSDS, we will not address how Azure Functions would be supported in this document though.  

We will consider how Authentication, S3, and DynamoDB are used in HSDS in the following sections.

#### 2.1.1 AWS Authentication

Each request to an AWS service needs to provide authentication credentials.  In HSDS on AWS these credentials can either be supplied via the environment variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY or by configuring the EC2 instance to use a specific  "IAM Role" (https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) and specifying that role in the AWS_IAM_ROLE environment variable.   When the AWS_IAM_ROLE is provided, the service makes a request to http://169.254.169.254/latest/meta-data/iam/security-credentials/ which returns temporary access keys.  When these expire, the service will renew them by calling the IAM endpoint again.  Using the IAM Role approach is  more secure since the actual keys don't need to be configured on the system.



#### 2.1.2 AWS S3

The AWS S3 (Simple Storage Service) is used by HSDS to read and write objects to persistent storage as defined in the HSDS Storage Schema (https://github.com/HDFGroup/hsds/blob/master/docs/design/obj_store_schema/obj_store_schema_v2.rst).  

Specific S3 operations used are: 

- GET - retrieve an object given its key
- PUT - store an object given its key
- DELETE - delete an object given its key
- GET List - get a list of keys given a prefix

HSDS doesn't use the S3 REST API directly, but rather uses the aiobotocore (https://aiobotocore.readthedocs.io/en/latest/) Python package.  This package provides an asynchronous API for S3.  The aiobotocore package is specific to AWS.

The List operations enables up to 1000 keys to be returned at a time.  Remaining keys can be returned in a paginated fashion by providing the last key returned in the previous request.

GET and PUT operations on S3 can be performed on objects up to 5 GB (objects used in HSDS are generally < 8 MB).

A GET operation performed immediately after a PUT operation may not return the same data (eventually consistent model).

AWS S3 supports up to 5,500 PUT/POST/DELETE requests per bucket and up to 5,500 GET requests (see https://docs.aws.amazon.com/AmazonS3/latest/dev/request-rate-perf-considerations.html).

#### 2.1.3 AWS DynamoDB

If the AWS_DYNAMODB_USERS_TABLE is environment variable is defined, HSDS will use a DynamoDB (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html)  table to store usernames and passwords.  Otherwise usernames and passwords are hard-coded in the hsds/admin/config/passwd.txt file.  DynamoDB is a NoSQL database that removes the need to setup and maintain a database such as MySQL.  

HSDS uses just the DynamoDB GET operation to retrieve the password for a given user.   The aiobotocore package is used in order that this operation can be performed asynchronously. 



### 2.2 Azure Equivalents

Next, we will discuss equivalent functionality on Azure.

#### 2.2.1 Authentication

Azure has the equivalent of authentication keys known as "Shared Key".  See: https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key.  For HSDS this shared key can be made available through an environment variable (name TBD) and then this value used in requests to Azure services.

Azure also supports the equivalent of AWS IAM, knows as Azure Active Directory (https://azure.microsoft.com/en-gb/services/active-directory/).  The Azure equivalent to IAM roles (https://docs.microsoft.com/en-us/azure/active-directory/develop/app-objects-and-service-principals) is a bit different than how AWS works though.  For the first release of HSDS on Azure, we will utilize shared keys and address Azure Active Directory in later releases.

#### 2.2.2 Azure Blob Storage

The Azure equivalent of AWS S3 is "Azure Block Blobs".  In Azure-speak, "blobs" are the equivalent of S3 objects and "containers" are the equivalent to S3 Buckets.   Microsoft offers a SDK for reading and writing to blob storage (https://github.com/Azure/azure-storage-python), but this package only recently (9/11/2019) added support async IO (see: https://github.com/Azure/azure-storage-python/issues/534).  An example of how the async api is used is here: https://github.com/Azure/azure-sdk-for-python/blob/master/sdk/storage/azure-storage-blob/tests/test_blob_samples_common_async.py.  

#### 2.2.3 Azure Table

Microsoft offers a NoSQL service roughly equivalent to DynamoDB called "Azure Table".  As with Azure Blob Storage, there is no existing SDK support for async access, but the REST API is documented here: https://docs.microsoft.com/en-us/rest/api/storageservices/table-service-rest-api.  



## 3 Implementation

### 3.1 Current code structure

Fortunately, all AWS specific code is isolated in two files:

- hsds/util/authUtil.py - getDynamoDB client
- hsds/util/s3Util.py - S3 operations

The call stack for when authUtil is used is as follows:

- HSDS_node process request
  - Call authUtil.validateUserPassword
    - authUtil.ValidateUserPassword
      - get DynamoDB table for user
      - if present, compare password with what is in table

The call tree for when s3Util is used as follows:

- Data Node needs to read an object
  - s3Util.getJSONObj or s3Util.getS3Bytes
- Data Node needs to write an object:
  - s3Util.putJSONObj or s3Util.putS3Bytes
- Data Node needs to delete an object:
  - s3Util.deleteS3Obj
- Data Node needs to list keys:
  - authUtil.getS3Keys
    - For each page of results: call authUtilPageItems



### 3.2 New code 

For the first release on Azure, the authUtil.py file will not need to be updated - only static password files will be supported, and DynamoDB code will not be invoked.  For subsequent releases, support for Azure Table will be added.  This will be done via using Async REST calls or creating a new async package for Azure Tables (as with block blobs described below).

In hsds/util, a new class will be created, storageUtil, that provides a set of generic operations on storage objects. E.g. getObjectJSON rather than getS3JSONObj.  This class will invoke calls on s3Util.py or the new azure-storage-blob package based on a storage type environment variable

Finally, any data node classes that currently call s3Util, will be updated to use the storageUtil class.



## 4 Conclusions

Code changes needed to support Azure are relatively limited.  Likely the challenge will mainly involve becoming familiar with the Azure environment, and "unknown unknowns" i.e. issues that we are not yet aware will be issues till we build a functioning system.

Pricing on Azure is similar to AWS.  E.g. S3 storage pricing is $0.023/GB/Month while Azure Block Blobs are 0.0184/GB/month.  There are many other pricing aspects (e.g. cost of running equivalent instances) to running HSDS and some experience will be needed before these can be properly assessed. 

Similarly, performance of HSDS running on Azure is expected to be comparable with AWS.  This can assumption can be validated by running existing benchmarks on Azure with comparable hardware and evaluating the performance.





​	













