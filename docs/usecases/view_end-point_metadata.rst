View end-point metadata
========================

Description 
------------
A user may need detailed metadata for data sets provided at a given end-point. Metadata, such 
as groups, data sets, data set attributes (dimensions, units, geo-spatial metadata , etc...) 
are often used to formulate a "plan" to work with the underlying data sets.

Scope
------
system

Level
------
user goal

Primary Actor
--------------
An earth science researcher, data analyst or application developer

Stakeholders and Interests
---------------------------
* An earth science researcher, data analyst or application developer who is interested in retrieving 
  metadata from NASA data products via web services
* A data provider who is interested in making available NASA data products via web services for broad user consumption
* A data manager 

Preconditions
--------------
1. An installation of HSDS on a cloud service provider (initially Amazon EC2).
2. The data product that was originally stored in HDF5 is correctly placed on an object store (initially 
   placed on Amazon Simple Storage Service (S3))
3. There is network connectivity between the client and the HSDS service
4. The user has become aware of the end-point through some other means

Minimal Guarantee
------------------
1. HSDS accepts and logs the attempted request 

Success Guarantee
------------------
1. HSDS accepts and logs the attempted request 
2. The HSDS service presents `JSON <http://www.json.org/>`_ output to the client based on the end-point query within a *reasonable* response time 

Main Success Scenario
----------------------
1. The user "connects" to the HSDS data service endpoint 
   
   1a. The user may determine the end-point through other means such as a directory service, scientific publication, etc... 

2. The user requests the metadata from the end-point
3. The HSDS service returns metadata from the end-point in JSON format to the client 
4. The user's client accepts the JSON from the HSDS service and loads it into the appropriate data structure for the application used by the user's client.

