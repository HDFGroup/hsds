Extract subsets from data sets that share common dimensions and grid (run parallel)
======================================================================================

Extends
--------
* Extract a one dimensional subset along a single dimension 
* Extract a two dimensional subset from a data set  
* Extract a three dimensional subset from a data set  
* Extract many one dimensional subsets along a single dimension 

Context of use
--------------
End user `remote sensing <https://en.wikipedia.org/wiki/Remote_sensing>`_ and or earth system modeling output
products are commonly structured on fixed grids, as in x and y or longitude and latitude. Each grid may represent
some quantity over some time range; it is often the case where several grids (or cubes) will run parallel to each other. 
The structure is roughly illustrated in the following figure:

.. image:: layoutmulti.png
      :align: center
      :alt: data parallel layout example 

Description 
-------------
The extraction extension for *Extract a two dimensional subset from a data set* is for some time step **t** for 
all **x** and **y** and for a group of data sets that run parallel to each other (share common dimensions, etc...), 
highlighted in black below:

.. image:: layoutmultisub.png
      :align: center
      :alt: data parallel 2d subset example 

All extensions are not shown here but will follow a similar description as above.

Scope
-----
system

Level
-----
user goal

Primary Actor
-------------
An earth science researcher, data analyst or application developer

Stakeholders and Interests
---------------------------
* An earth science researcher, data analyst or application developer who is interested in retrieving 
  NASA data products via web services
* A data provider who is interested in making available NASA data products via web services for broad user consumption
* A data manager 

Preconditions
--------------
1. An installation of HSDS on a cloud service provider (initially Amazon EC2).
2. The data product that was originally stored in HDF5 is correctly placed on an object store (initially 
   placed on Amazon Simple Storage Service (S3))
3. There is network connectivity between the client and the HSDS service
4. The user "knows" the dimensions of the data and the indices for the subset of interest 

Minimal Guarantee
------------------
1. HSDS accepts and logs the attempted request 

Success Guarantee
------------------
1. HSDS accepts and logs the attempted request 
2. The HSDS service presents `JSON <http://www.json.org/>`_ or binary output to the client based on the parameters and indices 
   noted above in a *reasonable* response time

Main Success Scenario
---------------------
1. The user "connects" to the HSDS data service endpoint 
   
   1a. The user may determine the bounds and or data set of interest either by querying the HSDS service end-point or through some other means 

2. The user requests a subset of the data set based on a set of index parameters. e.g. conn_handle.{tasmax, tasmaxQC, ndvi}[1, :, :] or
   conn_handle.{tasmax, tasmaxQC, ndvi}[:, 2, 2], etc... 
3. The HSDS service returns JSON or binary data to the client based on the parameters and indices determined above
4. The user's client accepts the JSON data from the HSDS service and loads it into the appropriate data structure 
   for the application used by the user's client.

