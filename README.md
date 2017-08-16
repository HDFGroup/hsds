HSDS (Highly Scalable Data Service) - REST-based service for HDF5 data using object storage
===========================================================================================

*NOTICE*: The code in this repository is proprietary to The HDF Group. The code is confidential
and cannot be shared with anyone without The HDF Group’s prior written permission

Introduction
------------
HSDS is a web service that implements a REST-based web service for HDF5 data stores
as described in the paper: http://hdfgroup.org/pubs/papers/RESTful_HDF5.pdf. 

Websites
--------

* Main website: http://www.hdfgroup.org
* Source code: https://github.com/HDFGroup/hsds
* Mailing list: hdf-forum@lists.hdfgroup.org <hdf-forum@lists.hdfgroup.org>
* Documentation: http://h5serv.readthedocs.org  (For REST API)

Other useful resources
----------------------

* RESTful HDF5 White Paper: https://www.hdfgroup.org/pubs/papers/RESTful_HDF5.pdf  
* SciPy17 Presentation: http://s3.amazonaws.com/hdfgroup/docs/hdf_data_services_scipy2017.pdf 
* HDF5 For the Web: https://hdfgroup.org/wp/2015/04/hdf5-for-the-web-hdf-server
* HSDS Security: https://hdfgroup.org/wp/2015/12/serve-protect-web-security-hdf5 


Quick Install
-------------

See: :doc:`docs/docker_install.rst`
 
 
Writing Client Applications
----------------------------
As a REST service, clients be developed using almost any programming language.  The 
test programs under: h5serv/test/integ illustrate some of the methods for peforming
different operations using Python. 

The related project: https://github.com/HDFGroup/h5pyd provides a (mostly) h5py-compatible 
interface to the server for Python clients.


Uninstalling
------------

HSDS only modifies the S3 bucket that it is configured to use, so to uninstall just remove 
source files, Docker images, and S3 bucket (or minio directory). 

    
Reporting bugs (and general feedback)
-------------------------------------

Create new issues at http://github.com/HDFGroup/h5serv/issues for any problems you find. 

For general questions/feedback, please use the list (hdf-forum@lists.hdfgroup.org).

License
-------

Currently the code in this repository is confidential and should not be shared with
anyone who is not covered by an NDA with The HDF Group.
