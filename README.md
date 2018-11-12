HSDS (Highly Scalable Data Service) - REST-based service for HDF5 data using object storage
===========================================================================================


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
* HSDS with Jupyter: https://www.slideshare.net/HDFEOS/hdf-kita-lab-jupyterlab-hdf-service 


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

For C/C++ clients, the HDF REST VOL is a HDF5 library plugin that enables the HDF5 API to read and write data 
using HSDS.  See: https://bitbucket.hdfgroup.org/users/jhenderson/repos/rest-vol/browse. 

Uninstalling
------------

HSDS only modifies the S3 bucket that it is configured to use, so to uninstall just remove 
source files, Docker images, and S3 bucket (or minio directory). 

    
Reporting bugs (and general feedback)
-------------------------------------

Create new issues at http://github.com/HDFGroup/hsds/issues for any problems you find. 

For general questions/feedback, please use the Kita forum: https://forum.hdfgroup.org/c/kita.

License
-------

This code is covered under an APACHE 2.0 license.  See LICENSE in this directory.

Integration with JupyterHub
---------------------------

The HDF Group provides access to an HSDS instance that is integrated with JupyterLab: Kita Lab.  Kita Lab is a hosted Jupyter environment with these features:

* Connection to a 16-node HSDS instance
* Dedicated Xeon core per user
* 10 GB Posix Disk
* 100 GB S3 storage for HDF data
* Sample programs and data files

Sign up for Kita Lab here: https://www.hdfgroup.org/hdfkitalab/. 

AWS Marketplace
---------------

The HDF Group provides an AWS Marketplace product, HDF Kita, that provides simple installation of HSDS
and related resources resources.  HDF Kita offers these features:

* Stores usernames and passwords in a secure DynamoDB Table
* Creates a AWS CloudWatch dashboard for service monitoring
* Aggregates container logs to AWS CloudWatch
* Includes Support by The HDF Group

HDF Kita for AWS Marketplace can be found here: https://aws.amazon.com/marketplace/pp/B07K2MWS1G. 
