HSDS (Highly Scalable Data Service) - REST-based service for HDF5 data
======================================================================

[![Build Status](https://travis-ci.org/HDFGroup/hsds.svg?branch=master)](https://travis-ci.org/HDFGroup/hsds)

Introduction
------------

HSDS is a web service that implements a REST-based web service for HDF5 data stores.
Data can be stored in either a POSIX files system, or using object based storage such as
AWS S3, Azure Blob Storage, or OpenIO <openio.io>.
HSDS can be run a single machine using Docker or on a cluster using Kubernetes (or AKS on Microsoft Azure)
The commercial offering based on this code is known as Kita&trade;.
More info at: <https://www.hdfgroup.org/solutions/hdf-kita/>.

Websites
--------

* Main website: <https://www.hdfgroup.org/kita>
* Source code: <https://github.com/HDFGroup/hsds>
* Mailing list: kita@forum.hdfgroup.org <kita@forum.hdfgroup.org>
* Documentation: <http://h5serv.readthedocs.org> (For REST API)

Other useful resources
----------------------

* RESTful HDF5 White Paper: <https://www.hdfgroup.org/pubs/papers/RESTful_HDF5.pdf>
* SciPy17 Presentation: <http://s3.amazonaws.com/hdfgroup/docs/hdf_data_services_scipy2017.pdf>
* HDF5 For the Web: <https://hdfgroup.org/wp/2015/04/hdf5-for-the-web-hdf-server>
* HSDS Security: <https://hdfgroup.org/wp/2015/12/serve-protect-web-security-hdf5>
* HSDS with Jupyter: <https://www.slideshare.net/HDFEOS/hdf-kita-lab-jupyterlab-hdf-service>
* AWS Big Data Blog: <https://aws.amazon.com/blogs/big-data/power-from-wind-open-data-on-aws/>


Quick Start
-------------

**On AWS**

Make sure you have Python 3, docker, docker-compose, and aws cli tools installed, then:
   `$runall.sh`
to run the server.

To run test suite, set an environment variable for the admin password:
   `$export ADMIN_PASSWORD=admin`
Note: passwords can (and should for production use) be modified by changing values in hsds/admin/config/password.txt and rebuilding the docker image.

Next, run the test suite by:
   `$python testall.py`

See: [docs/docker_install_aws.md](docs/docker_install_aws.md) for complete install instructions.

See: [docs/kubernetes_install.md](kubernetes_install.md) for setup on Kubernetes.

**On Azure** **(BETA)**

For complete instructions to install on a single Azure VM:
- See: [docs/docker_install_azure.md](docs/docker_install_azure.md)

For complete instructions to install on Azure Kubernetes Service (AKS):
- See: [docs/kubernetes_install_azure.md](docs/kubernetes_install_azure.md)

**On DCOS** **(BETA)**

For complete instructions to install on DCOS:
- See: [docs/docker_install_dcos.md](docs/docker_install_dcos.md)

**On Prem (POSIX-based storage)**  **(BETA)**

For complete instructions to install on a desktop or local server:
- See: [docs/docker_install_posix.md](docs/docker_install_posix.md)

Writing Client Applications
----------------------------

As a REST service, clients be developed using almost any programming language.  The
test programs under: hsds/test/integ illustrate some of the methods for performing
different operations using Python and HSDS REST API (using the requests package).

The related project: <https://github.com/HDFGroup/h5pyd> provides a (mostly) h5py-compatible
interface to the server for Python clients.

For C/C++ clients, the HDF REST VOL is a HDF5 library plugin that enables the HDF5 API to read and write data
using HSDS.  See: <https://github.com/HDFGroup/vol-rest>.

Uninstalling
------------

HSDS only modifies the storage location that it is configured to use, so to uninstall just remove
source files, Docker images, and S3 bucket/Azure Container/directory files.

Reporting bugs (and general feedback)
-------------------------------------

Create new issues at <http://github.com/HDFGroup/hsds/issues> for any problems you find.

For general questions/feedback, please use the Kita&trade; forum: <https://forum.hdfgroup.org/c/kita>.

License
-------

HSDS is licensed under an APACHE 2.0 license.  See LICENSE in this directory.

Integration with JupyterHub
---------------------------

The HDF Group provides access to an HSDS instance that is integrated with JupyterLab: Kita&trade; Lab.  Kita&trade; Lab is a hosted Jupyter environment with these features:

* Connection to a HSDS instance
* Dedicated Xeon core per user
* 10 GB Posix Disk
* 100 GB S3 storage for HDF data
* Sample programs and data files

Sign up for Kita&trade; Lab here: <https://www.hdfgroup.org/hdfkitalab/>.

AWS Marketplace
---------------

The HDF Group provides an AWS Marketplace product, Kita&trade; Server, which provides simple installation of HSDS
and related AWS resources.  Kita&trade; offers these features:

* Stores usernames and passwords in a secure DynamoDB Table
* Creates a AWS CloudWatch dashboard for service monitoring
* Aggregates container logs to AWS CloudWatch

Kita&trade; Server for AWS Marketplace can be found here: <https://aws.amazon.com/marketplace/pp/B07K2MWS1G>.
