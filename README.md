HSDS (Highly Scalable Data Service) - REST-based service for HDF5 data
======================================================================
 
Introduction
------------

HSDS is a web service that implements a REST-based web service for HDF5 data stores.
Data can be stored in either a POSIX files system, or using object based storage such as
AWS S3, Azure Blob Storage, or OpenIO <openio.io>.
HSDS can be run a single machine using Docker or on a cluster using Kubernetes (or AKS on Microsoft Azure).

Websites
--------

* Main website: <https://www.hdfgroup.org/solutions/highly-scalable-data-service-hsds/>
* Source code: <https://github.com/HDFGroup/hsds>
* Mailing list: hsds@forum.hdfgroup.org <hsd@forum.hdfgroup.org>
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

Make sure you have Python 3, Pip, and git installed, then:

   1. Clone this repo: `$ git clone https://github.com/HDFGroup/hsds`
   2. Go to the hsds directory: `$ cd hsds`
   3. Run install: `$ python setup.py install`  OR install from pypi: `$ pip install hsds`
   4. Setup password file: `$ cp admin/config/passwd.default admin/config/passwd.txt`
   5. Create a directory the server will use to store data, and then set the ROOT_DIR environment variable to point to it: `$ mkdir ~/hsds_data; export ROOT_DIR="${HOME}/hsds_data"`
   6. Start server: `$ ./runall.sh --no-docker`
   7. In a new shell, set the environment variable HSDS_ENDPOINT to the string displayed.  E.g.: `$ export HSDS_ENDPOINT=http+unix://%2Ftmp%2Fhs%2Fsn_1.sock`
   8. Run the test suite: `$ python testall.py`  
   9. (Optional) Post install setup (test data, home folders, cli tools, etc): [docs/post_install.md](docs/post_install.md)
   10. (Optional) Install the h5pyd package for an h5py compatible api and tool suite:  https://github.com/HDFGroup/h5pyd


To shut down the server, and the server was started with the --no-docker option, just control-C.

If using docker, run: `$ ./stopall.sh`
    
Note: passwords can (and should for production use) be modified by changing values in hsds/admin/config/password.txt and rebuilding the docker image.  Alternatively, an external identity provider such as Azure Active Directory or KeyCloak can be used.  See: [docs/azure_ad_setup.md](docs/azure_ad_setup.md) for Azure AD setup instructions or [docs/keycloak_setup.md](docs/keycloak_setup.md) for KeyCloak.

Detailed Install Instructions
-----------------------------

**On AWS**

See: [docs/docker_install_aws.md](docs/docker_install_aws.md) for complete install instructions.

See: [docs/kubernetes_install_aws.md](docs/kubernetes_install_aws.md) for setup on Kubernetes.

**On Azure** 

For complete instructions to install on a single Azure VM:
- See: [docs/docker_install_azure.md](docs/docker_install_azure.md)

For complete instructions to install on Azure Kubernetes Service (AKS):
- See: [docs/kubernetes_install_azure.md](docs/kubernetes_install_azure.md)

**On Prem (POSIX-based storage)** 

For complete instructions to install on a desktop or local server:
- See: [docs/docker_install_posix.md](docs/docker_install_posix.md)

**On DCOS** **(BETA)**

For complete instructions to install on DCOS:
- See: [docs/docker_install_dcos.md](docs/docker_install_dcos.md)

**General Install Topics**

Setting up docker:
- See [docs/setup_docker.md](docs/setup_docker.md)

Post install setup and testing:
- See [docs/post_install.md](docs/post_install.md)

Authorization, ACLs, and Role Based Access Control (RBAC):
- See [docs/authorization.mid](docs/authorization.md)

Writing Client Applications
----------------------------

As a REST service, clients be developed using almost any programming language.  The
test programs under: hsds/test/integ illustrate some of the methods for performing
different operations using Python and HSDS REST API (using the requests package).

The related project: <https://github.com/HDFGroup/h5pyd> provides a (mostly) h5py-compatible
interface to the server for Python clients.

For C/C++ clients, the HDF REST VOL is a HDF5 library plugin that enables the HDF5 API to read and write data
using HSDS.  See: <https://github.com/HDFGroup/vol-rest>. Note: requires v1.12.0 or greater version of the HDF5 library.

Uninstalling
------------

HSDS only modifies the storage location that it is configured to use, so to uninstall just remove
source files, Docker images, and S3 bucket/Azure Container/directory files.

Reporting bugs (and general feedback)
-------------------------------------

Create new issues at <http://github.com/HDFGroup/hsds/issues> for any problems you find.

For general questions/feedback, please use the HSDS forum: <https://forum.hdfgroup.org/c/hsds>.

License
-------

HSDS is licensed under an APACHE 2.0 license.  See LICENSE in this directory.

Integration with JupyterHub
---------------------------

The HDF Group provides access to an HSDS instance that is integrated with JupyterLab: HDF Lab.  HDF Lab is a hosted Jupyter environment with these features:

* Connection to a HSDS instance
* Dedicated Xeon core per user
* 10 GB Posix Disk
* 200 GB S3 storage for HDF data
* Sample programs and data files

Sign up for HDF Lab here: <https://www.hdfgroup.org/hdfkitalab/>.

Azure Marketplace
-----------------

VM Offer for Azure Marketplace. HSDS for Azure Marketplace provides an easy way to
setup a Azure instance with HSDS.  See: <https://azuremarketplace.microsoft.com/en-us/marketplace/apps/thehdfgroup1616725197741.hsdsazurevm?tab=Overview> for more information.

