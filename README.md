# HSDS (Highly Scalable Data Service) - REST-based service for HDF5 data

## Introduction

HSDS is a web service that implements a REST-based web service for HDF5 data stores.
Data can be stored in either a POSIX files system, or using object-based storage such as
AWS S3, Azure Blob Storage, or MinIO <min.io>.
HSDS can be run a single machine using Docker or on a cluster using Kubernetes (or AKS on Microsoft Azure).

In addition, HSDS can be run in serverless mode with AWS Lambda or h5pyd local mode.

## Quick Start

Make sure you have Python 3, Pip, and git installed, then:

1.  Clone this repo: `$ git clone https://github.com/HDFGroup/hsds`
2.  Go to the hsds directory: `$ cd hsds`
3.  Run install: `$ python setup.py install` OR install from pypi: `$ pip install hsds`
4.  Setup password file: `$ cp admin/config/passwd.default admin/config/passwd.txt`
5.  Create a directory the server will use to store data, and then set the ROOT_DIR environment variable to point to it: `$ mkdir ~/hsds_data; export ROOT_DIR="${HOME}/hsds_data"`
6.  Create the hsds test bucket: `$ mkdir $ROOT_DIR/hsdstest`
7.  Start server: `$ ./runall.sh --no-docker`
8.  In a new shell, set the environment variable HSDS_ENDPOINT to the string displayed. E.g.: `$ export HSDS_ENDPOINT=http+unix://%2Ftmp%2Fhs%2Fsn_1.sock`
9.  Set environment variables for the admin account: `$ export ADMIN_USERNAME=admin` and `$ export ADMIN_PASSWORD=admin` (adjust for any changes made to the passwd.txt file)
10. Run the test suite: `$ python testall.py`
11. (Optional) Post install setup (test data, home folders, cli tools, etc): [docs/post_install.md](docs/post_install.md)
12. (Optional) Install the h5pyd package for an h5py compatible api and tool suite: https://github.com/HDFGroup/h5pyd

To shut down the server, and the server was started with the --no-docker option, just control-C.

If using docker, run: `$ ./stopall.sh`

Note: passwords can (and should for production use) be modified by changing values in hsds/admin/config/password.txt and rebuilding the docker image. Alternatively, an external identity provider such as Azure Active Directory or KeyCloak can be used. See: [docs/azure_ad_setup.md](docs/azure_ad_setup.md) for Azure AD setup instructions or [docs/keycloak_setup.md](docs/keycloak_setup.md) for KeyCloak.

## Detailed Install Instructions

### On AWS

For complete instructions to install on a single Azure VM with Docker:

- See: [docs/docker_install_aws.md](docs/docker_install_aws.md)

For complete instructions to install on AWS Kubernetes Service (EKS):

- See: [docs/kubernetes_install_aws.md](docs/kubernetes_install_aws.md)

For complete instructions to install on AWS Lambda:

- See: [docs/aws_lambda_setup.md](docs/aws_lambda_setup.md).

### On Azure

For complete instructions to install on a single Azure VM with Docker:

- See: [docs/docker_install_azure.md](docs/docker_install_azure.md)

For complete instructions to install on Azure Kubernetes Service (AKS):

- See: [docs/kubernetes_install_azure.md](docs/kubernetes_install_azure.md)

### On Prem (POSIX-based storage)

For complete instructions to install on a desktop or local server:

- See: [docs/docker_install_posix.md](docs/docker_install_posix.md)

### On DCOS (BETA)

For complete instructions to install on DCOS:

- See: [docs/docker_install_dcos.md](docs/docker_install_dcos.md)

## General Install Topics

Setting up docker:

- See [docs/setup_docker.md](docs/setup_docker.md)

Post install setup and testing:

- See [docs/post_install.md](docs/post_install.md)

Authorization, ACLs, and Role Based Access Control (RBAC):

- See [docs/authorization.md](docs/authorization.md)

Running serverless with h5pyd:

- See <https://github.com/HDFGroup/h5pyd/blob/master/README.rst>

## Writing Client Applications

As a REST service, clients be developed using almost any programming language. The
test programs under: hsds/test/integ illustrate some of the methods for performing
different operations using Python and HSDS REST API (using the requests package).

The related project: <https://github.com/HDFGroup/h5pyd> provides a (mostly) h5py-compatible
interface to the server for Python clients.

For C/C++ clients, the HDF REST VOL is a HDF5 library plugin that enables the HDF5 API to read and write data
using HSDS. See: <https://github.com/HDFGroup/vol-rest>. Note: requires v1.12.0 or greater version of the HDF5 library.

## Uninstalling

HSDS only modifies the storage location that it is configured to use, so to uninstall just remove
source files, Docker images, and S3 bucket/Azure Container/directory files.

## Reporting bugs (and general feedback)

Create new issues at <http://github.com/HDFGroup/hsds/issues> for any problems you find.

For general questions/feedback, please use the HSDS forum: <https://forum.hdfgroup.org/c/hsds>.

## License

HSDS is licensed under an APACHE 2.0 license. See LICENSE in this directory.

## Integration with JupyterHub

The HDF Group provides access to an HSDS instance that is integrated with JupyterLab: HDF Lab. HDF Lab is a hosted Jupyter environment with these features:

- Connection to a HSDS instance
- Dedicated Xeon core per user
- 10 GB Posix Disk
- 200 GB S3 storage for HDF data
- Sample programs and data files

Sign up for HDF Lab here: <https://www.hdfgroup.org/hdfkitalab/>.

## Azure Marketplace

VM Offer for Azure Marketplace. HSDS for Azure Marketplace provides an easy way to
setup a Azure instance with HSDS. See: <https://azuremarketplace.microsoft.com/en-us/marketplace/apps/thehdfgroup1616725197741.hsdsazurevm?tab=Overview> for more information.

## Websites

- Main website: <https://www.hdfgroup.org/solutions/highly-scalable-data-service-hsds/>
- Source code: <https://github.com/HDFGroup/hsds>
- Forum: <https://forum.hdfgroup.org/c/hsds>
- Documentation: <http://h5serv.readthedocs.org> (For REST API)

## Other useful resources

### HDF Group Blog Posts

- Web Caching: <https://www.hdfgroup.org/2022/10/improve-hdf5-performance-using-caching/>
- HSDS Streaming: <https://www.hdfgroup.org/2022/08/hsds-streaming/>
- Cloud Storage Options for HDF5: <https://www.hdfgroup.org/2022/08/cloud-storage-options-for-hdf5/>
- HSDS Docker Images: <https://www.hdfgroup.org/2022/07/hsds-docker-images/>
- HSDS Container Types: <https://www.hdfgroup.org/2022/07/deep-dive-hsds-container-types/>
- Using Multiprocessing in Python: <https://www.hdfgroup.org/2022/06/speed-up-cloud-access-using-multiprocessing/>
- Biosimulations - case study with HSDS and Vega: <https://www.hdfgroup.org/2022/02/biosimulations-a-platform-for-sharing-and-reusing-biological-simulations/>
- HSDS for Microsoft Azure: <https://www.hdfgroup.org/2021/08/hsds-for-azure/>
- New Features in HSDS v0.6: <https://www.hdfgroup.org/2020/10/new-features-in-hsds-version-0-6/>
- HSDS Security: <https://hdfgroup.org/wp/2015/12/serve-protect-web-security-hdf5>
- HDF for the Web: HDF Server: <https://www.hdfgroup.org/2015/04/hdf5-for-the-web-hdf-server/>

### External Blogs and Articles

- A RESTful Meeting Between MATLAB and HDF Server: <https://www.mathworks.com/matlabcentral/fileexchange/59072-a-restful-meeting-between-matlab-and-hdf-server-web-based-hdf5-access-using-matlab>
- AWS Big Data Blog: <https://aws.amazon.com/blogs/big-data/power-from-wind-open-data-on-aws/>

### Slide Decks

- HSDS v0.7 New Features, EUHUG 2022: <https://www.hdfgroup.org/wp-content/uploads/2022/05/HSDS_New_Feautres_7.0.pdf>
- HSDS Serverless, EUHUG 2021: <https://www.hdfgroup.org/wp-content/uploads/2021/07/ServerlessHSDS.pdf>
- HSDS REST, HUG 2020: <https://www.hdfgroup.org/wp-content/uploads/2020/10/HSDS_Rest_Service_HDF5_Readey.pdf>
- HSDS with Jupyter, ESIP 2018: <https://www.slideshare.net/HDFEOS/hdf-kita-lab-jupyterlab-hdf-service>
- HDF Data Services, SciPy17: <http://s3.amazonaws.com/hdfgroup/docs/hdf_data_services_scipy2017.pdf>

### Videos

- HSDS Webinar: <https://www.youtube.com/watch?v=9b5TO7drqqE>
- HSDS Overview, Allotrope Connect Day: <https://www.youtube.com/watch?v=nRHXEkhlfZ0>
- The Use of HSDS on SlideRule, HUG 2020: <https://www.youtube.com/watch?v=i-KIoGqdEMg>
- HDF Data Services, SciPy 2017: <https://www.youtube.com/watch?v=EmnCz1Hg-VM>
- RESTful HDF, SciPy 2015: <https://www.youtube.com/watch?v=JSFZ3i3WcjQ>

### Papers

- restfulSE: A semantically rich interface for cloud-scale genomics with Bioconductor: <https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6392152>
- RESTful HDF5 White Paper: <https://www.hdfgroup.org/pubs/papers/RESTful_HDF5.pdf>
