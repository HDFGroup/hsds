[![HSDS Linux](https://github.com/HDFGroup/hsds/actions/workflows/hsds-linux.yml/badge.svg)](https://github.com/HDFGroup/hsds/actions/workflows/hsds-linux.yml)
[![HSDS Windows](https://github.com/HDFGroup/hsds/actions/workflows/hsds-windows.yml/badge.svg)](https://github.com/HDFGroup/hsds/actions/workflows/hsds-windows.yml)
[![h5pyd integration](https://github.com/HDFGroup/hsds/actions/workflows/h5pyd-integration.yml/badge.svg)](https://github.com/HDFGroup/hsds/actions/workflows/h5pyd-integration.yml)
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/hdfgroup/hsds)


# HSDS (Highly Scalable Data Service) - REST-based service for HDF5 data

## Introduction

HSDS is a web service that implements a REST-based web service for HDF5 data stores.
Data can be stored in either a POSIX files system, or using object-based storage such as
AWS S3, Azure Blob Storage, or [MinIO](https://min.io).
HSDS can be run a single machine with or without Docker or on a cluster using Kubernetes (or AKS on Microsoft Azure).

## Release Notes

[CHANGELOG.md](CHANGELOG.md) records the changes in the release being prepared; for
earlier releases, see the notes on each [release](https://github.com/HDFGroup/hsds/releases).

## What's New in HSDS v1.0.0

### New Features

- **Query improvements**: added a dedicated `/datasets/{id}/query` path for read-only queries, restored `query`-param support on `PUT /datasets/{id}/value` (query-based conditional update), updated the query syntax and evaluation engine (now backed by the `h5json` library), and extended query support to multi-dimensional datasets (previously limited to 1-D).
- **Region reference support**: `GET`/`PUT` value requests can now read and write HDF5 region references.
- **Consolidated domain metadata**: added support for generating and serving a consolidated summary of all objects in a domain, reducing the number of requests needed to inspect a domain's full structure.
- **Client-provided object IDs and timestamps**: `POST` requests for datasets, groups, and datatypes can now specify the object's ID and creation timestamp directly (subject to a configurable `max_timestamp_drift`), rather than always having the server generate them - useful for replication/migration scenarios.
- **Richer object-creation payloads**: `POST` requests for datasets and groups can now initialize attributes, links, and (for datasets) initial data values in the same request that creates the object.
- **Batch object creation**: added multi-object creation support for datasets, groups, and datatypes (create several objects in a single `POST`), backed by a new async `DomainCrawler`/`PostCrawler`-based implementation.
- **Improved array (`H5T_ARRAY`) dtype handling**: fixed selection/read/write handling for datasets whose own type is an array (subarray) dtype, not just array-typed fields nested in a compound type. (Note: this covers binary reads/writes; JSON-encoded writes to a top-level `H5T_ARRAY` dataset are still tracked as a known issue.)
- **Formal OpenAPI specification**: added `openapi.yml`, a full OpenAPI 3 description of the HSDS REST API.

### Notable Bug Fixes

- Fixed a race condition in node "ready" state handling that could cause requests to be routed to a node before it was fully initialized.
- Fixed a hang in the `DomainCrawler`'s data-write handler.
- Fixed handling of `H5S_UNLIMITED` in dataset `maxdims`.
- Fixed scalar-dataset value access, uninitialized attribute values, and a `chunkref`-indirect-layout bug.
- Fixed binary field-selection reads/writes (selecting a subset of compound-type fields).
- `runall.sh` now waits until the service reports a `READY` state before returning, instead of a fixed sleep.

### Other Changes

- Internal data-type, array, object-ID, shape, dataset, filter, link, and time utilities have been migrated to the new standalone [h5json](https://github.com/HDFGroup/hdf5-json) library, replacing several local `hsds/util/*` modules (see Breaking Changes below).
- Removed AWS Lambda support (`Dockerfile.lambda`, `lambda_function.py`, and related docs/config).
- Minimum supported Python version is now 3.11 (up from 3.10).

## Breaking Changes in v1.0.0

v1.0.0 is a major version bump and includes some deliberate, non-backward-compatible changes. If you're upgrading from a 0.x release, be aware of the following:

### REST API

- **Query responses changed**: using the `query` parameter on `PUT /datasets/{id}/value` used to return the matching rows in a `"value"` field, each row starting with its index. It now returns only the matching coordinates, in an `"indices"` field with one list per match (for example `[[1], [4]]` on a 1-D dataset). The new `GET /datasets/{id}/query` endpoint returns the same `"indices"` without updating anything, or little-endian int64 coordinates when a binary response is requested. `GET /datasets/{id}/value?query=...` still returns `"value"`, but its rows no longer start with the index: JSON rows lose their first element and binary rows lose their 8-byte index prefix. Use `/query` when you need the indices.
- **Query syntax changed**: queries are now evaluated by `h5json`, which has no `where` clause. A query such as `open < 4000 where symbol in (b'AAPL', b'EBAY')` now returns 400; write it as `open < 4000 AND symbol IN (AAPL, EBAY)`. Byte-string literals (`b'AAPL'`) and the `&` and `|` operators are still accepted. Domain queries (`GET /domains?query=...`) go through the same engine, and their existing syntax still works.
- **Dataset chunk layout moved in the JSON schema**: `GET /datasets/{id}` no longer returns a top-level `"layout"` key. Layout information (whether client-specified or server-generated) now always appears nested under `"creationProperties"."layout"`. Clients that read `dataset_json["layout"]` directly need to switch to `dataset_json["creationProperties"]["layout"]`.
- **Client chunk dimensions are used as given**: chunk dimensions supplied in `creationProperties.layout` are stored exactly as requested. 0.x servers resized them to fall between `min_chunk_size` and `max_chunk_size`.
- **Filters require a chunked layout**: creating a dataset with an `H5D_CONTIGUOUS` layout and a filter list now returns 400. 0.x servers silently switched such datasets to a chunked layout.
- **Unlimited dimensions are reported as `"H5S_UNLIMITED"`**: an unlimited dimension in `maxdims` is now returned as the string `"H5S_UNLIMITED"` rather than `0`. Requests may still use `0`. Datasets created by a 0.x server still report `0`, so clients should accept both.
- **External link field renamed**: external link objects now report the target file/domain under a `"file"` key instead of `"h5domain"` in API responses. Creating a link with `"h5domain"` in the request body is still accepted for backward compatibility, but it will no longer be echoed back that way - expect `"file"` in the response.
- **`id` in object-creation requests is now honored**: `POST /groups`, `/datasets` and `/datatypes` create the object with the `id` given in the request body, and return 400 if that ID already exists. 0.x servers ignored the key and always generated a new ID, so a client that posts an existing object's JSON back unchanged now gets 400 instead of a copy. Sending both `link` and `h5path` in one request also returns 400 now.
- **`getobjs` reads a background summary**: `GET /?getobjs=1` now builds `domain_objs` from a summary the data nodes write when they scan a domain, instead of crawling the domain on each request. A domain that has not been scanned yet, which includes every domain created by a 0.x server until its next scan, gets no `domain_objs` key, and a scanned domain reflects the last scan. Entries no longer include `id` or `attributeCount`, always include `attributes`, and the `include_attrs` parameter is ignored.
- **ACL updates require `updateACL`**: `PUT /acls/{username}` now returns 403 unless the caller has `updateACL` permission on the domain. 0.x servers let any authenticated user change a domain's ACLs.

### Storage and deployment

- **No downgrades or mixed-version clusters**: datasets created by v1.0.0 store their layout only under `creationProperties`, and new external links store `file` rather than `h5domain`. 0.x nodes read the old keys, so don't run 0.x and 1.x nodes against the same storage, and don't return to 0.x after writing data with 1.x.
- **Cluster readiness waits for the target node counts**: with a head node (the Docker deployments), service and data nodes stay out of the `READY` state until the head node has seen `TARGET_SN_COUNT` service nodes and `TARGET_DN_COUNT` data nodes register. Nodes started beyond those counts, for example with `docker compose --scale`, are not added to the cluster. The shipped Compose files set both counts from `SN_CORES` and `DN_CORES`.
- **Kubernetes load balancer manifests removed**: `admin/kubernetes/k8s_service_lb.yml` and `k8s_service_lb_azure.yml` are gone. Expose `k8s_service.yml` through `k8s_ingress_nginx.yml` or `k8s_gateway_envoy.yml` instead; see the Kubernetes install docs.
- **Log format changed**: timestamps are now on by default (`log_timestamps: true`) and use ISO 8601 instead of epoch seconds, and lines logged while handling a request include its trace ID in brackets. The `REQ>` and `RSP>` lines changed layout too. Log parsers written against 0.x need updating; the new `log_format: json` option gives structured output.

### Python and packaging

- **AWS Lambda support removed**: HSDS can no longer be deployed as an AWS Lambda function; `Dockerfile.lambda`, `lambda_function.py`, `hsds/util/awsLambdaClient.py`, and the associated setup docs have been removed. `HsdsApp` no longer accepts the `islambda` argument, and the `--removesitepackages` node option is gone.
- **New required dependency**: HSDS now depends on the [h5json](https://github.com/HDFGroup/hdf5-json) package (2.0.0 or later) for core type/array/object-ID/shape utilities. Code that imported HSDS's own `hsds.util.idUtil`, `hsds.util.timeUtil`, `hsds.util.hdf5dtype`, `hsds.util.arrayUtil`, or `hsds.util.boolparser` modules directly will break, as those modules have been removed in favor of `h5json` equivalents.
- **Minimum Python version raised to 3.11** (from 3.10). Building from source now requires setuptools 77 or later.

## Quick Start

### On your desktop/laptop

Make sure you have Python 3 and Pip installed, then:

1.  Run install: `$ ./build.sh --no-lint --no-docker` from source tree OR install from pypi: `$ pip install hsds`
2.  Create a directory the server will use to store data, example: `$ mkdir ~/hsds_data`
3.  Start server: `$ hsds --root_dir ~/hsds_data`
4.  Run the test suite. In a separate terminal run:
    -  Set user_name: `$ export USER_NAME=$USER`
    -  Set user_password: `$ export USER_PASSWORD=$USER`
    -  Set admin name: `$ export ADMIN_USERNAME=$USER`
    -  Set admin password: `$ export ADMIN_PASSWORD=$USER`
    -  Run test suite: `$ python testall.py --skip_unit`
5. (Optional) Install the h5pyd package for an h5py compatible api and tool suite: https://github.com/HDFGroup/h5pyd
6. (Optional) Post install setup (test data, home folders, cli tools, etc): [docs/post_install.md](docs/post_install.md)

To shut down the server, and the server is not running in Docker, just control-C.

If using docker, run: `$ ./stopall.sh`

Note: passwords can (and should for production use) be modified by changing values in hsds/admin/config/password.txt and rebuilding the docker image. Alternatively, an external identity provider such as Azure Active Directory or KeyCloak can be used. See: [docs/azure_ad_setup.md](docs/azure_ad_setup.md) for Azure AD setup instructions or [docs/keycloak_setup.md](docs/keycloak_setup.md) for KeyCloak.

## Detailed Install Instructions

### On AWS

For complete instructions to install on a single Azure VM with Docker:

- See: [docs/docker_install_aws.md](docs/docker_install_aws.md)

For complete instructions to install on AWS Kubernetes Service (EKS):

- See: [docs/kubernetes_install_aws.md](docs/kubernetes_install_aws.md)


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

Monitoring and metrics (Prometheus / Grafana):

- See [docs/prometheus_metrics.md](docs/prometheus_metrics.md)

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

## Azure Marketplace

VM Offer for Azure Marketplace. HSDS for Azure Marketplace provides an easy way to
setup a Azure instance with HSDS. See: <https://azuremarketplace.microsoft.com/en-us/marketplace/apps/thehdfgroup1616725197741.hsdsazurevm?tab=Overview> for more information.

## Websites

- Main website: <https://www.hdfgroup.org/solutions/highly-scalable-data-service-hsds/>
- Source code: <https://github.com/HDFGroup/hsds>
- Forum: <https://forum.hdfgroup.org/c/hsds>
- Documentation: <https://support.hdfgroup.org/documentation/index.html> 
- REST API: <https://github.com/HDFGroup/hdf-rest-api>

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
