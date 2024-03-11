.. HSDS documentation master file, created by
   sphinx-quickstart on Mon Feb 26 12:27:41 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

HSDS - Highly Scaleable Data Service
====================================

The HSDS is a Restful web service for working with HDF5 data.

`HDF5 <https://hdfgroup.org>`_ lets you store huge amounts of numerical
data, and easily manipulate that data from NumPy. For example, you can slice
into multi-terabyte datasets stored on disk, as if they were real NumPy
arrays. Thousands of datasets can be stored in a single file, categorized and
tagged however you want.

Where to start
--------------

* :ref:`Quick-start guide <quick>`
* :ref:`Installation <install>`


Other resources
---------------

* `Python and HDF5 O'Reilly book <https://shop.oreilly.com/product/0636920030249.do>`_
* `Ask questions on the HDF forum <https://forum.hdfgroup.org/c/hdf-tools/h5py>`_
* `GitHub project <https://github.com/h5py/h5py>`_


Introductory info
-----------------

.. toctree::
    :maxdepth: 1

    introduction
    quick
    related_projects


Installation
------------

.. toctree::
   :maxdepth: 1

   install/introduction
   install/docker_install_aws
   install/docker_install_azure
   install/docker_install_tencent
   install/docker_install_posix
   install/docker_install_dcos
   install/kubernetes_install_aws
   install/kubernetes_install_azure
   install/aws_lambda_setup
   install/post_install
   install/setup_docker

REST API
--------

.. toctree::
   :maxdepth: 1

   restapi/restapi  

Use cases
---------

.. toctree::
   :maxdepth: 1
   
   usecases/intro
   usecases/view_end-point_metadata
   usecases/extract_one_dimensional_subset
   usecases/extract_several_one_dimensional_subsets
   usecases/extract_subsets_with_shared_common_dimensions
   usecases/extract_two_dimensional_subset
   usecases/extract_three_dimensional_subset

Advanced topics
---------------

.. toctree::
    :maxdepth: 1

    advanced/authorization
    advanced/azure_ad_setup
    advanced/frontdoor_install_azure
    advanced/keycloak_setup


Meta-info about the HSDS project
--------------------------------

.. toctree::
    :maxdepth: 1

    whatsnew/index
    contributing
    release_guide
    faq
    licenses


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
