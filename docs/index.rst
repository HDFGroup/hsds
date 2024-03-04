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

Installation
------------

.. toctree::
   :maxdepth: 1

   docker_install_aws
   docker_install_azure
   docker_install_tencent
   docker_install_posix
   docker_install_dcos
   kubernetes_install_aws
   kubernetes_install_azure
   aws_lambda_setup
   post_install
   setup_docker

 

Advanced topics
---------------

.. toctree::
    :maxdepth: 1

    authorization
    related_projects


Meta-info about the HSDS project
--------------------------------

.. toctree::
    :maxdepth: 1

    contributing
    release_guide
    faq
    licenses


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
