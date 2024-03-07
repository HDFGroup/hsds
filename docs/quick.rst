.. _quick:

Quick Start Guide
=================

Install
-------

With `Anaconda <http://continuum.io/downloads>`_ or
`Miniconda <http://conda.pydata.org/miniconda.html>`_::

    conda install h5py


If there are wheels for your platform (mac, linux, windows on x86) and
you do not need MPI you can install ``h5py`` via pip::

  pip install h5py

With `Enthought Canopy <https://www.enthought.com/products/canopy/>`_, use
the GUI package manager or::

    enpkg h5py

To install from source see :ref:`install`.

Core concepts
-------------

An HDF5 file is a container for two kinds of objects: `datasets`, which are
array-like collections of data, and `groups`, which are folder-like containers
that hold datasets and other groups. The most fundamental thing to remember
when using h5py is:

    **Groups work like dictionaries, and datasets work like NumPy arrays**
