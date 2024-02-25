Post Install Configuration
==========================

Once HSDS is installed and running, the following are some optional
configuration steps to create test files, configure user home folders
and run integration tests. **Important:** trailing slashes are essential
here. These steps can be run on the server VM or on your client.

Home Folder Creation
--------------------

By design, only the admin account is allowed to create top-level domains
(e.g. ``/mydomain.h5`` cannot be created by a non-admin account).
Typically a folder named ``/home`` is created by the admin account, and
under ``/home``, folders that are owned by sepcific user accounts.

To set up folders in this fashion, follow the following steps:

1. Install h5py: ``$ pip install h5py``
2. Install h5pyd (Python client SDK): ``$ pip install h5pyd``
3. Run ``$ hsconfigure``. Answer prompts for HSDS endpoint, username,
   and password. Leave the prompt for API_KEY empty. These will become
   the defaults for hs commands and the h5pyd package
4. Create a top-level home folder if one does not already exists:
   ``$ hstouch -u admin -p <admin_passwd> /home/``
5. Setup home folders for each username that will need to create
   domains:
   ``$ hstouch -u admin -p <admin_passd> -o <username> /home/<username>/``
6. Run HSDS integration tests, and/or h5pyd tests if desired. See the
   relevant sections below

Running HSDS Tests
------------------

HSDS tests don’t depend on h5pyd (other than the optiona test data setup
- see below); rather they use the HSDS REST API to verify functionality
on the server. These can be run on any machine.

To run, perform the following steps:

1. (Optional) Export the admin password as ADMIN_PASSWORD (enables some
   additional tests the require admin privalages)
2. Get project source code:
   ``$ git clone https://github.com/HDFGroup/hsds``
3. Go to the source directory: ``$cd hsds``
4. Set the environement variable for the HSDS endpoint. E.g.:
   ``$ export HSDS_ENDPOINT=http://hsds.hdf.test``
5. If running the tests under an account other than test_user1, run:
   ``$ export USER_NAME=<username>``
6. Run the integration tests: ``$ python testall.py --skip_unit``
7. Some tests will be skipped with the message: ``Is test data setup?``.
   To resolve, see the Test Data Setup section below.

Running h5pyd Tests
-------------------

The h5pyd integration tests, verify that the Python SDK (by extension,
HSDS) is functioning correctly.

To run the h5pyd tests, perform the following steps:

1. Set environment variable for test output folder:
   ``export H5PYD_TEST_FOLDER="/home/<username>/h5pyd_test/"``
2. Create folder for test files: ``hstouch $H5PYD_TEST_FOLDER``
3. Get h5pyd code: ``git clone https://github.com/HDFGroup/h5pyd``
4. Go to the h5pyd directory: ``cd h5pyd``
5. Run h5pyd test suite: ``python testall.py``

Test Data Setup
---------------

Some HSDS integration tests expect specific HDF5 files to be loaded onto
the server. To set these up, perform the follwoing steps:

1. Create a test folder in the test account home folder:
   ``$ hstouch -u test_user1 -p <passwd> /home/test_user1/test/``
2. Download the following file:
   ``$ wget https://s3.amazonaws.com/hdfgroup/data/hdf5test/tall.h5``
3. Import into HSDS:
   ``$ hsload -v -u test_user1 -p <passwd> tall.h5 /home/test_user1/test/``
4. Verify upload:
   ``$ hsls -r -u test_user1 -p <passwd> /home/test_user1/test/tall.h5``
5. Re-run the integration tests: ``$ python testall.py --skip_unit``

Some HSDS integration tests depend on traditional HDF5 files. These
files can be found here: s3://hdf5.sample/data/hdf5test/. If you are
running HSDS with AWS S3 storage, set the environment variable
HDF5_SAMPLE_BUCKET to “hdf5.sample”. If you are using Azure Blob
storage, create a container, copy the files from the the S3 bucket to
the location data/hdf5test in the container, and set the
HDF5_SAMPLE_BUCKET environment variable to the container name. Finally,
if you are using Posix storage, create a directory under ROOT_DIR, copy
the files from the S3 bucket to data/hdf5test in that directory, and set
HDF5_SAMPLE_BUCKET to the directory name.
