Post Install Configuration
===========================

Once HSDS is installed and running, the following are some optional configuration steps to create test files, configure
user home folders and run integration tests.  These can be run from any machine that has connectivity to the server.

1. Install h5py: `$ pip install h5py`
2. Install h5pyd (Python client SDK): `$ pip install h5pyd`
3. Run `$ hsconfigure`.  Answer prompts for HSDS endpoint, username, and password.  These will become the defaults for hs commands and the h5pyd package
4. Create a top-level home folder if one does not already exists: `$ hstouch -u admin -p <admin_passwd> /home/`
5. Setup home folders for each username that will need to create domains: `$ hstouch -u admin -p <admin_passd> -o <username> /home/<username>/`
6. (Optional) Export the admin password as ADMIN_PASSWORD  (enables some additional tests the require admin privalages)
7. Get project source code: `$ git clone https://github.com/HDFGroup/hsds`
8. Go to the source directory: `$cd hsds`
9. Set the environement variable for the HSDS endpoint.  E.g.: `$ export HSDS_ENDPOINT=http://hsds.hdf.test`
10. If running the tests under an account other than test_user1, run: `$ export USER_NAME=<username>`
11. Run the integration tests: `$ python testall.py --skip_unit`
12. Some tests will be skipped with the message: `Is test data setup?`.  To resolve, see the Test Data Setup section below.


Test Data Setup
---------------

Some HSDS integration tests expect specific HDF5 files to be loaded onto the server.  To set these up, perform the follwoing steps:

1. Create a test folder in the test account home folder: `$ hstouch -u test_user1 -p <passwd> /home/test_user1/test/` 
2. Download the following file: `$ wget https://s3.amazonaws.com/hdfgroup/data/hdf5test/tall.h5`
3. Import into HSDS: `$ hsload -v -u test_user1 -p <passwd> tall.h5 /home/test_user1/test/`
4. Verify upload: `$ hsls -r -u test_user1 -p <passwd> /home/test_user1/test/tall.h5`
5. Re-run the integration tests: `$ python testall.py --skip_unit`

