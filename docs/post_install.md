Post Install Configuration
===========================

Once HSDS is installed and running, the following are some optional configuration steps to create test files and configure
user home folders.  These can be run from any machine that has connectivity to the server.

1. Install h5py: `$ pip install h5py`
2. Install h5pyd (Python client SDK): `$ pip install h5pyd`
3. Run `$ hsconfigure`.  Answer prompts for HSDS endpoint, username, and password.  These will become the defaults for hs commands and the h5pyd package
4. Create a top-level home folder if one does not already exists: `$ hstouch -u admin -p <admin_passwd> /home/`
5. Setup home folders for each username that will need to create domains: `$ hstouch -o <username> /home/<username>/`
6. Create a test folder in the test account home folder: `$ hstouch -u test_user1 -p <passwd> /home/test_user1/test/` 
7. Download the following file: `$ wget https://s3.amazonaws.com/hdfgroup/data/hdf5test/tall.h5`
8. Import into HSDS: `$ hsload -v -u test_user1 -p <passwd> tall.h5 /home/test_user1/test/`
9. Verify upload: `$ hsls -r -u test_user1 -p <passwd> /home/test_user1/test/tall.h5`
