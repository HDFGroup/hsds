Test Data Setup
----------------

Using the following procedure to import test files into hsds

1. Get project source code: `$ git clone https://github.com/HDFGroup/h5pyd`
2. Go to install directory: `$ cd h5pyd`
3. Build and install: `$ python setup.py install`
4. Go to apps dir: `$ cd apps`
5. Download the following file: `$ wget https://s3.amazonaws.com/hdfgroup/data/hdf5test/tall.h5`
6. In the following steps use the password that was setup for the test_user1 account in place of <passwd>
7. Create a test folder on HSDS: `$ python hstouch.py -u test_user1 -p <passwd> /home/test_user1/test` 
8. Import into hsds: `$ python hsload.py -v -u test_user1 -p <passwd> tall.h5 /home/test_user1/test/tall.h5`
9. Verify upload: `$ python hsls.py -r -u test_user1 -p <passwd> /home/test_user1/test/tall.h5


