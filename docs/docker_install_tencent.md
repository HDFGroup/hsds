Installation using Docker with Tencent Cloud object storage
===========================================================

1. In the Tencent Cloud console, select "Cloud Access Management/API Keys"
2. Press "Create Key" to create aa new access key.  Save the "SecretId" and "SecretKey" in a secure place
select "Compute/Cloud Virtual Machine"
3. Using the Tencent Cloud console, go to "Cloud Object Storage", select "Bucket List", then press "Create Bucket" button.  Choose a region that is in the same region as your VM (or geographically close to you if running HSDS on a external machine).  Enter a bucket name, and click "Next".  For other options, the default
values should be fine.  Record the bucket name and endpoint value.
4. Next in the console go to "Compute/Cloud Virtual Machine" and in the "Instances" pane, press the "Create" button to create a new VM
5. Select region and billing mode as desired
6. Select instance type.  4 core cpu and 16 GB memory recommended for best performance
7. Keep the default "TencentOS" image, and 50GB SSD storage
8. For the security group, enable ports 22, and 80
9. Start instance and ssh into the instance.  The following steps will be performed on the instance
10. Install Python 3 (e.g. with Miniconda <https://docs.conda.io/en/latest/miniconda.html>)
11. Install docker-compose by running: `$ curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose`, and `$ chmod +x /usr/local/bin/docker-compose` 
12. If docker is not running, start it with: `systemctl start docker`
13. Get project source code: `$ git clone https://github.com/HDFGroup/hsds`
14. Go to the hsds directory and run: `$ ./build.sh`
15. Go to admin/config directory: `$ cd hsds/admin/config`
16. Copy the file "passwd.default" to "passwd.txt".  Add any usernames/passwords you wish.  Modify existing passwords (for admin, test_user1, test_user2) for security.
17. If group-level permissions are desired (See [Authorization](authorization.md)), copy the file "groups.default" to "groups.txt".  Modify existing groups as needed
18. Create environment variables as in "Sample .bashrc" below.  Run `$ source .bashrc` to update the environment
19. Create the file **admin/config/override.yml** for deployment specific settings (see "Sample override.yml") if desired
20. Start the service `$./runall.sh <n>` where n is the number of containers desired (defaults to 4).  Typically you'll want this to be the number of cores on your VM.
21. Run `$ docker ps` and verify that the containers are running: hsds_head, hsds_sn_[1-n], hsds_dn_[1-n]
22. Run `$ curl http://127.0.0.1:5101/about` and verify that "cluster_state" is "READY" (might need to give it a minute or two)
23. Perform post install configuration.   See: [Post Install Configuration](post_install.md)


Sample .bashrc
--------------

These environment variables will be passed to the Docker containers on start up.

    export AWS_ACCESS_KEY_ID=1234567890            # Use SecretId from API Keys
    export AWS_SECRET_ACCESS_KEY=ABCDEFGHIJKL      # Use SecretKey from API Keys
    export BUCKET_NAME=hsdstest-123456789          # set to the name of the bucket you will be using
    export AWS_S3_GATEWAY=http://cos.ap-hongkong.myqcloud.com  # Use the endpoint given for your bucket
    export HSDS_ENDPOINT=http://hsds.hdf.test      # The DNS name of the instance  
    export LOG_LEVEL=INFO                          # Verbosity of server logs (DEBUG, INFO, WARN, or ERROR)
     
Sample override.yml
-------------------

Review the contents of **admin/config/config.yml** and create the file **admin/config/override.yml** for any keys where you don't wish to use the default value. E.g. "server_name".   


Installing Software Updates
---------------------------

To get the latest codes changes from the HSDS repo do the following:

1. Shutdown the service: `$ stopall.sh`
2. Get code changes: `$ git pull`
3. Build a new Docker image: `$ ./build/sh`
4. Start the service: `$ ./runall.sh`
