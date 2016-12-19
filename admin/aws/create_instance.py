import boto.ec2
import sys
import time
import config

if len(sys.argv) == 1 or sys.argv[1] == "-h" or sys.argv[1] == "--help":
    print("usage: python create_instance.py <name> <count>")
    print("   Note: if name is 'node', then the hsds_sn and hsds_dn services will be started")
    sys.exit(1)

count = 1
if len(sys.argv) > 2:
    count = int(sys.argv[2])

status_code = None

tag_name  = sys.argv[1]
print("count: {}".format(count))
print("tag_name: {}".format(tag_name))
startup_script = None
if tag_name == "node":
    startup_script = """#!/bin/sh
#start hsds_dn and hsds_sn services
systemctl start hsds_sn
systemctl start hsds_dn
"""


region = config.get("aws_region")
print("region: {}".format(region))
hsds_ami = config.get("hsds_ami")
print("ami: {}".format(hsds_ami))
security_group_id = config.get("security_group_id")
print("security group {}".format(security_group_id))
profile_name = config.get("profile_name")
print("profile name: {}".format(profile_name))
subnet_id = config.get("subnet_id")
print("subnet_id: {}".format(subnet_id))
key_name = config.get("key_name")
print("key name: {}".format(key_name))
instance_type = config.get("instance_type")
print("instance type: {}".format(instance_type))

conn = boto.ec2.connect_to_region(region)
reservation = conn.run_instances(hsds_ami,
    security_group_ids=[security_group_id,],
    user_data=startup_script,
    instance_profile_name=profile_name, 
    subnet_id=subnet_id,
    key_name=key_name,
    instance_type=instance_type,
    min_count=count,
    max_count=count)

instance_ids = []
for instance in reservation.instances:
    print("created instance:", instance.id)
    instance_ids.append(instance.id)

if len(instance_ids) != count:
    print("expected {} instances, but got: {}".format(len(instance_ids), count))


tags = {}
tags["Project"] = config.get("project_tag")
tags["Name"] = tag_name
tags["HSDS"] = "hsds"
print("creating instance tags: {}".format(tags))
conn.create_tags(instance_ids, tags)

while True:
    statuses = conn.get_all_instance_status(instance_ids=instance_ids)
    if len(statuses) < 1:
        print("waiting...")
        time.sleep(5)
        continue
    online_count = 0
    for status in statuses:
        print("instance status: {}".format(status.instance_status.status))
        if status.instance_status.status in (u'ok', 'ok'):
            online_count += 1
    print("online count: {}".format(online_count))
    if online_count == count:
        break  # done
    print("waiting...")
    time.sleep(5)

print("instance ids: {}".format(instance_ids))  

# create a public IP address
print("allocating public ips...")
for i in range(count):
    address = conn.allocate_address(domain='vpc')
    address.associate(instance_ids[i])
    print("public ip: {}".format(address.public_ip))

print('done')