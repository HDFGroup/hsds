import boto.ec2
import sys
import time
import config

if len(sys.argv) == 1 or sys.argv[1] == "-h" or sys.argv[1] == "--help":
    print("usage: python create_instance.py <name>")
    sys.exit(1)

tag_name  = sys.argv[-1]
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
instance_type =config.get("instance_type")
print("instance type: {}".format(instance_type))

conn = boto.ec2.connect_to_region(region)
reservation = conn.run_instances(hsds_ami,
    security_group_ids=[security_group_id,],
    instance_profile_name=profile_name, 
    subnet_id=subnet_id,
    key_name=key_name,
    instance_type=instance_type)

instance = reservation.instances[0]
print("created instance:", instance.id)
tags = {}
tags["Project"] = config.get("project_tag")
tags["Name"] = tag_name
print("creating instance tags: {}".format(tags))
conn.create_tags([instance.id,], tags)
is_online = False
while not is_online:
    statuses = conn.get_all_instance_status(instance_ids=[instance.id,])
    if len(statuses) < 1:
        print("waiting...")
        time.sleep(5)
        continue
    status = statuses[0]
    print("instance status: {}".format(status.instance_status.status))
    print("instance: ", instance.id, "status:", status.instance_status.status, "for instance:", tag_name)  # u'ok'
    if status.instance_status.status == u'ok':
        break
    print("waiting...")
    time.sleep(5)

print("instance id: {}".format(instance.id))    
print('done')