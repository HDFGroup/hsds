import boto.ec2
import config

conn = boto.ec2.connect_to_region(config.get("aws_region"))
reservations = conn.get_all_instances()
for res in reservations:
    for inst in res.instances:
        if 'Name' in inst.tags:
            print "%s (%s) [%s]" % (inst.tags['Name'], inst.id, inst.state)
        else:
            print "%s [%s]" % (inst.id, inst.state)
