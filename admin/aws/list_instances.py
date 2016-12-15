import boto.ec2
conn = boto.ec2.connect_to_region("us-west-2")
reservations = conn.get_all_instances()
for res in reservations:
    for inst in res.instances:
        if 'Name' in inst.tags:
            print "%s id: %s public: %s private: %s state: %s]" % (inst.tags['Name'], inst.id, inst.ip_address, inst.private_ip_address, inst.state)
        else:
            print "%s public: %s private: %s state: %s" % (inst.id, inst.ip_address, inst.private_ip_address, inst.state)

