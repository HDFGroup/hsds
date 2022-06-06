import boto.ec2
import config

region = config.get("aws_region")
conn = boto.ec2.connect_to_region(region)
reservations = conn.get_all_instances()
fields = ("id", "public ip", "private ip", "name", "subnet", "state")
format_str = "{:<20} {:<16} {:<16} {:<16} {:<16} {:<12}"
print(format_str.format(*fields))
sep = ("-" * 12,) * 6
print(format_str.format(*sep))
for res in reservations:
    for inst in res.instances:
        name = "<none>"
        if "Name" in inst.tags:
            name = inst.tags["Name"]
        if inst.ip_address is None:
            inst.ip_address = "<none>"
        if inst.private_ip_address is None:
            inst.private_ip_address = "<none>"
        if inst.subnet_id is None:
            inst.subnet_id = "<none>"
        print(
            format_str.format(
                inst.id,
                inst.ip_address,
                inst.private_ip_address,
                name,
                inst.subnet_id,
                inst.state,
            )
        )
