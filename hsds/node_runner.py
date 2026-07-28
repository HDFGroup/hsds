import sys
from . import config
from . import servicenode
from . import datanode
from . import headnode


def main():
    node_type = config.getCmdLineArg("node_type")
    if node_type is None:
        raise ValueError("no node_type argument found")
    if node_type not in ("sn", "dn", "head", "rn"):
        raise ValueError(f"Unexpected node type: {node_type}")
    print(f"hsds node main for node_type: {node_type}")
    print(f"python version: {sys.version}")
    print(f"sys path: {sys.path}")

    if node_type == "sn":
        servicenode.main()
    elif node_type == "dn":
        datanode.main()
    elif node_type == "head":
        headnode.main()
    else:
        # shouldn't ever get here
        raise ValueError("unexpected error")


if __name__ == "__main__":
    main()
