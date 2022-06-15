import pkg_resources
import site
import sys
from . import config
from . import servicenode
from . import datanode
from . import rangeget_proxy
from . import headnode


def removeSitePackages():

    # site_packages = "/var/lang/lib/python3.9/site-packages"
    # but this is removing: "/home/sbx_user1051/.local/lib/python3.9/site-packages" on lambda?
    site_packages = site.getusersitepackages()
    if not site_packages:
        return
    print("sitepackages:", site_packages)

    try:
        sys.path.remove(site_packages)
    except ValueError as ve:
        print(f"site_package remove error: {ve}")
    else:
        sys.path.insert(0, site_packages)
        for dist in pkg_resources.find_distributions(site_packages, True):
            pkg_resources.working_set.add(dist, site_packages, False, replace=True)


def main():
    node_type = config.getCmdLineArg("node_type")
    if node_type is None:
        raise ValueError("no node_type argument found")
    if node_type not in ("sn", "dn", "head", "rn"):
        raise ValueError(f"Unexpected node type: {node_type}")
    print(f"hsds node main for node_type: {node_type}")
    print(f"python version: {sys.version}")
    print(f"sys path: {sys.path}")
    if config.getCmdLineArg("removesitepackages"):
        removeSitePackages()

    if node_type == "sn":
        servicenode.main()
    elif node_type == "dn":
        datanode.main()
    elif node_type == "rn":
        rangeget_proxy.main()
    elif node_type == "head":
        headnode.main()
    else:
        # shouldn't ever get here
        raise ValueError("unexpected error")


if __name__ == "__main__":
    main()
