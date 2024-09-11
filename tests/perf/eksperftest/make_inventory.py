import h5pyd
import config
import sys

if len(sys.argv) > 1:
    if sys.argv[1] in ("-h", "--help"):
        sys.exit(f"usage: python {sys.argv[0]} [folder]")
    folder_path = sys.argv[1]
else:
    folder_path = None

inventory_domain = config.get("inventory_domain")
print("creating inventory domain:", inventory_domain)
f = h5pyd.File(inventory_domain, "x")
dt=[("filename", "S64"), ("start", "i8"), ("done", "i8"), ('status', "i4"), ('podname', "S40",)]
table = f.create_table("inventory", dtype=dt)

if config.get("public_read"):
    # make public read, and get acl
    acl = {"userName": "default"}
    acl["create"] = False
    acl["read"] = True
    acl["update"] = False
    acl["delete"] = False
    acl["readACL"] = True
    acl["updateACL"] = False
    f.putACL(acl)
    f.close()

print(f"created inventory: {inventory_domain}")

print(table)

if folder_path:
    folder = h5pyd.Folder(folder_path)
    for k in folder:
        filename = folder_path + k
        print(f"adding: {filename}")
        table.append([(filename, 0, 0, 0, "")])
        

