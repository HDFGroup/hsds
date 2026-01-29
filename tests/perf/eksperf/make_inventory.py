import h5pyd
import s3fs
import config
import sys

if len(sys.argv) > 1:
    if sys.argv[1] in ("-h", "--help"):
        sys.exit(f"usage: python {sys.argv[0]} [folder] [tgt]")
    folder_path = sys.argv[1]
    if folder_path[-1] != "/":
        folder_path += "/"  # add a trailing slash to denote a folder
else:
    folder_path = None

if len(sys.argv) > 2:
    inventory_domain = sys.argv[2]
else:
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
    prefix = ""
    if folder_path.startswith("s3://"):
        s3 = s3fs.S3FileSystem()
        items = s3.ls(folder_path)
        prefix = "s3://"
    else:
        items = h5pyd.Folder(folder_path)
    for item in items:
        if prefix:
            filename = prefix + item 
        else:
            filename = folder_path + item
        print(f"adding: {filename}")
        table.append([(filename, 0, 0, 0, "")])
        

