import sys
import sqlite3
from dbutil import getDomain, getRootObjects, getDatasetChunks
import config
    

#
# Main
#

if __name__ == '__main__':
      
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("Usage: python getdomaininfo.py [-v|-V] <domain_name>")
        sys.exit(1)
    verbose = False
    show_chunks = False
    domain = None
    if sys.argv[1] == "-v" and len(sys.argv) > 2:
        verbose = True
        domain = sys.argv[2]
    elif sys.argv[1] == "-V" and len(sys.argv) > 2:
        verbose = True
        show_chunks = True
        domain = sys.argv[2]
    else:
        domain = sys.argv[1]

    db_file = config.get("db_file")
    conn = sqlite3.connect(db_file)

    result = getDomain(conn, domain)

    

    if result is None:
        print("domain not found")
    else:
        print(result)

    domain_size = result["size"]
    object_count = 1  # the domain object

    dataset_ids = []
    
    if verbose and result["root"]:
        rows = getRootObjects(conn, result["root"])
        print("got: {} objects".format(len(rows)))
        for row in rows:
            print(row)
            object_count += 1
            domain_size += row["size"]
            if row["id"].startswith("d-"):
                dataset_ids.append(row["id"])
    
    print("")
    if verbose:
        for dataset_id in dataset_ids:
            print("dataset {}...".format(dataset_id))
            num_chunks = 0
            allocated_bytes = 0
            rows = getDatasetChunks(conn, dataset_id)
            for row in rows:
                if show_chunks:
                    print(row)
                num_chunks += 1
                allocated_bytes += row["size"]
                object_count += 1
                domain_size += row["size"]
            print("num_chunks: {}  allocated bytes; {}".format(num_chunks, allocated_bytes))

        if verbose and object_count > 0:
            print("object_count: {}".format(object_count))
            print("domain size: {}".format(domain_size))


    conn.close()

    print("done!")
     