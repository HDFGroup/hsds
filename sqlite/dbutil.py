import sqlite3
 

#
# Create tables for sqlite db
#
def dbInitTable(conn):
    c = conn.cursor()

    # Create object table (groups, datasets, and datatypes)
    # id format: <rootid>#<objid>
    c.execute("CREATE TABLE ObjectTable (id TEXT PRIMARY KEY)") 
    c.execute("ALTER TABLE  ObjectTable ADD COLUMN etag TEXT") 
    c.execute("ALTER TABLE  ObjectTable ADD COLUMN size INTEGER")
    c.execute("ALTER TABLE  ObjectTable ADD COLUMN lastModified TEXT")
    conn.commit()

    # Create Chunk Table
    c.execute("CREATE TABLE ChunkTable (id TEXT PRIMARY KEY)") 
    c.execute("ALTER TABLE  ChunkTable ADD COLUMN etag TEXT") 
    c.execute("ALTER TABLE  ChunkTable ADD COLUMN size INTEGER")
    c.execute("ALTER TABLE  ChunkTable ADD COLUMN lastModified TEXT")
    conn.commit()

    # Create Domain Table
    c.execute("CREATE TABLE DomainTable (id TEXT PRIMARY KEY)") 
    c.execute("ALTER TABLE  DomainTable ADD COLUMN etag TEXT") 
    c.execute("ALTER TABLE  DomainTable ADD COLUMN size INTEGER")
    c.execute("ALTER TABLE  DomainTable ADD COLUMN lastModified TEXT")
    c.execute("ALTER TABLE  DomainTable ADD COLUMN root TEXT") 
    conn.commit()

    # Create Top Level Domain Table
    c.execute("CREATE TABLE TLDTable (id TEXT PRIMARY KEY)") 
    conn.commit()

#
# Add given domain to domain table
#
def insertDomainTable(conn, id, etag='', objSize=0, lastModified='', rootid='' ):
    c = conn.cursor()
    try:
        c.execute("INSERT INTO DomainTable (id, etag, size, lastModified, root) VALUES ('{}', '{}', {}, '{}', '{}')". \
          format(id, etag, objSize, lastModified, rootid) )
    except sqlite3.IntegrityError:
        raise KeyError("Error, id already exists: {}".format(id))
    conn.commit()

#
# Add given domain to Top Level Domain table
#
def insertTLDTable(conn, id ):
    c = conn.cursor()
    try:
        c.execute("INSERT INTO TLDTable (id) VALUES ('{}')". \
          format(id) )
    except sqlite3.IntegrityError:
        raise KeyError("Error, id already exists: {}".format(id))
    conn.commit()

#
# Add given chunk to chunk table
#
def insertChunkTable(conn, id, etag='', objSize=0, lastModified=''):
    c = conn.cursor()
    try:
        c.execute("INSERT INTO ChunkTable (id, etag, size, lastModified) VALUES ('{}', '{}', {}, '{}')". \
          format(id, etag, objSize, lastModified) )
    except sqlite3.IntegrityError:
        raise KeyError("Error, id already exists: {}".format(id))
    conn.commit()

#
# Add batch of chunks to chunk table (much faster than inserting one by one)
#
def batchInsertChunkTable(conn, items):
    c = conn.cursor()
    try:
        c.executemany("INSERT INTO ChunkTable (id, etag, size, lastModified) VALUES (?, ?, ?, ?)", 
          items)
    except sqlite3.IntegrityError as se:
        raise KeyError("Error, id already exists: {}".format(se))
    conn.commit()

#
# Add given object to object table
#
def insertObjectTable(conn, id, etag='', objSize=0, lastModified='', rootid=None):
    if not rootid:
        raise ValueError("No root id supplied")
    c = conn.cursor()
    try:
        # create a concatenation of rootid and objid so we can efficiently find all objects in a domain
        key = rootid + '#' + id
        c.execute("INSERT INTO ObjectTable (id, etag, size, lastModified) VALUES ('{}', '{}', {}, '{}')". \
          format(key, etag, objSize, lastModified) )
    except sqlite3.IntegrityError:
        raise KeyError("Error, id already exists: {}".format(id))
    conn.commit()

#
# Get info on the given domain
#
def getDomain(conn, domain):
    c = conn.cursor()
    c.execute("SELECT * FROM DomainTable WHERE id='{}'".format(domain))
    all_rows = c.fetchall()
    if len(all_rows) == 0:
        return None  # not found
    if len(all_rows) > 1:
        # domain is primary key, so this shouldn't happen
        raise KeyError("Unexpected result for domain query")
    row = all_rows[0]
    result = {"domain": domain}
    result["etag"] = row[1]
    result["size"] = row[2]
    result["lastModified"] = row[3]
    result["root"] = row[4]
    return result

# 
# Get all objects with given root
#
def getRootObjects(conn, root):
    c = conn.cursor()
    c.execute("SELECT * FROM ObjectTable WHERE id LIKE '{}%'".format(root))
    all_rows = c.fetchall()
    results = []
    for row in all_rows:
        key = row[0]
        if key[38] != '#':
            raise ValueError("Unexpected db key value: {}".format(key))
        objid = key[39:]
        result = {"id": objid}
        result["etag"] = row[1]
        result["size"] = row[2]
        result["lastModified"] = row[3]
        results.append(result)
    return results

#
# Get all Top-Level Domains
#
def getTopLevelDomains(conn):
    c = conn.cursor()
    c.execute("SELECT * FROM TLDTable ")
    all_rows = c.fetchall()
    results = []
    for row in all_rows:
        key = row[0]
        results.append(key)
    return results

#
# Get all chunks for given dataset
#
def getDatasetChunks(conn, dsetid):
    c = conn.cursor()
    chunkid_prefix = "c" + dsetid[1:]
    query = "SELECT * FROM ChunkTable WHERE id LIKE '{}%'".format(chunkid_prefix)
    #print("query: {}".format(query))
    c.execute(query)
    all_rows = c.fetchall()
    results = []
    for row in all_rows:
        result = {"id": row[0]}
        result["etag"] = row[1]
        result["size"] = row[2]
        result["lastModified"] = row[3]
        results.append(result)
    return results
     

