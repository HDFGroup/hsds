import sqlite3
 

#
# Create tables for sqlite db
#
def dbInitTable(conn):
    c = conn.cursor()

    # Create group table  
    # id format: <rootid>#<objid>
    c.execute("CREATE TABLE GroupTable (id TEXT PRIMARY KEY)") 
    c.execute("ALTER TABLE  GroupTable ADD COLUMN etag TEXT") 
    c.execute("ALTER TABLE  GroupTable ADD COLUMN size INTEGER")
    c.execute("ALTER TABLE  GroupTable ADD COLUMN lastModified INTEGER")
    conn.commit()

    # Create type table  
    # id format: <rootid>#<objid>
    c.execute("CREATE TABLE TypeTable (id TEXT PRIMARY KEY)") 
    c.execute("ALTER TABLE  TypeTable ADD COLUMN etag TEXT") 
    c.execute("ALTER TABLE  TypeTable ADD COLUMN size INTEGER")
    c.execute("ALTER TABLE  TypeTable ADD COLUMN lastModified INTEGER")
    conn.commit()

    # Create dataset table  
    # id format: <rootid>#<objid>
    c.execute("CREATE TABLE DatasetTable (id TEXT PRIMARY KEY)") 
    c.execute("ALTER TABLE  DatasetTable ADD COLUMN etag TEXT") 
    c.execute("ALTER TABLE  DatasetTable ADD COLUMN size INTEGER")
    c.execute("ALTER TABLE  DatasetTable ADD COLUMN lastModified INTEGER")
    c.execute("ALTER TABLE  DatasetTable ADD COLUMN chunkCount INTEGER")
    c.execute("ALTER TABLE  DatasetTable ADD COLUMN totalSize INTEGER")
    conn.commit()

    # Create Chunk Table
    c.execute("CREATE TABLE ChunkTable (id TEXT PRIMARY KEY)") 
    c.execute("ALTER TABLE  ChunkTable ADD COLUMN etag TEXT") 
    c.execute("ALTER TABLE  ChunkTable ADD COLUMN size INTEGER")
    c.execute("ALTER TABLE  ChunkTable ADD COLUMN lastModified INTEGER")
    conn.commit()

    # Create Root Table
    c.execute("CREATE TABLE RootTable (id TEXT PRIMARY KEY)") 
    c.execute("ALTER TABLE  RootTable ADD COLUMN etag TEXT") 
    c.execute("ALTER TABLE  RootTable ADD COLUMN size INTEGER")
    c.execute("ALTER TABLE  RootTable ADD COLUMN lastModified INTEGER")
    c.execute("ALTER TABLE  RootTable ADD COLUMN chunkCount INTEGER")
    c.execute("ALTER TABLE  RootTable ADD COLUMN groupCount INTEGER")
    c.execute("ALTER TABLE  RootTable ADD COLUMN datasetCount INTEGER")
    c.execute("ALTER TABLE  RootTable ADD COLUMN typeCount INTEGER")
    c.execute("ALTER TABLE  RootTable ADD COLUMN totalSize INTEGER")
    
    
    conn.commit()

    # Create Domain Table
    c.execute("CREATE TABLE DomainTable (id TEXT PRIMARY KEY)") 
    c.execute("ALTER TABLE  DomainTable ADD COLUMN etag TEXT") 
    c.execute("ALTER TABLE  DomainTable ADD COLUMN size INTEGER")
    c.execute("ALTER TABLE  DomainTable ADD COLUMN lastModified INTEGER")
    c.execute("ALTER TABLE  DomainTable ADD COLUMN root TEXT")
    c.execute('ALTER TABLE  DomainTable ADD COLUMN owner TEXT') 
    conn.commit()


#
# Get primary key given objid and rootid (for Group, Type, and Dataset tables)
#
def getPrimaryKey(table, objid, rootid=None):
    if table in ("GroupTable", "TypeTable", "DatasetTable"):
        if not rootid:
            raise ValueError("No rootid suplied")
        id = "{}#{}".format(rootid, objid)
    elif table in ("ChunkTable", "RootTable", "DomainTable"):
        id = objid # For other tables, primary key is just objid (or domain name)
    else:
        raise ValueError("Invalid table name: {}".format(table))
    return id

#
# Get Table name for given objid
def getTableForId(objid):
    if objid.startswith("g-"):
        table = "GroupTable"
    elif objid.startswith("d-"):
        table = "DatasetTable"
    elif objid.startswith("t-"):
        table = "TypeTable"
    elif objid.startswith("c-"):
        table = "ChunkTable"
    elif objid.startswith("/"):
        table = "DomainTable"
    else:
        raise ValueError("Unexpected objid: {}".format(objid))
    return table

#
# Get the column name in RootTable for given id
#
def getCountColumnName(objid):
    if objid.startswith("g-"):
        col_name = "groupCount"
    elif objid.startswith("d-"):
        col_name = "datasetCount"
    elif objid.startswith("t-"):
        col_name = "typeCount"
    elif objid.startswith("c-"):
        col_name = "chunkCount"
    else:
        col_name = None
    return col_name
 


#
# Add given row to table
#
def insertRow(conn, objid, etag='', objSize=0, lastModified=0, rootid='', owner='', table=None):
    if not table:
        table = getTableForId(objid)
    # For Group, Type, and Dataset table, primary key is <objid>#<rootid>

    id = getPrimaryKey(table, objid, rootid)
    query = "INSERT INTO {} (id, etag, size, lastModified".format(table)
    if table == "DomainTable":
        query += ", root, owner)"
    elif table == "RootTable":
        query += ", chunkCount, groupCount, datasetCount, typeCount, totalSize)"
    else:
        query += ")"
    query += " VALUES ('{}', '{}', {}, {}".format(id, etag, objSize, lastModified)
    if table == "DomainTable":
        query += ", '{}', '{}')".format(rootid, owner)
    elif table == "RootTable":
        query += ", {}, {}, {}, {}, {})".format(0,0,0,0,0)
    else:
        query += ")"
    c = conn.cursor()
    try:
        c.execute(query )
    except sqlite3.IntegrityError as e:
        raise KeyError("Error, id already exists: {}, error: {}".format(id, e))
    conn.commit()

#
# Update specified column in db row
#
def updateRowColumn(conn, objid, column, value, rootid=0, table=None):
    if not table:
        table = getTableForId(objid)
    id = getPrimaryKey(table, objid, rootid)
    query = "UPDATE {} SET {}={} WHERE id='{}'".format(table, column, value, id)
    c = conn.cursor()
    try:
        c.execute(query)
    except sqlite3.Error as e:
        raise KeyError("Error updating table, error: {}".format(e))
    conn.commit()

#
# Delete the given row
#
def deleteRow(conn, objid, rootid=0, table=None):
    if not table:
        table = getTableForId(objid)
    id = getPrimaryKey(table, objid, rootid)
    query = "DELETE FROM {} WHERE id='{}'".format(table, id)
    c = conn.cursor()
    try:
        c.execute(query)
    except sqlite3.Error as e:
        raise KeyError("Error deleting row from table, error: {}".format(e))
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
# Get info on given id
#
def getRow(conn, objid, rootid=None, table=None):
    if not table:
        table = getTableForId(objid)
         
    id = getPrimaryKey(table, objid, rootid)
    c = conn.cursor()
    
    c.execute("SELECT * FROM {} WHERE id='{}'".format(table, id))
    all_rows = c.fetchall()
    if len(all_rows) == 0:
        return None  # not found
    if len(all_rows) > 1:
        # domain is primary key, so this shouldn't happen
        raise KeyError("Unexpected result for query")
    row = all_rows[0]
    result = {"id": id}
    result["etag"] = row[1]
    result["size"] = row[2]
    result["lastModified"] = row[3]
    if table == "DatasetTable":
        result["chunkCount"] = row[4]
        result["totalSize"] = row[5]
    elif table == "RootTable":
        result["chunkCount"] = row[4]
        result["groupCount"] = row[5]
        result["datasetCount"] = row[6]
        result["typeCount"] = row[7]
        result["totalSize"] = row[8]
    elif table == "DomainTable":
        result["root"] = row[4]
        result["owner"] = row[5]
    
    return result

# 
# Get all rows from rootTable
#
def getRootObjects(conn):
    c = conn.cursor()
    c.execute("SELECT * FROM RootTable")
    all_rows = c.fetchall()
    results = []
    for row in all_rows:
        rootid = row[0]
        result = {"id": rootid}
        result["etag"] = row[1]
        result["size"] = row[2]
        result["lastModified"] = row[3]
        results.append(result)
    return results



#
# Get domains with given prefix
#
def getDomains(conn, prefix, limit=None, marker=None):
    c = conn.cursor()
    query = "SELECT * from DomainTable WHERE id LIKE '{}%'".format(prefix)
    c.execute(query)
    all_rows = c.fetchall()
    domains = []
    for row in all_rows:
        id = row[0]
        # don't include sub-items of this folder
        if id[len(prefix):].find('/') == -1:
            domain = {"name": id}
            if marker:
                if marker == id:
                    marker = None
                continue
            
            if row[4]:
                domain["root"] = row[4]
            if row[5]:
                domain["owner"] = row[5]
            domains.append(domain)
            if limit and len(domains) == limit:
                break
    return domains


#
# Get all chunks for given dataset
#
def getDatasetChunks(conn, dsetid, limit=None):
    c = conn.cursor()
    chunkid_prefix = "c" + dsetid[1:]
    query = "SELECT * FROM ChunkTable WHERE id LIKE '{}%'".format(chunkid_prefix)
    if limit:
        query += " LIMIT {}".format(limit)
    c.execute(query)
    all_rows = c.fetchall()
    results = {}
    for row in all_rows:
        id = row[0]
        etag = row[1]
        size = row[2]
        lastModified = row[3]
        results[id] = {"etag":etag, "size": size, "lastModified": lastModified}
    return results

#
# Iterate through all objects
#
def listObjects(conn, rootid=None, limit=None):
    objs = {}
    c = conn.cursor()
    tableCollectionMap = {"Group": "groups", "Dataset": "datasets", "Type": "datatypes"}
    for table in tableCollectionMap:
        query = "SELECT * FROM {}Table".format(table)
        if rootid:
            query += " WHERE id LIKE '{}%'".format(rootid)
        if limit:
            query += " LIMIT {}".format(limit)
        for row in c.execute(query):
            rowid = row[0]
            parts = rowid.split('#')
            rootid = parts[0]
            objid = parts[1]
            if rootid not in objs:
                objs[rootid] = {"groups": {}, "datasets": {}, "datatypes": {}, "size": 0, "lastModified": 0, "chunkCount": 0}
            root = objs[rootid]
            obj = {} 
            obj["size"] = row[2]
            obj["lastModified"] = row[3]
            col = root[tableCollectionMap[table]]  # root's groups, datasets, or datatypes
            col[objid] = obj
             
    return objs 





     

