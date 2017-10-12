import sqlite3

def init_table(conn):
    c = conn.cursor()

    # Creating a new SQLite table with 1 column
    c.execute("CREATE TABLE S3Keys (s3id TEXT PRIMARY KEY)")  
    c.execute("ALTER TABLE  S3Keys ADD COLUMN size INTEGER")
    c.execute("ALTER TABLE  S3Keys ADD COLUMN created TEXT")
    conn.commit()

def insertS3Key(conn, s3id, size, created):
    c = conn.cursor()
    try:
        c.execute("INSERT INTO S3Keys (s3id, size, created) VALUES ('{}', {}, '{}')". \
          format(s3id, size, created) )
    except sqlite3.IntegrityError:
        print("Error, s3id already exists: {}".format(s3id))
    conn.commit()
    print("added ({} {} {})".format(s3id, size, created))

#
# Main
#

DB_FILE = "mysqllite.db"
TEXT_FILE = "s3data.txt"
conn = sqlite3.connect(DB_FILE)
init_table(conn)
line_number = 0
print("start")
with conn:
    with open(TEXT_FILE) as fileobject:
        for line in fileobject:
            line_number += 1
            #print("got line: {}".format(line))
            fields = line.split()
            # 2017-09-23 08:03:54    1589184 fffff-c-d237e7ec-85f3-11e7-bf89-0242ac110008_2482_13_11
            if len(fields) != 4:
                print("ignoring line {}: {}".format(line_number, line))
                continue
        
            create_date = fields[0] + ' ' + fields[1]
            if len(create_date) != 19:
                print("ignoring line (bad date) {}: {}".format(line_number, line))
                continue
            try:
                size = int(fields[2])
            except ValueError:
                print("ignoring line (bad size) {}: {}".format(line_number, line))
                continue
            s3id = fields[3]
            insertS3Key(conn, s3id, size, create_date)



conn.close()
