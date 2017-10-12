import sqlite3
import sys

 
def findS3Key(conn, s3id):
    c = conn.cursor()
    c.execute("SELECT * FROM S3Keys WHERE s3id='{}'".\
        format(s3id) )
    id_exists = c.fetchone()
    if id_exists:
        print("found: {}".format(id_exists))
    else:
        print("not found")
     

#
# Main
#

if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
    print("usage: python getkey.py <s3id>")
    sys.exit(1)

s3key = sys.argv[1]

DB_FILE = "mysqllite.db"
conn = sqlite3.connect(DB_FILE)
findS3Key(conn, s3key)
conn.close()
 
