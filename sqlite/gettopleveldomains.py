import sqlite3
from dbutil import getTopLevelDomains 
import config
    

#
# Main
#

if __name__ == '__main__':
    db_file = config.get("db_file")
    conn = sqlite3.connect(db_file)

    result = getTopLevelDomains(conn)


    if result is None:
        print("no domains found")
    else:
        print(result)

    conn.close()

    print("done!")
     