import MySQLdb
from constants import *

conn = MySQLdb.connect(host='127.0.0.1', user='root', passwd=MYSQL_PWD, port=3306, db='testlubm', charset='utf8')
cur = conn.cursor()
with open(PATH + "data/log/used_tables.txt", "r") as f:
    tnums = f.readlines()
used_tnames = set()
for tnum in tnums:
    used_tnames.add("t" + tnum.strip())
cur.execute("show tables")
table_names = [tname[0] for tname in cur.fetchall()]
for tname in table_names:
    if tname not in used_tnames:
        print(tname)
        cur.execute("drop table " + tname)
        conn.commit()

cur.close()
conn.close()