import re
import sys
import time

import MySQLdb
from constants import *

with open(sys.argv[1], "r") as f:
    actions = f.readlines()

conn = MySQLdb.connect(host='127.0.0.1',
                       user='root', passwd=MYSQL_PWD,
                       port=3306, db='testlubm', charset='utf8')  # testlubm, testwatdiv
cur = conn.cursor()

cur.execute("show tables")
table_names = [tname[0] for tname in cur.fetchall()]
tnums = len(table_names) - 1
for i in range(tnums + 1):
    tname = table_names[i]
    cur.execute("show index from " + tname)
    all_indices = cur.fetchall()
    for each_index in all_indices:
        cur.execute("DROP INDEX {0} ON {1}".format(each_index[2], each_index[0]))
for action in actions:
    action = action.strip()
    print(action)
    match = re.match("create (.*) index on (.*) on (.*)", action)
    if match:
        index_type = match.group(1)
        column = match.group(2)
        table = match.group(3)
    sql = "CREATE INDEX {0}index USING {1} ON {2}({0})".format(column, index_type, table)
    cur.execute(sql)

conn.commit()

with open(PATH + "data/lubm_sql_rewritten.txt", "r") as f:
    workload = f.readlines()
total_time = 0
for sql in workload:
    time_start = time.time()
    cur.execute(sql)
    cur.fetchall()
    time_end = time.time()
    total_time += time_end - time_start
with open(PATH + "data/log/lubm_contrast.txt", "w") as f:
    f.write(str(total_time))

cur.close()
conn.close()