import re

from mydb2 import Mydb
import sys
from constants import *

mydb = Mydb(None, 'contrast')
filepath = sys.argv[1]
with open(filepath, "r") as f:
    actions = f.readlines()
for action in actions:
    action = action.strip()
    if action.strip().startswith("divide"):
        divide_match = re.match("divide t0 by (.*)", action.strip())
        if divide_match:
            p = divide_match.group(1)
            print(action)
            mydb.divide(p)
            print("done", action)
    elif action.strip().startswith("merge"):
        merge_match = re.match("merge.*by t(.*)\\.(.*) = t(.*)\\.(.*)", action.strip())
        if merge_match:
            tnum1 = int(merge_match.group(1))
            t1 = "t" + str(tnum1)
            c1 = merge_match.group(2)
            tnum2 = int(merge_match.group(3))
            t2 = "t" + str(tnum2)
            c2 = merge_match.group(4)
            cond = ["." + c1, "." + c2]
            print(tnum1)
            print(tnum2)
            print(mydb.table_attributes)
            length1 = len(mydb.table_attributes.get(tnum1))
            print(length1)
            length2 = len(mydb.table_attributes.get(tnum2))
            print(length2)
            length = length1 + length2
            maps = Mydb.init_tchar_maps_for_merge_fetch()
            if length == 6:
                choice = maps.get(6).index(cond)
            elif length == 9:
                if length1 == 6:
                    choice = maps.get(96).index(cond)
                elif length1 == 3:
                    choice = maps.get(93).index(cond)
            elif length == 12:
                if length1 == 3:
                    choice = maps.get(123).index(cond)
                elif length1 == 6:
                    choice = maps.get(126).index(cond)
                elif length1 == 9:
                    choice = maps.get(129).index(cond)
            print(action)
            mydb.merge(tnum1, tnum2, choice)
            print("done", action)
with open(PATH + "lubm_contrast.txt", "w") as f:
    f.write(str(mydb.get_total_time()))
