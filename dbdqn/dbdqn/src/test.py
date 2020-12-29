import shelve
import time
import MySQLdb
import numpy as np
from mydb2 import Mydb

if __name__ == '__main__':
    episode = 250
    mydb = Mydb.load_mydb2(episode)
    print(mydb.plain_search())
    print(mydb.get_total_time())
