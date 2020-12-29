import shelve
import time
from datetime import datetime

import MySQLdb
from constants import *


class Myindex:
    workload = []  # sql语句负载

    def __init__(self):
        self.LOGGING = MyindexLogger()

        self.conn = MySQLdb.connect(host='127.0.0.1',
                                    user='root', passwd=MYSQL_PWD,
                                    port=3306, db='testlubm', charset='utf8')  # testlubm, testwatdiv
        self.cur = self.conn.cursor()
        self.__load_workload()
        self.cur.execute("show tables")
        self.table_names = [tname[0] for tname in self.cur.fetchall()]
        self.tnums = len(self.table_names) - 1
        self.columns = list()

        self.id2tnum = {0: 0, 1: 1, 2: 2, 3: 3, 4: 5, 5: 6, 6: 7, 7: 8, 8: 9, 9: 20, 10: 21}  # 共11个表 testlubm
        # ts = [0,1,2,3,4,6,7,8,9,10,11,12,13,14,16,19,21] # 共17个表 testwatdiv
        # self.id2tnum = {i:ts[i] for i in range(len(ts))}

        for i in range(self.tnums + 1):
            tname = self.table_names[i]
            self.cur.execute("DESCRIBE " + tname)
            for col in self.cur.fetchall():
                if 'p' not in col[0]:
                    self.columns.append(tname + "." + col[0])
        self.indices = [0] * len(self.columns)
        self.last_time = None
        self.visited_states = list()

        self.reset()

    def indices2state(self, indices):
        state = [0 for i in range(3 * len(indices))]
        for col, index_type in enumerate(indices):
            state[3 * col + index_type] = 1
        return state

    def reset(self):
        self.LOGGING.write("Reset...", True)
        self.indices = [0] * len(self.columns)
        self.visited_states = [self.indices.copy()]

        self.last_time = self.get_total_time()
        self.LOGGING.write("initial time: {0} s".format(self.last_time))
        self.LOGGING.write("Drop all indices.")
        for i in range(self.tnums + 1):
            tname = self.table_names[i]
            self.cur.execute("show index from " + tname)
            all_indices = self.cur.fetchall()
            for each_index in all_indices:
                self.cur.execute("DROP INDEX {0} ON {1}".format(each_index[2], each_index[0]))
        self.LOGGING.write("Create initial hash indices.")
        for i in range(self.tnums + 1):
            tname = self.table_names[i]
            sql = "CREATE INDEX sindex USING hash ON {0}(s)".format(tname)
            self.LOGGING.write(sql)
            self.cur.execute(sql)
        self.conn.commit()
        self.LOGGING.write("Reset completed", True)
        return self.indices2state(self.indices)

    def judge_legal(self):
        abandon_list = []
        action_dim = 3 * len(self.indices)
        for action in range(action_dim):
            index_type = action % 3
            tname_i = action // 3
            new_indices = self.indices.copy()
            new_indices[tname_i] = index_type
            if new_indices in self.visited_states:
                abandon_list.append(action)
        return abandon_list

    def step(self, action):
        index_type = action % 3
        tname_i = action // 3
        tname, col = self.columns[tname_i].split(".")
        if index_type == 0:
            self.cur.execute("show index from " + tname)
            for each_index in self.cur.fetchall():
                if each_index[2] == col + "index":
                    sql = "DROP INDEX {0}index ON {1}".format(col, tname)
                    self.cur.execute(sql)
                    self.conn.commit()
                    self.indices[tname_i] = index_type
                    self.LOGGING.write(sql)
                    break
        elif index_type == 1:
            delect_sql = "DROP INDEX {0}index ON {1}".format(col, tname)
            create_sql = "CREATE INDEX {0}index USING hash ON {1}({0})".format(col, tname)
            self.cur.execute("show index from " + tname)
            for each_index in self.cur.fetchall():
                if each_index[2] == col + "index":
                    self.cur.execute(delect_sql)
                    self.conn.commit()
                    break
            self.cur.execute(create_sql)
            self.conn.commit()
            self.indices[tname_i] = index_type

            self.LOGGING.write(delect_sql)
            self.LOGGING.write(create_sql)
        elif index_type == 2:
            delect_sql = "DROP INDEX {0}index ON {1}".format(col, tname)
            create_sql = "CREATE INDEX {0}index USING btree ON {1}({0})".format(col, tname)
            self.cur.execute("show index from " + tname)
            for each_index in self.cur.fetchall():
                if each_index[2] == col + "index":
                    self.cur.execute(delect_sql)
                    self.conn.commit()

            self.cur.execute(create_sql)
            self.conn.commit()
            self.indices[tname_i] = index_type

            self.LOGGING.write(delect_sql)
            self.LOGGING.write(create_sql)

        self.visited_states.append(self.indices.copy())

        temp = self.get_total_time()
        reward = (self.last_time - temp) * 1000
        self.last_time = temp

        return self.indices2state(self.indices), reward, False

    def save_myindex(self, episode):
        myindex_dump = {'tnums': self.tnums,
                        'columns': self.columns,
                        'indices': self.indices}

        with shelve.open(PATH + 'data/myindex_dump/myindex_dump' + str(episode)) as fw:
            for key in myindex_dump:
                fw[key] = myindex_dump[key]

    def get_total_time(self):
        total_time = 0
        for sql in self.workload:
            time_start = time.time()
            try:
                self.cur.execute(sql)
                self.cur.fetchall()
                self.conn.commit()
            except:
                self.conn.rollback()
            time_end = time.time()
            total_time += time_end - time_start
        self.LOGGING.write("total time: {0}s".format(str(total_time)))
        return total_time

    def __load_workload(self, path=PATH + "data/lubm_sql_rewritten.txt"):
        with open(path, "r") as f:
            self.workload = f.readlines()


class MyindexLogger:
    """
    mydb日志管理
    """

    def __init__(self, path=PATH + 'data/myindex-log.txt'):
        self.f = open(path, 'w')

    def write(self, content, print_flag=False):
        """
        写入日志
        :param content: 写入日志的内容，写入时转换为str
        :param print_flag: True，打印到控制台；False，不打印到控制台
        :return:
        """
        if print_flag:
            print(content)
        curr_time = datetime.now()
        self.f.write(curr_time.strftime("%Y-%m-%d %H:%M:%S") + "\t" + str(content) + "\n")
        self.f.flush()

    def close(self):
        self.f.close()


if __name__ == '__main__':
    myindex = Myindex()
    print(myindex.step(1))
    # myindex.reset()
