import re
import shelve
import time

from contrast1.frequency_stats import FrequencyStats
from parse_join import JoinParser

import pymysql
from constants import *


class Contrast:
    def __init__(self, workload_path):

        self.conn = pymysql.connect(host='127.0.0.1', user='root', passwd=MYSQL_PWD, port=3319, db='contrast',
                                    charset='utf8')  # 连接数据库
        self.cur = self.conn.cursor()

        file = shelve.open("../../data/contrast/workload_stats")
        self.frequency = file['frequency']
        self.p_conditions_set = file['p_conditions_set']
        self.join_conditions_tuple_list = file['join_conditions_tuple_list']
        file.close()

        self.workload_path = workload_path
        self.workloads = []

        self.max_tnum = 0
        self.hashtp = dict()
        self.hashtp2 = {}
        self.all_sql_pri_results = dict()

    def load_workload(self, savepath):
        join_paser = JoinParser("t0")
        f1 = shelve.open("../../data/contrast/priority.dat")
        for key in f1.keys():
            self.workloads.append(join_paser.parse_transfer(key))
            self.all_sql_pri_results[join_paser.parse_transfer(key)] = f1[key]
        f1.close()

        print("workloads:")
        print(self.workloads)
        print("all_sql_pri_results:")
        print(self.all_sql_pri_results)

        frequency_stats = FrequencyStats(self.workload_path, savepath)
        frequency_stats.statistics()

    def __create_single_table(self):
        self.max_tnum += 1  # 每次建表最大序号+1
        tnum = self.max_tnum  # 当前表序号
        tname = 't' + str(tnum)  # 当前表名
        sql = 'CREATE TABLE IF NOT EXISTS ' + tname + \
              ' (p VARCHAR(255) NOT NULL,' \
              's VARCHAR(255) NOT NULL,' \
              'o VARCHAR(255) NOT NULL' \
              ') charset utf8'
        self.cur.execute(sql)
        self.conn.commit()

    def __create_two_merged_table(self):
        self.max_tnum += 1  # 每次建表最大序号+1
        tnum = self.max_tnum  # 当前表序号
        tname = 't' + str(tnum)  # 当前表名
        sql = 'CREATE TABLE IF NOT EXISTS ' + tname + \
              ' (p VARCHAR(255) NOT NULL,' \
              's VARCHAR(255) NOT NULL,' \
              'o VARCHAR(255) NOT NULL,' \
              'bp VARCHAR(255) NOT NULL,' \
              'bs VARCHAR(255) NOT NULL,' \
              'bo VARCHAR(255) NOT NULL' \
              ') charset utf8'
        self.cur.execute(sql)
        self.conn.commit()

    def create_single_view(self):
        for p in self.p_conditions_set:
            self.cur.execute("select * from t0 where p = %s", (p))
            results = self.cur.fetchall()
            if results:
                self.__create_single_table()
                self.cur.executemany("insert into t" + str(self.max_tnum) + "(p,s,o) VALUES (%s,%s,%s)", results)
                self.conn.commit()
                self.hashtp[self.max_tnum] = p
                self.hashtp2[self.max_tnum] = {p}
                print("表t" + str(self.max_tnum) + "创建并加载完成")

    def create_two_merged_view(self):
        for join_tuple in self.join_conditions_tuple_list:
            a = None
            b = None
            ta = None
            tb = None
            for k, v in self.hashtp.items():
                if join_tuple[0] == v:
                    a = k
                    ta = "t" + str(a)
                if join_tuple[1] == v:
                    b = k
                    tb = "t" + str(b)
            if ta and tb:
                sql = None
                if join_tuple[2] == 1:
                    sql = "select * from {0}, {1} where {0}.o = {1}.o".format(ta, tb)
                elif join_tuple[2] == 2:
                    sql = "select * from {0}, {1} where {0}.o = {1}.s".format(ta, tb)
                elif join_tuple[2] == 3:
                    sql = "select * from {0}, {1} where {0}.s = {1}.o".format(ta, tb)
                elif join_tuple[2] == 4:
                    sql = "select * from {0}, {1} where {0}.s = {1}.s".format(ta, tb)
                else:
                    print("join_tuple值非法！")
                print(sql)
                if sql:
                    self.cur.execute(sql)
                    results = self.cur.fetchall()
                    if results:
                        self.__create_two_merged_table()
                        self.cur.executemany(
                            "INSERT INTO t" + str(self.max_tnum) + "(p,s,o,bp,bs,bo) VALUES (%s,%s,%s,%s,%s,%s)",
                            results)
                        self.conn.commit()
                        if join_tuple[2] == 1:
                            self.hashtp2[self.max_tnum] = {"{0}.o={1}.o".format(join_tuple[0], join_tuple[1])}
                        elif join_tuple[2] == 2:
                            self.hashtp2[self.max_tnum] = {"{0}.o={1}.s".format(join_tuple[0], join_tuple[1])}
                        elif join_tuple[2] == 3:
                            self.hashtp2[self.max_tnum] = {"{0}.s={1}.o".format(join_tuple[0], join_tuple[1])}
                        elif join_tuple[2] == 4:
                            self.hashtp2[self.max_tnum] = {"{0}.s={1}.s".format(join_tuple[0], join_tuple[1])}
                        print("表t" + str(self.max_tnum) + "创建并加载完成")

    def match_one_result_to_hashtp2(self, result):
        # 用于从hashtp2字典中由值查键
        keys = list()
        values = list()
        for key, value in self.hashtp2.items():
            keys.append(key)
            values.append(value)

        all_t_conditions = result[-1]
        t_names = list()
        for single_t_conditions in all_t_conditions:  # traverse each table's conds of this sql
            print(single_t_conditions)
            print(single_t_conditions in values)
            if single_t_conditions in values:  # from hashtp2.values search t_name with this single_t_conds
                t_name = keys[values.index(single_t_conditions)]
                t_names.append(t_name)
            else:  # if one table is not found, then the result is not available
                return None
        return t_names

    def query(self):
        total_time = 0  # 总用时

        for sql in self.workloads:  # 遍历工作负载中的每一条sql语句
            for result in self.all_sql_pri_results[sql]:  # traverse each result of this sql
                print()
                print(result)
                t_names = self.match_one_result_to_hashtp2(result)  # get the mached table names
                print(t_names)
                if t_names is not None:
                    break
            if t_names is None:
                # print('Error mydb2.get_reward 453rows: can\'t match the sql ',sql )
                # continue
                # parse_where函数中，解析p谓词的部分逻辑
                conditions = sql.split('where')[1].strip()
                pcond = {}
                if (conditions.__contains__(" and ")):
                    for condition in conditions.split(" and "):
                        pmatch = re.match('(.)\\.p\s*=\s*"(.*)".*', condition.strip())
                        pmatch_ = re.match('p\s*=\s*"(.*)"', condition.strip())
                        if pmatch is not None:
                            pcond[pmatch.group(1)] = pmatch.group(2)
                        elif pmatch_ is not None:
                            pcond['t0'] = pmatch_.group(1)
                # 获取到pcond之后，结合hashtp，用来查找是否有满足条件的单表
                to_be_rewritten = {}
                for pcond_key, pcond_value in pcond.items():
                    for hasthtp_key, hashtp_value in self.hashtp.items():
                        if pcond_value in hashtp_value and len(hashtp_value) == 1:
                            to_be_rewritten[pcond_key] = "t" + str(hasthtp_key)
                # 改写
                if to_be_rewritten:
                    for to_be_rewritten_key, to_be_rewritten_value in to_be_rewritten.items():
                        # 改写from子句
                        if "t0 " + to_be_rewritten_key + "," in sql:
                            sql = sql.replace("t0 " + to_be_rewritten_key + ",", to_be_rewritten_value + ",")
                        elif "t0 " + to_be_rewritten_key in sql:
                            sql = sql.replace("t0 " + to_be_rewritten_key, to_be_rewritten_value)
                        # 改写select子句和where子句
                        sql = sql.replace(to_be_rewritten_key + ".", to_be_rewritten_value + ".")

            else:  # if matched successfully, change the original sql's table name (tp0,tp1,...) to existing t_names (t0,t1,...)
                sql = result[1]
                for i, tp_name in enumerate(result[0]):
                    sql = sql.replace(tp_name, "t" + str(t_names[i]))
            print(sql)
            # 执行
            time_start = time.time()
            try:
                self.cur.execute(sql)
                test_result = self.cur.fetchall()
                self.conn.commit()
            except:
                self.conn.rollback()
            time_end = time.time()
            total_time += time_end - time_start
        return total_time


if __name__ == '__main__':
    contrast = Contrast("../../data/workload_graph_test")
    contrast.load_workload("../../data/contrast/workload_stats")
    contrast.create_single_view()
    print("hashtp:")
    print(contrast.hashtp)

    contrast.create_two_merged_view()
    # contrast.hashtp2 = {1: set(), 2: set(), 3: set(), 4: {"type.s=comment.s"}, 5: {"comment.o=topic.s"}}
    print("hashtp2:")
    print(contrast.hashtp2)
    print(contrast.query())
