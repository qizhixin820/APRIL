import shelve
from datetime import datetime

import MySQLdb
import re
import time

import shelve

from constants import *
DATABASE = 'testlubm'


class Mydb:
    workload = []  # sql语句负载

    def __init__(self, mydb_dump_number, db):
        # 0. 连接database(mydb/watdivDB), 每轮不需重复开启关闭
        self.conn = MySQLdb.connect(host='127.0.0.1',
                                    user='root', passwd=MYSQL_PWD,
                                    port=3306, db=db, charset='utf8')
        self.cur = self.conn.cursor()

        # 1. 每轮需重新初始化的对象, 记录已有database的信息
        self.max_tnum = 0  # 当前数据库表的最大序号
        self.hashtp = {}  # 映射：表->Set(表所包含的谓词) 
        self.table_attributes = {}  # 存储每个表的属性名
        # 用于重写sql时替换表名, KEY is INTEGER of t_num, VALUE is SET of pname and conditions
        self.hashtp2 = {}  # like
        # 用于重写sql时替换属性, KEY is STR of t_name, VALUE is DICT whose key is p_name, value is p_order(a,b,c,d), 
        self.hashtp3 = {}  # like {'t5': {'type':'', 'comment':'b'} }

        # 2. 各episode持久存储 merge后结果为empty的action
        if mydb_dump_number is None:
            self.empty_action_list = list()
        else:
            with shelve.open(PATH + 'data/mydb2_dump/mydb2_dump_in_episode' + str(mydb_dump_number)) as f:
                self.empty_action_list = f['empty_action_list']

        # 3. 各episode持久存储, 下面三个成员由load_workload读取
        self.ps_in_workload = None
        self.all_sql_pri_results = dict()  # 优先级映射
        self.pri_attr_map = dict()  # 属性映射（优先级结果中读取），用于在改写sql时改写属性
        self.load_workload()

        # 4. 日志
        self.LOGGING = MydbLogger()  # mydb2的日志管理对象

    def save_mydb2(self, episode):
        mydb2_dump = {'max_tnum': self.max_tnum,
                      'hashtp': self.hashtp,
                      'table_attributes': self.table_attributes,
                      'hashtp2': self.hashtp2,
                      'hashtp3': self.hashtp3,
                      'empty_action_list': self.empty_action_list, }

        with shelve.open(PATH + 'data/mydb2_dump/mydb2_dump_in_episode_test' + str(episode)) as fw:
            for key in mydb2_dump:
                fw[key] = mydb2_dump[key]

    @staticmethod
    def load_mydb2(episode):
        mydb = Mydb(None, DATABASE)
        with shelve.open(PATH + 'data/mydb2_dump/mydb2_dump_in_episode_test' + str(episode)) as fw:
            mydb.max_tnum = fw['max_tnum']
            mydb.hashtp = fw['hashtp']
            mydb.hashtp2 = fw['hashtp2']
            mydb.hashtp3 = fw['hashtp3']
            mydb.table_attributes = fw['table_attributes']
            mydb.empty_action_list = fw['empty_action_list']
        return mydb

    @staticmethod
    def init_t0(filepath, db):
        conn = MySQLdb.connect(host='127.0.0.1',
                               user='root', passwd=MYSQL_PWD,
                               port=3306, db=db, charset='utf8')
        cur = conn.cursor()
        with open(filepath) as fp:
            text_lines = fp.readlines()
        cur.execute("drop table if exists t0 ")
        conn.commit()
        cur.execute(
            "create table if not exists t0 (p varchar(255) not null, s varchar(255) not null, o varchar(255) not null) engine=memory charset utf8")
        conn.commit()
        data = list()
        print(len(text_lines))
        for line in text_lines:
            match_line = re.match('<(.*)>\s*<(.*)>\s*(<.*>|".*")', line)
            if match_line:
                s = match_line.group(1)
                p = match_line.group(2)
                o = match_line.group(3)[1:-1]
                data.append((p, s, o))
            else:
                print(line)
        data = tuple(data)
        sql = "INSERT INTO t0(p, s, o) VALUES (%s, %s, %s)"
        patch = 10000
        for i in range(len(data) // patch + 1):
            print(i * patch, ":", (i + 1) * patch)
            cur.executemany(sql, data[i * patch:(i + 1) * patch])
        conn.commit()

    def init_watdiv_t0(self, filepath):
        with open(filepath) as fp:
            text_lines = fp.readlines()
        fp.close()
        try:
            self.cur.execute(
                "create table if not exists t0 (p varchar(255) not null, s varchar(255) not null, o varchar(255) not null) engine=memory charset utf8")
            self.conn.commit()
        except:
            self.conn.rollback()
        data = list()
        print(len(text_lines))

        for line in text_lines:
            match_line = re.match('<(.*)>\\s*<(.*)>\\s*(<.*>|".*")\\s*\\.', line)
            if match_line:
                s = match_line.group(1)
                p = match_line.group(2)
                o = match_line.group(3)[1:-1][:255]
                data.append((p, s, o))
            else:
                print(line)
        data = tuple(data)
        sql = "INSERT INTO t0(p, s, o) VALUES (%s, %s, %s)"
        patch = 10000
        for i in range(len(data) // patch + 1):
            print(i * patch, ":", (i + 1) * patch)
            self.cur.executemany(sql, data[i * patch:(i + 1) * patch])
        self.conn.commit()

    def get_t0_p_list(self):
        """Return a LIST of p names in t0.
        """

        sql = 'select distinct p from t0;'
        self.cur.execute(sql)
        ps = self.cur.fetchall()

        p_list = [p[0] for p in ps]
        return p_list

    def load_workload(self):
        """
        加载self.workload、self.ps_in_workload、self.all_sql_pri_results、self.pri_attr_map
        :return:
        """
        with shelve.open(PATH + "data/preprocess/priority.dat") as f1:
            for key in f1.keys():
                self.workload.append(key)
                self.all_sql_pri_results[key] = f1[key]

        with shelve.open(PATH + "data/preprocess/workload_stats") as f2:
            self.ps_in_workload = f2['p_conditions_set']

        with shelve.open(PATH + "data/preprocess/attribute_map.dat") as f3:
            for key in f3.keys():
                self.pri_attr_map[key] = f3[key]  # like {    sql: {'tp5': {'':'type', 'b':'comment'} },        }

    def divide(self, p):
        """
        根据谓词p从t0中提取记录，并插入到新表中。
        :param p: 谓词条件（str）
        :return:
        """
        results = self.__select(0, p)  # 提取记录
        self.LOGGING.write('mydb2 execute divide action p: {0}'.format(p), True)
        with open(PATH + "data/log/lubm.txt", "a+", encoding="utf8") as f:
            f.write("divide t0 by {0}\n".format(p))
        tnum = self.__create_table()  # 创建单表
        self.__insert_from_results(tnum, results)  # 插入数据

        self.hashtp.setdefault(self.max_tnum, {p})  # 更新hashtp

        self.hashtp2[tnum] = {p}  # 无连接条件，更新hashtp2

        # 更新hashtp3
        self.hashtp3['t' + str(tnum)] = dict()
        self.hashtp3['t' + str(tnum)][p] = ''

    def merge(self, tnum1, tnum2, choice):
        """Merge table tnum1 and tnum2 according to choice,
        Don't create table if result is empty,
        If new table created:
            update hashtp, hashtp2, hashtp3

        Args:
            actions of three number, 2 tables' id and 1 join condition's id
        """

        self.LOGGING.write('mydb2 execute merge action: {} {} {}'.format(tnum1, tnum2, choice), True)
        tchar_maps_for_merge_fetch = Mydb.init_tchar_maps_for_merge_fetch()

        # create query sql by action(tnum1, tnum2, choice)
        tname1 = "t" + str(tnum1)
        tname2 = "t" + str(tnum2)
        # 根据两表合并之后新表的总列数，组装查询sql
        length1 = len(self.table_attributes.get(tnum1))
        length2 = len(self.table_attributes.get(tnum2))
        length = length1 + length2
        sql = "select * from " + tname1 + ", " + tname2 + " where "
        if length == 6:
            sql = sql + tname1 + tchar_maps_for_merge_fetch.get(6)[choice][0] + " = " + tname2 + \
                  tchar_maps_for_merge_fetch.get(6)[choice][1] + ";"
        elif length == 9:
            if length1 == 6:
                sql = sql + tname1 + tchar_maps_for_merge_fetch.get(96)[choice][0] + " = " + tname2 + \
                      tchar_maps_for_merge_fetch.get(96)[choice][1] + ";"
            elif length1 == 3:
                sql = sql + tname1 + tchar_maps_for_merge_fetch.get(93)[choice][0] + " = " + tname2 + \
                      tchar_maps_for_merge_fetch.get(93)[choice][1] + ";"
        elif length == 12:
            if length1 == 3:
                sql = sql + tname1 + tchar_maps_for_merge_fetch.get(123)[choice][0] + " = " + tname2 + \
                      tchar_maps_for_merge_fetch.get(123)[choice][
                          1] + ";"
            elif length1 == 6:
                sql = sql + tname1 + tchar_maps_for_merge_fetch.get(126)[choice][0] + " = " + tname2 + \
                      tchar_maps_for_merge_fetch.get(126)[choice][
                          1] + ";"
            elif length1 == 9:
                sql = sql + tname1 + tchar_maps_for_merge_fetch.get(129)[choice][0] + " = " + tname2 + \
                      tchar_maps_for_merge_fetch.get(129)[choice][
                          1] + ";"
        with open(PATH + "data/log/lubm.txt", "a+", encoding="utf8") as f:
            f.write("merge {0} and {1} by {2}\n".format(tname1, tname2, sql.split("where")[1].strip()))

        # execute the sql and get the query results
        self.cur.execute(sql)
        results = self.cur.fetchall()

        # if query result is empty, store the action, return(don't create new table)
        if len(results) == 0:
            self.empty_action_list.append((tnum1, tnum2, choice))
            self.LOGGING.write('action not executed: empty result', True)
            return

        # actually do the action: create table, insert query results into it

        self.LOGGING.write('sql executed by merge: ' + sql)
        self.__create_dtable(tnum1, tnum2)
        self.__insert_from_dresults(self.max_tnum, results)

        # 更新hashtp
        temp = self.hashtp.get(tnum1)
        temp2 = self.hashtp.get(tnum2)
        self.hashtp.setdefault(self.max_tnum, temp.union(temp2))

        # update hashtp2
        # TODO p1, p2合法性检查
        if length1 == 3:
            self.cur.execute("select distinct p from " + tname1)
            p1 = self.cur.fetchall()
        elif length1 == 6:
            self.cur.execute("select distinct p,bp from " + tname1)
            p1 = self.cur.fetchall()
        elif length1 == 9:
            self.cur.execute("select distinct p,bp,cp from " + tname1)
            p1 = self.cur.fetchall()
        if length2 == 3:
            self.cur.execute("select distinct p from " + tname2)
            p2 = self.cur.fetchall()
        elif length2 == 6:
            self.cur.execute("select distinct p,bp from " + tname2)
            p2 = self.cur.fetchall()
        elif length2 == 9:
            self.cur.execute("select distinct p,bp,cp from " + tname2)
            p2 = self.cur.fetchall()
        self.hashtp2[self.max_tnum] = self.hashtp2[tnum1].copy()
        self.hashtp2[self.max_tnum].update(self.hashtp2[tnum2])
        self.hashtp2[self.max_tnum].update(self.get_added_hashtp2_item(p1, p2, length1, length2, choice))

        # TODO 检查条件连通性
        for condition_set in self.hashtp2[self.max_tnum]:
            Mydb.check_transitive_join(condition_set)

        # update hashtp3
        self.hashtp3['t' + str(self.max_tnum)] = dict()
        length = length1 + length2
        if length == 6:
            self.cur.execute("select distinct p,bp from " + 't' + str(self.max_tnum))
            p = self.cur.fetchall()[0]  # 查询结果是两层tuple，并且合并的表都是以谓词为单位，所以查询结果应该只有一条记录，对查询结果取第0个元素，直接降维
            self.hashtp3['t' + str(self.max_tnum)][p[0]] = ''
            self.hashtp3['t' + str(self.max_tnum)][p[1]] = 'b'
        elif length == 9:
            self.cur.execute("select distinct p,bp,cp from " + 't' + str(self.max_tnum))
            p = self.cur.fetchall()[0]
            self.hashtp3['t' + str(self.max_tnum)][p[0]] = ''
            self.hashtp3['t' + str(self.max_tnum)][p[1]] = 'b'
            self.hashtp3['t' + str(self.max_tnum)][p[2]] = 'c'
        elif length == 12:
            self.cur.execute("select distinct p,bp,cp,dp from " + 't' + str(self.max_tnum))
            p = self.cur.fetchall()[0]
            self.hashtp3['t' + str(self.max_tnum)][p[0]] = ''
            self.hashtp3['t' + str(self.max_tnum)][p[1]] = 'b'
            self.hashtp3['t' + str(self.max_tnum)][p[2]] = 'c'
            self.hashtp3['t' + str(self.max_tnum)][p[3]] = 'd'
        return

    @staticmethod
    def check_transitive_join(condition_set):
        """
        检查连接条件集合的连通性，如{a.s=b.s, b.s=c.s}，可以发现隐藏条件{a.s=c.s}，并将其加入到原集合中
        :param condition_set:   原条件集合
        :return:
        """
        # 过滤连接条件
        join_condition_set = set()
        for cond in condition_set:
            if "=" in cond:
                join_condition_set.add(cond)
        # 构造连通池
        connected_pool = []
        for cond in join_condition_set:
            left, right = cond.split("=")
            new_set_flag = True
            for c_set in connected_pool:
                if left in c_set or right in c_set:
                    c_set.add(left)
                    c_set.add(right)
                    new_set_flag = False
                    break
            if new_set_flag:
                connected_pool.append(set((left, right)))
        # 对连通池中的每一个连通集，重组连接条件，加入到原集合中
        for c_set in connected_pool:
            c_tuple = tuple(c_set)
            for i in range(len(c_tuple) - 1):
                for j in range(i + 1, len(c_tuple)):
                    condition_set.add(c_tuple[i] + "=" + c_tuple[j])
                    condition_set.add(c_tuple[j] + "=" + c_tuple[i])

    def match_one_result_to_hashtp2(self, all_t_conditions):
        """从hashtp2中，查询一条优先级项目是否存在于mydb中
        :param all_t_conditions: 优先级项目的连接条件(四元组的第四分量)
        :return: a list of table ids in mydb that match all_t_conditions
        :return: None if there is one table that is 't0' or not in mydb
        """
        # 用于从hashtp2字典中由值查键
        keys = list()
        values = list()
        for key, value in self.hashtp2.items():
            keys.append(key)
            values.append(value)

        t_nums = list()
        # pri result[-1] 和 result[0] 顺序相同
        for single_t_conditions in all_t_conditions:  # traverse each table's conds of this sql
            if single_t_conditions in values:  # from hashtp2.values search t_name with this single_t_conds
                t_num = keys[values.index(single_t_conditions)]
                t_nums.append(t_num)
            else:  # if one table is not found, then the result is not available
                return None
        return t_nums

    def match_result_with_t0_to_hashtp2(self, result):
        if 't0' not in result[0]:
            return None

        # 用于从hashtp2字典中由值查键
        keys = list()
        values = list()
        for key, value in self.hashtp2.items():
            keys.append(key)
            values.append(value)

        t_nums = list()
        # pri result[-1] 和 result[0] 顺序相同
        for single_t_conditions in result[-1]:  # traverse each table's conds of this sql
            if single_t_conditions == set():
                t_nums.append('OCCUPIED')
            elif single_t_conditions in values:  # from hashtp2.values search t_name with this single_t_conds
                t_num = keys[values.index(single_t_conditions)]
                t_nums.append(t_num)
            else:  # 存在result会产生这种情况, 出现时, 说明有一个表不存在于mydb, 即该result不可用
                return None
        return t_nums

    def get_total_time(self):
        """
        对负载中每一条sql，根据优先级，采取最优的改写策略，并查询之，返回总的查询时间。
        :return: total_time 即负载中所有查询的最优查询时间之和
        :note: total_time单位为s
        """
        total_time = 0  # 总用时
        sql_file = open(PATH + "data/lubm_sql_rewritten.txt", "w")
        used_tnum = set()
        for sql in self.workload:  # 遍历工作负载中的每一条sql语句
            matched_result = None
            for result in self.all_sql_pri_results[sql]:  # traverse each result of this sql
                t_nums = self.match_one_result_to_hashtp2(result[-1])  # get the matched table ids/numbers
                if t_nums is not None:  # 已匹配一条优先级项目，无需继续匹配
                    matched_result = result
                    break
            if t_nums is None:  # for all result, match_one_result_to_hashtp2 return None, that means 1. 't0' in result or 2. there is a table not in mydb
                for result in self.all_sql_pri_results[sql]:  # traverse each result of this sql
                    t_nums = self.match_result_with_t0_to_hashtp2(result)  # match result that uses 't0',
                    if t_nums:
                        matched_result = result
                        break
                sql = matched_result[1]
                '''
                for i, tp_name in enumerate(matched_result[0]):
                    if tp_name == "t0":
                        continue
                    # 1. replace the original sql's table name:
                    t_name_for_replace = 't' + str(t_nums[i])
                    sql = sql.replace(tp_name,
                                      t_name_for_replace)  # like select t5.s from t5 where t5.o="senior user" and t5.co="明星"
                '''
                for i, tp_name in enumerate(matched_result[0]):
                    if tp_name == 't0':
                        continue
                    # replace the original sql's table name: like 't1 a'(before where) and 'a'(after where)
                    t_name_for_replace = 't' + str(t_nums[i])
                    t_chr = 'a' + chr(97 + i)

                    sql_split_from_left, sql_split_from_right = sql.split('from')
                    #   change sql right of 'from'
                    sql_split_from_right = sql_split_from_right.split(tp_name)
                    sql_split_from_right = sql_split_from_right[0] \
                                           + t_name_for_replace + ' ' + t_chr \
                                           + t_chr.join(sql_split_from_right[1:])
                    #   change sql left of 'from'
                    sql_split_from_left = sql_split_from_left.replace(tp_name, t_chr)
                    #   construct new sql
                    sql = sql_split_from_left + 'from' + sql_split_from_right
            else:  # 存在优先级项目匹配成功
                # If table matched successfully:
                #   1. change the original sql's table name (tp0,tp1,...) to existing t_names (t0,t1,...)
                #   2. change the attribute to be in the correct order
                #   3. construct new sql

                # sql是优先级项目对应的sql，为了适应当前表序列的查询，需要修改其表明，还可能需要修改其表属性
                sql_in_workload = sql
                sql = matched_result[1]

                for i, tp_name in enumerate(matched_result[0]):
                    # 1. replace the original sql's table name and construct new sql: like 't1 a'(before where) and 'a'(after where)
                    t_name_for_replace = 't' + str(t_nums[i])
                    t_chr = 'a' + chr(97 + i)

                    sql_split_from_left, sql_split_from_right = sql.split('from')
                    # change sql right of 'from'
                    sql_split_from_right = sql_split_from_right.split(tp_name)
                    sql_split_from_right = sql_split_from_right[0] \
                                           + t_name_for_replace + ' ' + t_chr \
                                           + t_chr.join(sql_split_from_right[1:])
                    # change sql left of 'from'
                    sql_split_from_left = sql_split_from_left.replace(tp_name, t_chr)
                    # construct new sql, like select t5.s from t5 where t5.o="senior user" and t5.co="明星"
                    sql = sql_split_from_left + 'from' + sql_split_from_right

                    # print('sql:', sql)

                    # 2. replace attributes in sql:
                    # traverse all conds, judge which cond to replace attributes, do the replace, and reconstruct sql for next replace iteration
                    conds = [e.strip() for e in
                             sql.split('where')[1].split('and')]  # like ['t5.o = "senior user"' , 't5.co = "明星"']
                    new_conds = list()
                    for j, cond in enumerate(conds):
                        new_cond = cond
                        left, right = [e.strip() for e in cond.split('=')]

                        # cond左部的属性替换
                        t_chr_in_cond = left.split('.')[0]
                        attr = left.split('.')[1]
                        if attr in ['p', 's', 'o', 'bp', 'bs', 'bo', 'cp', 'cs', 'co', 'dp', 'ds',
                                    'do'] and t_chr_in_cond == t_chr:
                            # get the right p_order/attr[0]
                            p_name = self.pri_attr_map[sql_in_workload][tp_name][
                                '' if len(attr) == 1 else attr[
                                    0]]  # pri_attr_map[sql_in_workload]: {'tp5': {'':'type', 'b':'comment'} }
                            p_order = self.hashtp3[t_name_for_replace][
                                p_name]  # mydb2 hashtp3: {'t5': {'type':'', 'comment':'b'} }

                            # construct new_cond
                            attr = p_order + attr[-1]
                            left = t_chr + '.' + attr

                            new_cond = left + '=' + right
                            # print('attr,new_cond:',attr,new_cond)

                        # cond右部的属性替换, 需判断右部是字符串value还是类似a.s的
                        t_chr_in_cond = right.split('.')[0]
                        attr = right.split('.')[1]
                        if attr in ['p', 's', 'o', 'bp', 'bs', 'bo', 'cp', 'cs', 'co', 'dp', 'ds',
                                    'do'] and t_chr_in_cond == t_chr:
                            # get the right p_order/attr[0]
                            p_name = self.pri_attr_map[sql_in_workload][tp_name][
                                '' if len(attr) == 1 else attr[
                                    0]]  # pri_attr_map[sql_in_workload]: {'tp5': {'':'type', 'b':'comment'} }
                            p_order = self.hashtp3[t_name_for_replace][
                                p_name]  # mydb2 hashtp3: {'t5': {'type':'', 'comment':'b'} }

                            # construct new_cond
                            attr = p_order + attr[-1]
                            right = t_chr + '.' + attr

                            new_cond = left + '=' + right
                            # print('attr,new_cond:',attr,new_cond)

                        new_conds.append(new_cond)
                    # 3. construct new sql for next replace, until all tp_name replaced
                    sql = sql.split('where')[0] + ' where ' + ' and '.join(new_conds)
                    # print('sql3--------:', sql)

            # 将改写后的sql查询用到的表写入日志中，以作调试记录
            from_sql = re.search("from (.*)where", sql).group(1).strip()
            ts = from_sql.split(",")
            temp_used_tnum = set()
            for each_t in ts:
                each_t = each_t.strip()
                tnum_match = re.search("t(\\d*)", each_t)
                if tnum_match:
                    temp_used_tnum.add(tnum_match.group(1))
            used_tnum = used_tnum.union(temp_used_tnum)


            # 执行，为了准确计算时间，以下代码块不要执行写入日志

            time_start = time.time()
            # try:
            sql_file.write(sql + "\n")
            self.cur.execute(sql)
            test_result = self.cur.fetchall()
            self.conn.commit()

            # except:
            # self.conn.rollback()
            # self.LOGGING.write('error 311', True)
            time_end = time.time()
            total_time += time_end - time_start

            # self.LOGGING.write("query result:")
            # self.LOGGING.write(str(test_result))  # TODO 真正运行时该条日志不必开启，写文件比较耗时
            self.LOGGING.write('sql executed by get_total_time: ' + sql)
        with open(PATH + "data/log/used_tables.txt", "w") as f:
            for tnum in used_tnum:
                f.write(str(tnum) + "\n")
        sql_file.close()
        self.LOGGING.write("total_time: " + str(total_time))
        return total_time

    def judge_legal(self, max_tnum, pre_list):
        """Return a List of redundant actions' index.

        Args:
            max_tnum: INT, max number of tables in mydb, restricted by dqn main method
            pre_list: LIST of p_names in t0

        All situations considered:
            divide 2 : table exist; workload not used
             merge 6 :
        """
        ret_list = list()

        # 先检查分表操作会不会生成重复表
        p_num = len(pre_list)
        for i in range(p_num):
            p = pre_list[i]
            for k, v in self.hashtp.items():
                if k == 0:  # 初始表包含所有p, 不用和初始表对比
                    continue
                if len(v) == 1 and p in v:  # 如果有一个表只含p一种谓词, 则不用再增加一个只有p的表, 否则会重复
                    ret_list.append(i)
        # 负载中未用到的p也是冗余动作
        for i, p in enumerate(pre_list):
            if (p not in self.ps_in_workload) and (i not in ret_list):
                ret_list.append(i)

        # 再检查合表操作会不会 不存在 或者 生成重复表
        index = p_num
        for tnum1 in range(2, max_tnum + 1):
            for tnum2 in range(1, tnum1):

                # 冗余（redundant）操作检查

                # 第1种冗余操作: 硬性限制(无关choice): 表必须存在;
                #          方法: 以self.max_tnum为界限
                first_flag = False
                if tnum1 > self.max_tnum or tnum2 > self.max_tnum:
                    first_flag = True

                # 第2种冗余操作: 硬性限制(无关choice): 合并结果表的p不超过4个, 故组合共有6种: 1,2,3 —— 1 ; 1,2 —— 2 ; 1 —— 3;
                #                方法: 用2个表中p的个数length1, length2判断
                second_flag = False
                if not first_flag:
                    length1 = len(self.table_attributes.get(tnum1))
                    length2 = len(self.table_attributes.get(tnum2))
                    if length2 == 3:
                        if length1 > 9:
                            second_flag = True
                    elif length2 == 6:
                        if length1 > 6:
                            second_flag = True
                    elif length2 == 9:
                        if length1 > 3:
                            second_flag = True
                    else:
                        second_flag = True

                # 第3种冗余操作: 硬性限制(无关choice): 如果两个表有相同的某个p, 则不可再进行连接
                #           方法: 用self.hashtp判断两集合是否有交集
                third_flag = False
                if not first_flag and not second_flag:
                    for p_name in self.hashtp[tnum1]:
                        if p_name in self.hashtp[tnum2]:
                            third_flag = True

                # 第4种冗余操作: choice的个数(有关length1,length2)
                #                方法: 设置一个最大choice值
                # 第5种冗余操作: (有关choice)合并得到的表已经存在
                #                方法: 先查询两个表中的p, 再遍历choice, 用hashtp2中的连接条件判断

                if not first_flag and not second_flag and not third_flag:
                    # 第4种
                    length1 = len(self.table_attributes.get(tnum1))
                    length2 = len(self.table_attributes.get(tnum2))
                    max_choice = self.__get_max_choice(length1, length2)
                    # 第5种
                    tname1 = 't' + str(tnum1)
                    tname2 = 't' + str(tnum2)
                    if length1 == 3:
                        self.cur.execute("select distinct p from " + tname1)
                        p1 = self.cur.fetchall()
                    elif length1 == 6:
                        self.cur.execute("select distinct p,bp from " + tname1)
                        p1 = self.cur.fetchall()
                    elif length1 == 9:
                        self.cur.execute("select distinct p,bp,cp from " + tname1)
                        p1 = self.cur.fetchall()
                    if length2 == 3:
                        self.cur.execute("select distinct p from " + tname2)
                        p2 = self.cur.fetchall()
                    elif length2 == 6:
                        self.cur.execute("select distinct p,bp from " + tname2)
                        p2 = self.cur.fetchall()
                    elif length2 == 9:
                        self.cur.execute("select distinct p,bp,cp from " + tname2)
                        p2 = self.cur.fetchall()

                # 遍历choice:
                # 123. 将前3种(choice无关的)冗余操作加入到ret_list
                # 4. 第4种:merge后结果为empty的操作为冗余操作
                # 5. 判断第5种冗余操作(连接条件重复), 并加入到ret_list
                # 6. empty result 限制
                for choice in range(0, 16):  # choice的取值虽然与length1和length2有关, 但dqn里设置为(最多)16个choice
                    if first_flag or second_flag or third_flag:  # 第1,2,3种限制
                        ret_list.append(index)
                    elif choice >= max_choice:  # 第4种限制
                        ret_list.append(index)
                    elif (tnum1, tnum2, choice) in self.empty_action_list:  # 6.空结果限制
                        ret_list.append(index)
                    else:  # 第5种限制, 对每个choice, 使用hashtp2的连接条件, 判断是否产生已有表
                        # 先计算 合并结果表的 hashtp2, 即单表连接条件
                        tmp_conds = self.hashtp2[tnum1].copy()
                        tmp_conds.update(self.hashtp2[tnum2].copy())
                        tmp_conds.update(self.get_added_hashtp2_item(p1, p2, length1, length2, choice))
                        if tmp_conds in self.hashtp2.values():
                            ret_list.append(index)
                    index += 1
        return ret_list

    def tearDown(self):
        """delete tables except t0
        :return:
        """
        tnum = self.max_tnum
        # 删除除了t0以外的其他表序列
        for i in range(1, tnum + 1):
            tname = "t" + str(i)
            try:
                sql = "drop table if exists " + tname
                self.cur.execute(sql)
                self.conn.commit()
            except:
                self.conn.rollback()

        # 重新初始化self的参数
        self.max_tnum = 0
        self.hashtp = {}
        self.table_attributes = {}
        self.hashtp2 = {}
        self.hashtp3 = {}
        self.empty_action_list = list()

    def plain_search(self):
        """
        按照原表t0查询工作负载总用时
        :return: 在t0上查询的工作负载总用时
        """
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
            self.LOGGING.write('plain_search time, sql: ' + str(time_end - time_start) + ', ' + sql)
        return total_time

    def __create_table(self):
        """
        新建一个表（divide中调用）
        :return: 新表序号
        """
        self.max_tnum += 1  # 每次建表最大序号+1
        tname = 't' + str(self.max_tnum)  # 当前表名
        sql = 'CREATE TABLE IF NOT EXISTS ' + tname + \
              ' (p VARCHAR(255) NOT NULL,' \
              's VARCHAR(255) NOT NULL,' \
              'o VARCHAR(255) NOT NULL' \
              ') engine=memory charset utf8'
        self.cur.execute(sql)
        self.conn.commit()
        attribute = ["p", "s", "o"]
        self.table_attributes.setdefault(self.max_tnum, attribute)  # 记录新生成表的属性名
        return self.max_tnum

    def __create_dtable(self, tnum1, tnum2):
        """
        新建一个表（merge中调用）
        :param tnum1: merge中的参数表tnum1
        :param tnum2: merge中的参数表tnum2
        :return:
        """
        self.max_tnum += 1  # 表序号+1
        tname = 't' + str(self.max_tnum)  # 新表表名
        length = len(self.table_attributes.get(tnum1)) + len(self.table_attributes.get(tnum2))  # 新表应该有的总列数

        sql = "CREATE TABLE IF NOT EXISTS " + tname + \
              " (p VARCHAR(255) NOT NULL, s VARCHAR(255) NOT NULL, o VARCHAR(255) NOT NULL, " \
              "bp VARCHAR(255) NOT NULL, bs VARCHAR(255) NOT NULL, bo VARCHAR(255) NOT NULL"
        end = ") engine=memory charset utf8"  # sql尾部
        sql1 = ",cp VARCHAR(255) NOT NULL, cs VARCHAR(255) NOT NULL, co VARCHAR(255) NOT NULL"
        sql2 = ",dp VARCHAR(255) NOT NULL, ds VARCHAR(255) NOT NULL, do VARCHAR(255) NOT NULL"
        attribute = ["p", "s", "o", "bp", "bs", "bo"]
        # 根据合并之后的新表应该有的列数，对建表sql进行组装
        if length == 6:
            sql = sql + end
        elif length == 9:
            sql = sql + sql1 + end
            attribute.extend(["cp", "cs", "co"])
        elif length == 12:
            sql = sql + sql1 + sql2 + end
            attribute.extend(["cp", "cs", "co", "dp", "ds", "do"])
        self.cur.execute(sql)
        self.conn.commit()
        self.table_attributes.setdefault(self.max_tnum, attribute)  # 记录新合成表的属性名

    @staticmethod
    def init_tchar_maps_for_merge_fetch():
        maps = {}
        list1 = [[".o", ".o"], [".o", ".s"], [".s", ".o"], [".s", ".s"]]
        maps.setdefault(6, list1)
        list2 = [[".o", ".o"], [".o", ".s"], [".s", ".o"], [".s", ".s"], [".bo", ".o"], [".bo", ".s"], [".bs", ".o"],
                 [".bs", ".s"]]
        maps.setdefault(96, list2)
        list3 = [[".o", ".o"], [".o", ".s"], [".o", ".bo"], [".o", ".bs"], [".s", ".o"], [".s", ".s"], [".s", ".bo"],
                 [".s", ".bs"]]
        maps.setdefault(93, list3)
        list4 = [[".o", ".o"], [".o", ".s"], [".o", ".bo"], [".o", ".bs"], [".o", ".co"], [".o", ".cs"], [".s", ".o"],
                 [".s", ".s"], [".s", ".bo"], [".s", ".bs"], [".s", ".co"], [".s", ".cs"]]
        maps.setdefault(123, list4)
        list5 = [[".o", ".o"], [".o", ".s"], [".o", ".bo"], [".o", ".bs"], [".s", ".o"], [".s", ".s"], [".s", ".bo"],
                 [".s", ".bs"],
                 [".bo", ".o"], [".bo", ".s"], [".bo", ".bo"], [".bo", ".bs"], [".bs", ".o"], [".bs", ".s"],
                 [".bs", ".bo"], [".bs", ".bs"]]
        maps.setdefault(126, list5)
        list6 = [[".o", ".o"], [".o", ".s"], [".bo", ".o"], [".bo", ".s"], [".co", ".o"], [".co", ".s"], [".s", ".o"],
                 [".s", ".s"], [".bs", ".o"], [".bs", ".s"], [".cs", ".o"], [".cs", ".s"]]
        maps.setdefault(129, list6)
        return maps

    # 根据select查询结果，插入表tnum中
    def __insert_from_results(self, tnum, results):
        """
        将查询结果results插入到表tnum中（向被divide的表中插入，也即表tnum中只有三列）
        :param tnum:    插入表tnum
        :param results: 要插入的数据
        :return:
        """
        tname = "t" + str(tnum)
        try:
            self.cur.executemany("INSERT INTO " + tname + "(p,s,o) VALUES (%s,%s,%s)", results)
            self.conn.commit()
        except:
            self.conn.rollback()

    def __insert_from_dresults(self, tnum, results):
        """
        将查询结果results插入到表tnum中（向被merge的表中插入，也即表tnum中至少有6列）
        :param tnum:    插入表tnum
        :param results: 要插入的数据
        :return:
        """
        tname = "t" + str(tnum)
        try:
            length = len(self.table_attributes.get(tnum))
            if length == 6:
                self.cur.executemany(
                    "INSERT INTO " + tname + "(p,s,o,bp,bs,bo) VALUES (%s,%s,%s,%s,%s,%s)",
                    results)
                self.conn.commit()
            elif length == 9:
                self.cur.executemany(
                    "INSERT INTO " + tname + "(p,s,o,bp,bs,bo,cp,cs,co) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    results)
                self.conn.commit()
            elif length == 12:
                self.cur.executemany(
                    "INSERT INTO " + tname + "(p,s,o,bp,bs,bo,cp,cs,co,dp,ds,do) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    results)
                self.conn.commit()
        except:
            self.conn.rollback()

    # #  是否存在对应的表
    # #  hashtp_sql_pri: 存储表序列，sql语句和优先级信息
    # # key：表序列
    # # hashtp_pri: 表和其对应的谓词信息
    # def __is_exists(self, hashtp_sql_pri, key, hashtp_pri):
    #     sql = hashtp_sql_pri.get(key)[0]
    #     tablenames = key.split(",")
    #     table_map = []
    #     for t in tablenames:  # 对每一个表，找寻表序列中是否存在
    #         flag = 0
    #         temp = ""
    #         for t1 in self.hashtp:
    #             if self.__is_same(hashtp_pri.get(t), self.hashtp.get(t1)):
    #                 flag = 1
    #                 temp = t1
    #                 break
    #         if flag == 1:  # 说明当前表找到了，则继续寻找下一个表
    #             table_map0 = []
    #             table_map0.add(t)
    #             table_map0.add(temp)
    #             table_map.add(table_map0)  # 将替换的表信息存储到list中，用于改写sql语句
    #         else:  # 说明表序列中不存在当前优先级对应的表
    #             return False
    #             # 将原sql语句改写为新的sql语句并返回
    #             ####################################
    #             # 待补充
    #             # 存在的问题是不确定替换字符串是否正确
    #             # 用replace函数替换

    #         return True

    def __select(self, tnum, p=None):
        """
        按照谓词p查询表tnum，若p为None，则无条件查询
        :param tnum: 查询表tnum
        :param p: 查询谓词条件
        :return: 查询得到的结果记录
        """
        tname = "t" + str(tnum)
        if p == None:
            sql = "SELECT * FROM " + tname
        else:
            sql = "SELECT * FROM " + tname + " WHERE p = %s"
        self.cur.execute(sql, (p,))
        results = self.cur.fetchall()
        return results

    # def __assemble_priority_from_str(self, priority):
    #     assemble = []
    #     return assemble

    # Note: length is length1 + length2
    def get_added_hashtp2_item(self, p1, p2, length1, length2, choice):
        """构造两个表的连接条件
        
        :param p1: 第一个表的 p names
        :param p2: 第二个表的 p names
        :param length1: 
        :param length2:
        :param choice:
        :return:
        """
        cond = ''
        length = length1 + length2
        if length == 6:
            if choice == 0:
                cond = p1[0][0] + '.o=' + p2[0][0] + '.o'
            elif choice == 1:
                cond = p1[0][0] + '.o=' + p2[0][0] + '.s'
            elif choice == 2:
                cond = p1[0][0] + '.s=' + p2[0][0] + '.o'
            elif choice == 3:
                cond = p1[0][0] + '.s=' + p2[0][0] + '.s'
        elif length == 9:
            if length1 == 6:
                if choice == 0:
                    cond = p1[0][0] + '.o=' + p2[0][0] + '.o'
                elif choice == 1:
                    cond = p1[0][0] + '.o=' + p2[0][0] + '.s'
                elif choice == 2:
                    cond = p1[0][0] + '.s=' + p2[0][0] + '.o'
                elif choice == 3:
                    cond = p1[0][0] + '.s=' + p2[0][0] + '.s'
                elif choice == 4:
                    cond = p1[0][1] + '.o=' + p2[0][0] + '.o'
                elif choice == 5:
                    cond = p1[0][1] + '.o=' + p2[0][0] + '.s'
                elif choice == 6:
                    cond = p1[0][1] + '.s=' + p2[0][0] + '.o'
                elif choice == 7:
                    cond = p1[0][1] + '.s=' + p2[0][0] + '.s'
            elif length1 == 3:
                if choice == 0:
                    cond = p1[0][0] + '.o=' + p2[0][0] + '.o'
                elif choice == 1:
                    cond = p1[0][0] + '.o=' + p2[0][0] + '.s'
                elif choice == 2:
                    cond = p1[0][0] + '.o=' + p2[0][1] + '.o'
                elif choice == 3:
                    cond = p1[0][0] + '.o=' + p2[0][1] + '.s'
                elif choice == 4:
                    cond = p1[0][0] + '.s=' + p2[0][0] + '.o'
                elif choice == 5:
                    cond = p1[0][0] + '.s=' + p2[0][0] + '.s'
                elif choice == 6:
                    cond = p1[0][0] + '.s=' + p2[0][1] + '.o'
                elif choice == 7:
                    cond = p1[0][0] + '.s=' + p2[0][1] + '.s'
        elif length == 12:
            if length1 == 3:
                if choice == 0:
                    cond = p1[0][0] + '.o=' + p2[0][0] + '.o'
                elif choice == 1:
                    cond = p1[0][0] + '.o=' + p2[0][0] + '.s'
                elif choice == 2:
                    cond = p1[0][0] + '.o=' + p2[0][1] + '.o'
                elif choice == 3:
                    cond = p1[0][0] + '.o=' + p2[0][1] + '.s'
                elif choice == 4:
                    cond = p1[0][0] + '.o=' + p2[0][2] + '.o'
                elif choice == 5:
                    cond = p1[0][0] + '.o=' + p2[0][2] + '.s'
                elif choice == 6:
                    cond = p1[0][0] + '.s=' + p2[0][0] + '.o'
                elif choice == 7:
                    cond = p1[0][0] + '.s=' + p2[0][0] + '.s'
                elif choice == 8:
                    cond = p1[0][0] + '.s=' + p2[0][1] + '.o'
                elif choice == 9:
                    cond = p1[0][0] + '.s=' + p2[0][1] + '.s'
                elif choice == 10:
                    cond = p1[0][0] + '.s=' + p2[0][2] + '.o'
                elif choice == 11:
                    cond = p1[0][0] + '.s=' + p2[0][2] + '.s'
            elif length1 == 6:
                if choice == 0:
                    cond = p1[0][0] + '.o=' + p2[0][0] + '.o'
                elif choice == 1:
                    cond = p1[0][0] + '.o=' + p2[0][0] + '.s'
                elif choice == 2:
                    cond = p1[0][0] + '.o=' + p2[0][1] + '.o'
                elif choice == 3:
                    cond = p1[0][0] + '.o=' + p2[0][1] + '.s'
                elif choice == 4:
                    cond = p1[0][0] + '.s=' + p2[0][0] + '.o'
                elif choice == 5:
                    cond = p1[0][0] + '.s=' + p2[0][0] + '.s'
                elif choice == 6:
                    cond = p1[0][0] + '.s=' + p2[0][1] + '.o'
                elif choice == 7:
                    cond = p1[0][0] + '.s=' + p2[0][1] + '.s'
                elif choice == 8:
                    cond = p1[0][1] + '.o=' + p2[0][0] + '.o'
                elif choice == 9:
                    cond = p1[0][1] + '.o=' + p2[0][0] + '.s'
                elif choice == 10:
                    cond = p1[0][1] + '.o=' + p2[0][1] + '.o'
                elif choice == 11:
                    cond = p1[0][1] + '.o=' + p2[0][1] + '.s'
                elif choice == 12:
                    cond = p1[0][1] + '.s=' + p2[0][0] + '.o'
                elif choice == 13:
                    cond = p1[0][1] + '.s=' + p2[0][0] + '.s'
                elif choice == 14:
                    cond = p1[0][1] + '.s=' + p2[0][1] + '.o'
                elif choice == 15:
                    cond = p1[0][1] + '.s=' + p2[0][1] + '.s'
            elif length1 == 9:
                if choice == 0:
                    cond = p1[0][0] + '.o=' + p2[0][0] + '.o'
                elif choice == 1:
                    cond = p1[0][0] + '.o=' + p2[0][0] + '.s'
                elif choice == 2:
                    cond = p1[0][1] + '.o=' + p2[0][0] + '.o'
                elif choice == 3:
                    cond = p1[0][1] + '.o=' + p2[0][0] + '.s'
                elif choice == 4:
                    cond = p1[0][2] + '.o=' + p2[0][0] + '.o'
                elif choice == 5:
                    cond = p1[0][2] + '.o=' + p2[0][0] + '.s'
                elif choice == 6:
                    cond = p1[0][0] + '.s=' + p2[0][0] + '.o'
                elif choice == 7:
                    cond = p1[0][0] + '.s=' + p2[0][0] + '.s'
                elif choice == 8:
                    cond = p1[0][1] + '.s=' + p2[0][0] + '.o'
                elif choice == 9:
                    cond = p1[0][1] + '.s=' + p2[0][0] + '.s'
                elif choice == 10:
                    cond = p1[0][2] + '.s=' + p2[0][0] + '.o'
                elif choice == 11:
                    cond = p1[0][2] + '.s=' + p2[0][0] + '.s'

        left, right = cond.split('=')
        reverse_cond = right + '=' + left
        return [cond, reverse_cond]

    def __get_max_choice(self, length1, length2):
        """根据两个表p的个数判断有多少个choice
        :param length1: 第一个表的属性个数
        :param length2: 第二个表的属性个数
        :return: total numbers of join connection choice
        """
        max_choice = None
        length = length1 + length2
        if length == 6:  # 1 1
            max_choice = 4
        elif length == 9:
            if length1 == 6:  # 2 1
                max_choice = 8
            elif length1 == 3:  # 1 2
                max_choice = 8
        elif length == 12:
            if length1 == 3:  # 1 3
                max_choice = 12
            elif length1 == 6:  # 2 2
                max_choice = 16
            elif length1 == 9:  # 3 1
                max_choice = 12
        return max_choice


class MydbLogger:
    """
    mydb日志管理
    """

    def __init__(self, path=PATH + 'data/mydb-log.txt'):
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
    mydb = Mydb(None, DATABASE)
    mydb.init_t0(PATH + "data/University1_2.nt")
    # mydb.init_watdiv_t0('../data/watdiv_data.txt')
