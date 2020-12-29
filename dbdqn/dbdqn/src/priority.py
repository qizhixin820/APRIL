import pandas as pd
import MySQLdb
import re
import time

from mydb2 import Mydb
from parse_join import JoinParser
import shelve


class Priority:
    def __init__(self):
        self.conn = MySQLdb.connect(host='127.0.0.1', user='root', passwd='1108xmjr', port=3306, db='blog_graph',
                                    charset='utf8')  # 连接数据库
        self.cur = self.conn.cursor()
        self.table_attributes = {}  # 存储每个表的属性名
        self.max_tnum_pri = 0  # 用于计算优先级时创建表名称
        self.tname_weight = {}  # 存储每个表和它的权重
        self.tname_condition = {}  # 存储每个表和其涉及的连接条件 like {'tp0': set(), 'tp1': set(), 'tp2': set(), 'tp3': {'a.s=b.s'}}
        self.tname_p = {}  # 存储表名和其对应的谓词信息 like {'tp0': '用户类型', 'tp1': '发布', 'tp2': '话题'}
        self.tname_map = {}  # 存储原表到谓词的映射
        self.tname_attributes_map = {}  # 存储表中每个属性对应原属性信息
        # like {'tp0': {'a.p': 'tp0.p', 'a.s': 'tp0.s', 'a.o': 'tp0.o'}, 'tp1': {'b.p': 'tp1.p', 'b.s': 'tp1.s', 'b.o': 'tp1.o'}, 'tp2': {'c.p': 'tp2.p', 'c.s': 'tp2.s', 'c.o': 'tp2.o'}, 'tp3': {'a.p': 'tp3.p', 'a.s': 'tp3.s', 'a.o': 'tp3.o', 'b.p': 'tp3.bp', 'b.s': 'tp3.bs', 'b.o': 'tp3.bo'}}
        self.table_number = 0  # 记录原sql中涉及的表的个数
        self.pri_success_tables = []  # 存储每一种可能的组合以及其对应的sql语句
        self.weight_sum = 0  # 记录组合的权重和

    def re_init(self):
        '''
        self.table_attributes = {}  # 存储每个表的属性名
        self.max_tnum_pri = 0  # 用于计算优先级时创建表名称
        self.tname_weight = {}  # 存储每个表和它的权重
        self.tname_condition = {}  # 存储每个表和其涉及的连接条件 like {'tp0': set(), 'tp1': set(), 'tp2': set(), 'tp3': {'a.s=b.s'}}
        self.tname_p = {}  # 存储表名和其对应的谓词信息 like {'tp0': '用户类型', 'tp1': '发布', 'tp2': '话题'}
        self.tname_map = {}  # 存储原表到谓词的映射
        self.tname_attributes_map = {}  # 存储表中每个属性对应原属性信息
        # like {'tp0': {'a.p': 'tp0.p', 'a.s': 'tp0.s', 'a.o': 'tp0.o'}, 'tp1': {'b.p': 'tp1.p', 'b.s': 'tp1.s', 'b.o': 'tp1.o'}, 'tp2': {'c.p': 'tp2.p', 'c.s': 'tp2.s', 'c.o': 'tp2.o'}, 'tp3': {'a.p': 'tp3.p', 'a.s': 'tp3.s', 'a.o': 'tp3.o', 'b.p': 'tp3.bp', 'b.s': 'tp3.bs', 'b.o': 'tp3.bo'}}
        self.table_number = 0  # 记录原sql中涉及的表的个数
        self.pri_success_tables = []  # 存储每一种可能的组合以及其对应的sql语句
        self.weight_sum = 0  # 记录组合的权重和
        '''
        sql = "SELECT COUNT(*) TABLES, table_schema FROM information_schema.TABLES WHERE table_schema = 'blog_graph' GROUP BY table_schema;"
        self.cur.execute(sql)
        tp_table_num = self.cur.fetchall()[0][0] - 1
        for i in range(tp_table_num):
            self.cur.execute('drop table tp' + str(i))
            self.conn.commit()


    def __divide_pri(self, p, tname, tname0, weight):
        """计算优先级时使用： 从t0中分割单表出来,将表模式存储到成员变量table_attributes中
        p:谓词
        tname: 表名，用于获取优先级的表名以tp开头
        tname0: p名
        """
        results = self.__select(0, p)
        print('-----', p)
        attributes = {}
        sql = 'CREATE TABLE IF NOT EXISTS ' + tname + \
              ' (p VARCHAR(255) NOT NULL,' \
              's VARCHAR(255) NOT NULL,' \
              'o VARCHAR(255) NOT NULL' \
              ') charset utf8'
        self.cur.execute(sql)
        t1 = tname + ".p"
        t2 = tname + ".s"
        t3 = tname + ".o"
        t01 = tname0 + ".p"
        t02 = tname0 + ".s"
        t03 = tname0 + ".o"

        attributes.setdefault(t01, t1)
        attributes.setdefault(t02, t2)
        attributes.setdefault(t03, t3)

        attribute = ["p", "s", "o"]
        self.table_attributes.setdefault(tname, attribute)  # 记录新生成表的属性名
        try:
            self.cur.executemany("INSERT INTO " + tname + "(p,s,o) VALUES (%s,%s,%s)", results)
            self.conn.commit()
        except:
            # print('insert fail: ' + tname)
            self.conn.rollback()

        # 存储表对应的谓词信息,以及表对应的连接条件
        self.tname_p.setdefault(tname, p)
        con = set()
        self.tname_condition.setdefault(tname, con)  # 单表涉及的连接条件为空
        self.tname_attributes_map.setdefault(tname, attributes)
        self.tname_weight.setdefault(tname, weight)

    def __select(self, tnum, p=None):
        tname = "t" + str(tnum)
        if p == None:
            sql = "SELECT * FROM " + tname
        else:
            sql = "SELECT * FROM " + tname + " WHERE p = %s"
        self.cur.execute(sql, (p,))
        results = self.cur.fetchall()
        return results

    # 辅助函数，提取原表中的表名
    def __get_tablename(self, maps):
        count = 0
        tnames = []
        for key in maps:
            if count % 3 == 0:
                temp = key.split(".")[0]
                tnames.append(temp)
            count = count + 1
        return tnames

    # 合并2个表，得到新表
    def __merge_pri(self, tname1, tname2, tname, condition):
        print("合并" + tname1 + "  " + tname2 + "得到  " + tname)
        length = len(self.table_attributes.get(tname1)) + len(self.table_attributes.get(tname2))
        sql = "CREATE TABLE IF NOT EXISTS " + tname + \
              " (p VARCHAR(255) NOT NULL, s VARCHAR(255) NOT NULL, o VARCHAR(255) NOT NULL, " \
              "bp VARCHAR(255) NOT NULL, bs VARCHAR(255) NOT NULL, bo VARCHAR(255) NOT NULL"
        end = ") charset utf8"
        sql1 = ",cp VARCHAR(255) NOT NULL, cs VARCHAR(255) NOT NULL, co VARCHAR(255) NOT NULL"
        sql2 = ",dp VARCHAR(255) NOT NULL, ds VARCHAR(255) NOT NULL, do VARCHAR(255) NOT NULL"
        attribute = ["p", "s", "o", "bp", "bs", "bo"]
        attributes = {}  # 存储原连接条件到现连接条件的映射
        length1 = len(self.table_attributes.get(tname1))
        length2 = len(self.table_attributes.get(tname2))
        # 提取原来的2个表的名字
        t_1 = self.__get_tablename(self.tname_attributes_map.get(tname1))
        t_2 = self.__get_tablename(self.tname_attributes_map.get(tname2))
        if length == 6:
            sql = sql + end
        elif length == 9:
            sql = sql + sql1 + end
            attribute.extend(["cp", "cs", "co"])
        elif length == 12:
            sql = sql + sql1 + sql2 + end
            attribute.extend(["cp", "cs", "co", "dp", "ds", "do"])
        sql = sql + ";"

        self.cur.execute(sql)
        self.table_attributes.setdefault(tname, attribute)  # 记录新合成表的属性名
        index = 0
        for l in t_1:
            t01 = l + ".p"
            t02 = l + ".s"
            t03 = l + ".o"
            t1 = tname + "." + attribute[index]
            t2 = tname + "." + attribute[index + 1]
            t3 = tname + "." + attribute[index + 2]
            index = index + 3
            attributes.setdefault(t01, t1)
            attributes.setdefault(t02, t2)
            attributes.setdefault(t03, t3)
        for l in t_2:
            t01 = l + ".p"
            t02 = l + ".s"
            t03 = l + ".o"
            t1 = tname + "." + attribute[index]
            t2 = tname + "." + attribute[index + 1]
            t3 = tname + "." + attribute[index + 2]
            index = index + 3
            attributes.setdefault(t01, t1)
            attributes.setdefault(t02, t2)
            attributes.setdefault(t03, t3)

        con = condition.split("=")
        index1 = con[0]
        index2 = con[1]
        s1 = self.tname_attributes_map.get(tname1).get(index1)
        s2 = self.tname_attributes_map.get(tname2).get(index2)
        sql = "select * from " + tname1 + ", " + tname2 + " where " + str(s1) + "=" + str(s2)
        print(sql)
        print(tname)
        self.cur.execute(sql)
        results = self.cur.fetchall()

        try:
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
            # print('insert fail: ' + tname)
            self.conn.rollback()

        con = self.tname_condition.get(tname1).union(self.tname_condition.get(tname2))
        con.add(condition)
        self.tname_condition.setdefault(tname, con)  # 合并表涉及的连接条件为原来两个表的条件加新的连接条件
        self.tname_attributes_map.setdefault(tname, attributes)
        weight = self.tname_weight.get(tname1) + self.tname_weight.get(tname2)
        self.tname_weight.setdefault(tname, weight)

    def __is_same(self, s1, s2):
        if len(s1) != len(s2):
            return False
        for s in s1:
            if s in s2:
                continue
            else:
                return False
        return True

    #  计算优先级
    def __get_pri_sql(self, sql):
        total_time = 0
        time_start = time.time()
        try:
            self.cur.execute(sql)
            self.cur.fetchall()
            self.conn.commit()
        except:
            self.conn.rollback()
        time_end = time.time()
        total_time += time_end - time_start
        return total_time

    # 辅助函数得到当前表包含的单表的名字
    def __get_allname(self, tname):
        count = 0
        tnames = set()
        maps = self.tname_attributes_map.get(tname)
        for key in maps:
            if count % 3 == 0:
                temp = key.split(".")[0]
                tnames.add(temp)
            count = count + 1
        return tnames

    # 辅助函数：判断当前连接条件是否符合要求
    def __is_contains(self, condition, tname_Set, tname):
        for name in tname_Set:
            if name in condition:
                new_set = set()
                new_set.add(condition)
                new_set = new_set.union(self.tname_condition.get(tname))
                for s in self.tname_condition:
                    if len(new_set - self.tname_condition.get(s)) == 0:
                        return False
                return True
        return False

    # 根据所有的连接条件生成合并的表
    # condition是原set集合
    # 根据原set将所有可能的合并表都创建出来
    def __create_table_pri(self, condition_set, length, tmap):
        temp_condition_set = set()
        for s in self.tname_condition:
            if len(self.table_attributes.get(s)) == length:
                temp_condition_set.add(s)

        for s in temp_condition_set:  # 遍历所有生成的表

            tnames = self.__get_allname(s)
            for condition in condition_set:

                if condition not in self.tname_condition.get(s):
                    if self.__is_contains(condition, tnames, s):
                        temp = condition.split("=")
                        temp1 = temp[0].split(".")[0]
                        temp2 = temp[1].split(".")[0]
                        t = "tp" + str(self.max_tnum_pri)
                        self.max_tnum_pri = self.max_tnum_pri + 1
                        if temp1 in tnames:
                            self.__merge_pri(s, tmap.get(temp2), t, condition)
                        else:
                            self.__merge_pri(tmap.get(temp1), s, t, condition)

    # 在sql0的基础上改写为创建的sql语句
    def __rewrite_sql_pri(self, tables_set, sql0, sql0_conlist):
        sql_select = sql0.split("from")[0]
        sql_where = " where "
        sql_from = "from "
        for con in sql0_conlist:
            flag = 0
            for s in tables_set:
                if con in self.tname_condition.get(s):
                    flag = 1
                    break
            if flag == 0:
                sql_where = sql_where + con + " and "
        if sql_where == " where ":
            sql_where = ""
        else:
            sql_where = sql_where[0:-5]

        for table in tables_set:
            sql_from = sql_from + table + ","
            map = self.tname_attributes_map.get(table)
            for att in map:
                sql_where = sql_where.replace(att, map.get(att))
                sql_select = sql_select.replace(att, map.get(att))
        sql_from = sql_from[0:-1]
        sql = sql_select + sql_from + sql_where
        return sql

    def __rewrite_sql(self, sql):
        conditions = sql.split('where')[1].strip()  # 获取sql的where部分
        sql0 = sql.split("where")[0] + " where "
        sql0_conlist = []  # like ['a.s=b.s', 'b.o=c.s', 'a.o="高级用户"', 'c.o="大学"']
        pcond, scond, ocond, sscond, socond, oscond, oocond = self.__parse_where(conditions)  # 解析各条件

        condition_pri = []  # 用于构建2个表合并使用, like [['a', 'b', ['s', 's']], ['b', 'c', ['o', 's']]]
        condition_pri_tri = set()  # 用于构建三个表及以上的合并使用, like {'a.s=b.s', 'b.o=c.s'}
        for con in sscond:
            c = [con[0], con[1], ["s", "s"]]
            con_str = con[0] + ".s=" + con[1] + ".s"
            sql0_conlist.append(con_str)
            condition_pri_tri.add(con_str)
            condition_pri.append(c)
        for con in socond:
            c = [con[0], con[1], ["s", "o"]]
            con_str = con[0] + ".s=" + con[1] + ".o"
            sql0_conlist.append(con_str)
            condition_pri_tri.add(con_str)
            condition_pri.append(c)
        for con in oscond:
            c = [con[0], con[1], ["o", "s"]]
            con_str = con[0] + ".o=" + con[1] + ".s"
            sql0_conlist.append(con_str)
            condition_pri_tri.add(con_str)
            condition_pri.append(c)
        for con in oocond:
            c = [con[0], con[1], ["o", "o"]]
            con_str = con[0] + ".o=" + con[1] + ".o"
            sql0_conlist.append(con_str)
            condition_pri_tri.add(con_str)
            condition_pri.append(c)
        for con in scond:
            con_str = con + ".s=\'" + scond[con] + "\'"
            sql0_conlist.append(con_str)
        for con in ocond:
            con_str = con + ".o=\'" + ocond[con] + "\'"
            sql0_conlist.append(con_str)

        # 处理单表的过程
        hashtp_pri_name = {}  # 建立原表到新生成的单表的映射信息
        # 首先创建对应pcond中谓词数量的单表
        weight = 0
        single_tname_set = set()
        for key in pcond:  # 单属性条件中每个属性拆出一个表
            weight = weight + 1
            self.table_number = self.table_number + 1
            tname = "tp" + str(self.max_tnum_pri)
            self.max_tnum_pri = self.max_tnum_pri + 1
            hashtp_pri_name.setdefault(key, tname)
            self.tname_map.setdefault(key, pcond[key])
            self.__divide_pri(pcond[key], tname, key, weight)
            single_tname_set.add(tname)
            self.weight_sum = self.weight_sum + weight

        for con in condition_pri:  # 每个连接条件中的2个表合并
            tname1 = hashtp_pri_name.get(con[0])  # TODO why always get?
            tname2 = hashtp_pri_name.get(con[1])
            tname = "tp" + str(self.max_tnum_pri)
            self.max_tnum_pri = self.max_tnum_pri + 1
            con_pri = con[0] + "." + con[2][0] + "=" + con[1] + "." + con[2][1]
            self.__merge_pri(tname1, tname2, tname, con_pri)

        # 创建2个以上的表合并的过程
        for i in range(2, self.table_number):
            num = 3 * i
            self.__create_table_pri(condition_pri_tri, num, hashtp_pri_name)

        # 根据生成的表序列找到所有合适的序列并计算优先级

        # 之前全是建表的逻辑, 之后枚举优先级

        # 改写 sql 为 from tp0 tp1 tp2 这些单属性表, 而不是 a b c, 故可以消除pcond
        pri_flag = 0
        basic_pri_item = None
        while pri_flag < self.max_tnum_pri:  # 当遍历完所有的表序号，则可以结束循环
            stack = []
            stack.append(pri_flag)  # 当前表序号入栈
            stack_top = 0
            table_name = "tp" + str(pri_flag)  # 当前栈底的表名字
            list_weight = self.tname_weight.get(table_name)
            tname_set = self.__get_allname(table_name)
            for i in range(pri_flag + 1, self.max_tnum_pri):
                temp = "tp" + str(i)
                temp_set = self.__get_allname(temp)
                if list_weight + self.tname_weight.get(temp) <= self.weight_sum and len(tname_set & temp_set) == 0:
                    list_weight = list_weight + self.tname_weight.get(temp)
                    stack.append(i)
                    stack_top = stack_top + 1
                    tname_set = tname_set.union(temp_set)
                elif len(tname_set & temp_set) != 0:
                    continue
                else:
                    break
            if list_weight == self.weight_sum:  # 说明当前栈中的组合满足要求,则可构造相应的组合并继续弹栈找新的组合
                success_tables = set()
                for index in range(0, stack_top + 1):
                    temp = "tp" + str(stack[index])
                    success_tables.add(temp)
                temp_sql = self.__rewrite_sql_pri(success_tables, sql0, sql0_conlist)
                temp_list = [success_tables, temp_sql]
                self.pri_success_tables.append(temp_list)

                if success_tables == single_tname_set:
                    basic_pri_item = temp_list.copy()

            while stack_top > 0:  # 在循环中找到每一个可能的组合
                maxx = stack[stack_top] + 1
                temp_stack = "tp" + str(stack[stack_top])
                temp_set_stack = self.__get_allname(temp_stack)
                tname_set = tname_set - temp_set_stack
                list_weight = list_weight - self.tname_weight.get(temp_stack)

                stack_top = stack_top - 1
                stack.pop()
                for i in range(maxx, self.max_tnum_pri):
                    temp = "tp" + str(i)
                    temp_set = self.__get_allname(temp)
                    if list_weight + self.tname_weight.get(temp) <= self.weight_sum and len(
                            tname_set & temp_set) == 0:
                        list_weight = list_weight + self.tname_weight.get(temp)
                        stack.append(i)
                        stack_top = stack_top + 1
                        tname_set = tname_set.union(temp_set)
                    elif len(tname_set & temp_set) != 0:
                        continue
                    else:
                        break
                if list_weight == self.weight_sum:  # 说明当前栈中的组合满足要求,则可构造相应的组合并继续弹栈找新的组合

                    success_tables = set()
                    for index in range(0, stack_top + 1):
                        temp = "tp" + str(stack[index])
                        success_tables.add(temp)
                    temp_sql = self.__rewrite_sql_pri(success_tables, sql0, sql0_conlist)
                    temp_list = [success_tables, temp_sql]
                    self.pri_success_tables.append(temp_list)

            pri_flag = pri_flag + 1

        # TODO
        # 增加有关t0的优先级项目
        if basic_pri_item:
            basic_pri_item[0] = list(basic_pri_item[0])
            length = len(basic_pri_item[0])
            maxx = 1 << length
            for i in range(1, maxx):
                rewritten_basic_pri_item = [e.copy() if type(e) == list else e for e in basic_pri_item]
                bits = self.__int2bit(i, length)
                current_index = 0
                for j, bit in enumerate(bits):
                    if bit == 1:
                        replaced_tname = rewritten_basic_pri_item[0][j]
                        rewritten_basic_pri_item[0][j] = "t0"

                        sql_split_from_left, sql_split_from_right = rewritten_basic_pri_item[1].split('from')

                        # change from right
                        sql_split_from_right = sql_split_from_right.split(replaced_tname)
                        sql_split_from_right = sql_split_from_right[0] \
                                               + "t0 " + chr(97 + current_index) \
                                               + chr(97 + current_index).join(sql_split_from_right[1:])
                        # change from left
                        sql_split_from_left = sql_split_from_left.replace(replaced_tname, chr(97 + current_index))

                        # construct new sql
                        rewritten_basic_pri_item[1] = sql_split_from_left + 'from' + sql_split_from_right

                        # add p condition of t0
                        rewritten_basic_pri_item[1] += " and {0}.p=\'{1}\'".format(chr(97 + current_index),
                                                                                   self.tname_p[basic_pri_item[0][j]])
                        current_index += 1
                self.pri_success_tables.append(rewritten_basic_pri_item)

        ## debug语句，可注释掉
        for key in self.tname_attributes_map:
            print(key)
            print(self.tname_attributes_map.get(key))
        for succ in self.pri_success_tables:
            print(succ)

    def __int2bit(self, n, length):
        bits = []
        for i in range(length):
            bits.append(n % 2)
            n //= 2
        return bits

    # 返回属性对应顺序
    # {'tp5': {'a.p': 'tp0.p', 'a.s': 'tp0.s', 'a.o': 'tp0.o'}
    # pri add method: {'tp5': {'':'type', 'b':'comment'} }
    def get_attribute_map(self, sql):
        result = {}
        for tname in self.tname_attributes_map:
            names = self.__get_tablename(self.tname_attributes_map.get(tname))
            t = {}
            for i in range(len(names)):
                if i == 0:
                    t.setdefault('', self.tname_map.get(names[i]))
                if i == 1:
                    t.setdefault('b', self.tname_map.get(names[i]))
                if i == 2:
                    t.setdefault('c', self.tname_map.get(names[i]))
                if i == 3:
                    t.setdefault('d', self.tname_map.get(names[i]))
            result.setdefault(tname, t)

        print('')
        print('get attribute map:', result)
        with shelve.open("../data/preprocess/attribute_map.dat") as f:
            f[sql] = result

    # 获得原sql对应的所有优先级组合以及其sql语句
    # 返回结果是list，list中的每个元素是3元组，[set，sql，priority]
    # 结果按优先级从高到低排序，set是表序列，sql是对应的查询语句，priority是优先级
    def get_allpri(self, sql):
        self.__rewrite_sql(sql)

        result = []
        if self.weight_sum == 0:
            s = set()
            s.add("t0")
            result.append((s, sql, 0))
            return result
        for li in self.pri_success_tables:
            time = self.__get_pri_sql(li[1])

            result.append((li[0], li[1], time))
        result.sort(key=lambda x: x[2], reverse=False)

        self.tname_condition['t0'] = set()
        self.tchr2p = dict()  # like {'a': 'type', 'b': 'comment', 'c': 'topic'}

        # select t5.s from t5 where t5.o="senior user" and t5.co="明星"

        # {'tp5': {'a.p': 'tp0.p', 'a.s': 'tp0.s', 'a.o': 'tp0.o'}
        # pri add method: {'tp5': {'':'type', 'b':'comment'} }

        # mydb2 hashtp3: {'tp5': {'type':'', 'comment':'b'} }
        for t_name in self.tname_p.keys():
            character = list(self.tname_attributes_map[t_name].keys())[0].split('.')[0]
            self.tchr2p[character] = self.tname_p[t_name]
        for i in range(len(result)):  # 将每条sql的cond,
            result[i] = list(result[i])
            result[i][0] = list(result[i][0])

            tnames = result[i][0]
            all_t_conditions = list()
            for tname in tnames:  # 先从{'tp0', 'tp4'}形式变为[{'b.o=c.s'}, {'a.s=b.s'}]形式
                if len(self.tname_condition[tname]) == 0 and tname != 't0':  # len(tname_condition[未连接过的表 or 't0']) = 0
                    all_t_conditions.append({self.tname_p[tname]})
                else:  # 连接过的表 or t0
                    all_t_conditions.append(self.tname_condition[tname])

            new_all_t_conditions = list()
            for single_t_conditions in all_t_conditions:  # 再变为[['发布.o=话题.s'], ['用户类型.s=发布.s']]形式   
                new_single_t_conditions = list()
                for cond in single_t_conditions:
                    if len(cond.split('=')) == 1:  # 说明是谓词, 不用转换就加入
                        new_single_t_conditions.append(cond)
                    else:  # 说明是连接条件, 转换后加入本身及其转置 eg. 'abc'.s='bcd'.o, 'bcd'.o='abc'.s
                        left, right = cond.split('=')  # a.s b.o
                        left_tchr, left_attr = left.split('.')  # a s
                        right_tchr, right_attr = right.split('.')  # b o
                        left = self.tchr2p[left_tchr] + '.' + left_attr  # 'topic'.s
                        right = self.tchr2p[right_tchr] + '.' + right_attr  # 'comment'.o
                        new_cond = left + '=' + right  # 'topic'.s = 'comment'.o
                        reverse_new_cond = right + '=' + left  # 'comment'.o = 'topic'.s
                        # 存储new_cond及其转置
                        new_single_t_conditions.append(new_cond)  # 'topic'.s = 'comment'.o
                        new_single_t_conditions.append(reverse_new_cond)  # 'comment'.o = 'topic'.s
                        # 存储p
                        new_single_t_conditions.append(self.tchr2p[left_tchr])  # 'topic'
                        new_single_t_conditions.append(self.tchr2p[right_tchr])  # 'comment'
                new_all_t_conditions.append(set(new_single_t_conditions))  # 再变为[{'发布.o=话题.s'}, {'用户类型.s=发布.s'}]形式
            result[i].append(new_all_t_conditions)
            
            
            # TODO 检查连接条件连通性
            for condition_set in result[i][-1]:
                Mydb.check_transitive_join(condition_set)
        
        self.tname_condition.pop('t0') 
        return result

    #  返回所有表的关系模式，用于找到原表序列中相匹配的组合
    #  可用来改写原sql语句
    # 返回的结果是字典，以tp1表为例，tp1->{b.p:tp1.p,b.s:tp1.s,b.o:tp1.o}
    def get_all_tableschema(self):
        return self.tname_attributes_map

    ## 解析得到where中所有的条件
    def __parse_where(self, conditions):
        pcond = {}
        scond = {}
        ocond = {}
        sscond = []
        socond = []
        oscond = []
        oocond = []
        # print('conditions: ', conditions)
        if (conditions.__contains__(" and ")):
            for condition in conditions.split(" and "):
                pmatch = re.match("(.)\\.p\s*=\s*'(.*)'.*", condition.strip())
                pmatch_ = re.match("p\s*=\s*'(.*)'", condition.strip())
                smatch = re.match("(.)\\.s\s*=\s*'(.*)'.*", condition.strip())
                smatch_ = re.match("s\s*=\s*'(.*)'", condition.strip())
                omatch = re.match("(.)\\.o\s*=\s*'(.*)'.*", condition.strip())
                omatch_ = re.match("o\s*=\s*'(.*)'", condition.strip())
                ssmatch = re.match("(.)\\.s\s=\s(.)\\.s", condition.strip())
                somatch = re.match("(.)\\.s\s=\s(.)\\.o", condition.strip())
                osmatch = re.match("(.)\\.o\s=\s(.)\\.s", condition.strip())
                oomatch = re.match("(.)\\.o\s=\s(.)\\.o", condition.strip())
                if pmatch is not None:
                    pcond[pmatch.group(1)] = pmatch.group(2)
                elif pmatch_ is not None:
                    pcond['t0'] = pmatch_.group(1)
                if smatch is not None:
                    scond[smatch.group(1)] = smatch.group(2)
                elif smatch_ is not None:
                    scond['t0'] = smatch_.group(1)
                if omatch is not None:
                    ocond[omatch.group(1)] = omatch.group(2)
                elif omatch_ is not None:
                    ocond['t0'] = omatch_.group(1)
                if ssmatch is not None:
                    sscond.append((ssmatch.group(1), ssmatch.group(2)))
                if somatch is not None:
                    socond.append((somatch.group(1), somatch.group(2)))
                if osmatch is not None:
                    oscond.append((osmatch.group(1), osmatch.group(2)))
                if oomatch is not None:
                    oocond.append((oomatch.group(1), oomatch.group(2)))
        return pcond, scond, ocond, sscond, socond, oscond, oocond


if __name__ == '__main__':
    
    #parser = JoinParser("t0")

    # 读取工作负载，并将工作负载和优先级序列一一映射写到配置文件暗中
    workloads = None
    with open("../data/sql_queries_test", "r", encoding="utf8") as f:
        workloads = f.readlines()

    f1 = shelve.open("../data/preprocess/priority.dat")
    f2 = shelve.open("../data/preprocess/ps_in_workload.dat")
    for sql in workloads:
        #sql = parser.parse_transfer(workload)
        pri = Priority()
        
        result = pri.get_allpri(sql)
        f1[sql] = result
        f2[sql] = list(pri.tname_p.values())
        
        pri.get_attribute_map(sql)

        pri.re_init()
        print('sql:', sql)
        print('Shelve Result:', f1[sql])
        print('Shelve Ps:', f2[sql])
        print('')
        print('')
    f1.close()
    f2.close()

    import preprocess.workload_stats
    preprocess.workload_stats.main()
