import re
import time
from itertools import permutations

import MySQLdb

from mydb2 import MydbLogger
from parse_join import JoinParser
from constants import *


def int2quaternary(n, length):
    """
    将十进制的n转换成四进制的数，并以长度为length的数组返回，数组中每一个元素表示四进制数的一位
    :param n:
    :param length:
    :return:
    """
    bits = []
    for i in range(length):
        bits.append(n % 4)
        n //= 4
    return bits


class MView:
    """

    """
    username = "root"  # 数据库连接用户名
    password = MYSQL_PWD  # 数据库连接密码
    port = 3306  # 数据库连接端口

    def __init__(self, filepath):
        self.parse_join = JoinParser("t0")
        self.query_count = 0

        self.LOGGING = MydbLogger("../../data/contrast-log.txt")
        self.__init_ps()  # 初始化谓词集合
        self.__init_query_frequency(filepath)  # 初始化常用查询DICT

        # 关于结构的...
        self.level_nodes = dict()  # level->set(节点)
        self.graph = dict()  # 节点->set(父节点)
        self.candidate_view = dict()  # DICT（节点->被物化频率）
        self.materialized_view = set()  # MV

        # 辅助的...
        self.tables_rows = dict()  # DICT （节点->行数）
        self.join_cost_cache = dict()
        self.node2tname = dict()
        self.max_tnum = 0

    def __init_ps(self):
        """
        初始化谓词集合
        :return:
        """
        conn = MySQLdb.connect(host='127.0.0.1', user=self.username, passwd=self.password, port=self.port, db='mydb',
                               charset='utf8')  # 连接数据库
        cur = conn.cursor()
        cur.execute("select distinct p from t0")
        temp_ps = cur.fetchall()
        self.ps = set(x[0] for x in temp_ps)
        cur.close()
        conn.close()
        self.LOGGING.write("初始化谓词集合完成，谓词共 {0} 个。".format(len(self.ps)))

    def __init_query_frequency(self, filepath):
        """
        初始化常用查询DICT（query->频率）
        :param filepath:    工作负载文件路径
        :return:
        """
        self.query_frequency = dict()
        with open(filepath, "r", encoding="utf8") as f:
            workloads = [self.parse_join.parse_transfer(x.strip()) for x in f.readlines()]
        if workloads:
            for workload in workloads:
                if workload:
                    self.query_frequency.setdefault(workload, 0)
            for workload in workloads:
                if workload:
                    self.query_frequency[workload] += 1
            self.query_count = len(workloads)
            for key in self.query_frequency.keys():
                self.query_frequency[key] /= self.query_count
        self.LOGGING.write("初始化常用查询完成，常用（非重复）查询共 {0} 条。".format(len(self.query_frequency.keys())))

    def __init_tables_rows(self):
        """
        对当前偏序图上的所有节点，求得其连接结果的行数，存储在self.tables_rows中
        :return:
        """
        conn = MySQLdb.connect(host='127.0.0.1', user=self.username, passwd=self.password, port=self.port, db='mydb',
                               charset='utf8')  # 连接数据库
        cur = conn.cursor()
        for node in self.graph.keys():
            sql = self.__construct_sql_from_node(node)
            self.LOGGING.write("sql in __init_tables_rows: " + sql, True)
            cur.execute(sql)
            rows = cur.fetchone()[0]
            self.tables_rows[node] = rows
        cur.close()
        conn.close()
        self.LOGGING.write("表行数统计完成。")

    def partial_order_graph(self):
        """
        根据谓词全集，生成偏序图
        每一层的节点数公式：4^(level-1) * |Permutation(level)| / 2 * |Combination(level,8)|
        :return:
        """
        max_join = 3
        max_level = max_join + 1

        # 初始化层-节点dict
        for level in range(max_level):
            self.level_nodes.setdefault(level, set())

        # 第一层节点构造
        for p in self.ps:
            new_node = frozenset([p])
            self.graph.setdefault(new_node, set())
            self.level_nodes[0].add(new_node)

        self.LOGGING.write("第一层节点构造完成，当前共 {0} 个节点。".format(len(self.graph.keys())), True)

        p_combination = list()
        # 每一层都是p组合集合
        for i in range(max_level):
            p_combination.append(list())
        # 第一层的组合
        p_combination[0] = list(self.graph.keys())
        # 第二层的组合
        for i in range(len(p_combination[0]) - 1):
            for j in range(i + 1, len(p_combination[0])):
                p_combination[1].append(p_combination[0][i].union(p_combination[0][j]))
        # 第三层及以上的组合
        for level in range(2, max_level):
            for p_comb in p_combination[level - 1]:
                for p in p_combination[0]:
                    if not p.intersection(p_comb) and p_comb.union(p) not in p_combination[level]:
                        p_combination[level].append(p_comb.union(p))

        # 第二层的节点构造，以及父子关系构造
        for join_set in p_combination[1]:
            # self.ps2node.setdefault(join_set, set())
            temp_list = list(join_set)
            for c1 in ('o', 's'):
                for c2 in ('o', 's'):
                    new_node = join_set.union({"{0}.{1}={2}.{3}".format(temp_list[0], c1, temp_list[1], c2)}).union({
                        "{0}.{1}={2}.{3}".format(temp_list[1], c2, temp_list[0], c1)})
                    self.graph.setdefault(new_node, set())
                    # self.graph[frozenset([temp_list[0]])].add(new_node)
                    # self.graph[frozenset([temp_list[1]])].add(new_node)
                    self.graph[new_node].add(frozenset([temp_list[0]]))
                    self.graph[new_node].add(frozenset([temp_list[1]]))
                    self.level_nodes[1].add(new_node)

        self.LOGGING.write("第二层节点构造完成，当前共 {0} 个节点。".format(len(self.graph.keys())), True)

        # 第三层及以上的节点构造，以及父子关系构造
        for level in range(2, max_level):
            # 当前层
            for join_set in p_combination[level]:
                p_list = list(join_set)
                # p_list是p的组合，要求得所有可能的连接顺序，需要获取p_list的全排列（两两重复，但在set结构中去重了）
                permutation = list(permutations(list(x for x in range(len(p_list)))))
                for each_perm in permutation:  # 遍历每一种连接顺序
                    possible_p_list = list()  # 临时存放连接条件
                    for index in range(len(each_perm) - 1):
                        p1 = p_list[each_perm[index]]
                        p2 = p_list[each_perm[index + 1]]
                        possible_p_list.append((p1, p2))  # 构造连接条件，并加入到临时变量中
                    possible_char_tuple = (('o', 'o'), ('o', 's'), ('s', 'o'), ('s', 's'))  # 连接属性条件指示变量
                    # 连接属性条件有4种情况，设连接条件有n个，则需要遍历4^n次
                    for i in range(pow(4, len(possible_p_list))):
                        # 每次遍历，使用位转换函数，定位具体选择的连接属性条件
                        quaternary = int2quaternary(i, len(possible_p_list))
                        new_node = join_set.copy()  # 拷贝，不能修改join_set
                        # 构造连接条件，每个条件反向构造一次（hashtp2的格式）
                        for index, item in enumerate(possible_p_list):
                            new_node = new_node.union(
                                {"{0}.{1}={2}.{3}".format(item[0], possible_char_tuple[quaternary[index]][0], item[1],
                                                          possible_char_tuple[quaternary[index]][1])})
                            new_node = new_node.union(
                                {"{0}.{1}={2}.{3}".format(item[1], possible_char_tuple[quaternary[index]][1], item[0],
                                                          possible_char_tuple[quaternary[index]][0])})
                        self.graph.setdefault(new_node, set())  # 当前层节点生成
                        self.level_nodes[level].add(new_node)  # 当前节点加入层-节点dict中
                        # 遍历上一层的每一个节点，凡是当前节点的子集的，均为当前节点的子节点（使用集合结构表示父节点集，用于去重）
                        for last_level_node in self.level_nodes[level - 1]:
                            if last_level_node.issubset(new_node):
                                # self.graph[last_level_node].add(new_node)
                                self.graph[new_node].add(last_level_node)
            self.LOGGING.write("第{1}层节点构造完成，当前共 {0} 个节点。".format(len(self.graph.keys()), level + 1), True)

    def choose_candidate_view(self):
        """
        候选视图选择，得到候选视图被物化的频率
        :return:
        """
        for query in self.query_frequency.keys():
            node = self.__construct_node_from_sql(query)
            ancestors = list()
            stack = list()
            stack.append(node)
            while len(stack) > 0:
                temp_pop = stack.pop()
                ancestors.append(temp_pop)
                if temp_pop not in self.graph.keys():
                    continue
                else:
                    for parent in self.graph[temp_pop]:
                        stack.append(parent)
            for ancestor in ancestors:
                self.candidate_view.setdefault(ancestor, 0)
                self.candidate_view[ancestor] += self.query_frequency[query]
        self.LOGGING.write("候选视图选择完成，共 {0} 个候选视图".format(len(self.candidate_view.keys())), True)

    def benefit(self, node):
        """ 
        获取节点node的物化收益
        :param node:
        :return:
        """
        # 不能用单表的行数估计代价了, 还要给父节点和t0表连接, 连接后的行数才是代价, 后面改写sql也这个思路了。
        f_t0_changed = 0  # t0的更新频率
        stack = list()
        stack.append(node)
        m = None
        min_cost = float('inf')  # float的最大值
        # 遍历节点node的所有祖宗节点
        while len(stack) > 0:
            pop_elem = stack.pop()
            # 寻找node节点的合法父节点中，行数最小的节点m
            # 只有node的已被物化的节点视为合法
            temp_cost = self.__join_cost(node, pop_elem)
            if pop_elem != node and pop_elem in self.materialized_view and temp_cost < min_cost:
                m = pop_elem
                min_cost = temp_cost
            # node的所有父节点进栈
            for parent in self.graph[pop_elem]:
                stack.append(parent)
        # todo 第二项是否还需要用到table_rows？
        if m:
            return self.candidate_view[node] * min_cost - f_t0_changed * \
                   self.tables_rows[node]
        else:  # 如果m为空，则代价为全部从t0中查找的连接代价
            return self.candidate_view[node] * self.__join_cost(node, frozenset()) - f_t0_changed * \
                   self.tables_rows[node]

    def get_total_time(self):
        conn = MySQLdb.connect(host='127.0.0.1', user=self.username, passwd=self.password, port=self.port,
                               db='mydb',
                               charset='utf8')  # 连接数据库
        cur = conn.cursor()
        total_time = 0
        for query in self.query_frequency.keys():
            query_ = self.__rewrite_query(query)
            start_time = time.time()
            cur.execute(query_)
            end_time = time.time()
            total_time += (end_time - start_time) * (self.query_count * self.query_frequency[query])
            self.LOGGING.write(
                "负载 {0} 执行完成，频数为 {1}，单次时间为 {2} 秒".format(query, str(self.query_count * self.query_frequency[query]),
                                                         str(end_time - start_time)))
        self.LOGGING.write("所有负载执行完成，总查询时间：{0} 秒".format(str(total_time)))

    def __rewrite_query(self, query):
        # 改造__construct_node_from_sql(self, sql)
        # 得到和query对应的节点FROZENSET
        # 得到query中的o和s的等值条件，以及投影条件
        print(query)
        node = set()
        join_conditions = re.findall("\\S+\\.\\w\\s*=\\s*\\S+\\.\\w", query)
        o_conditions = re.findall("\\S+\\.o\\s*=\\s*'\\S+'", query)
        s_conditions = re.findall("\\S+\\.s\\s*=\\s*'\\S+'", query)
        p_conditions_dict = MView.get_p_conditions_dict(query)
        o_conditions_dict = dict()
        s_conditions_dict = dict()
        for value in p_conditions_dict.values():
            node.add(value)  # p加入节点
        for o_cond in o_conditions:
            o_cond_match = re.match("(\\S+)\\.o\\s*=\\s*'(\\S+)'", o_cond.strip())
            if o_cond_match:
                o_conditions_dict[p_conditions_dict[o_cond_match.group(1)]] = o_cond_match.group(2)
            else:
                print("error")
        for s_cond in s_conditions:
            s_cond_match = re.match("(\\S+)\\.s\\s*=\\s*'(\\S+)'", s_cond.strip())
            if s_cond_match:
                s_conditions_dict[p_conditions_dict[s_cond_match.group(1)]] = s_cond_match.group(2)
            else:
                print("error")
        for join_cond in join_conditions:
            join_cond_match = re.match("(\\S+)\\.(\\w)\\s*=\\s*(\\S+)\\.(\\w)", join_cond.strip())
            if join_cond_match:
                node.add("{0}.{1}={2}.{3}".format(p_conditions_dict[join_cond_match.group(1)],
                                                  join_cond_match.group(2), p_conditions_dict[join_cond_match.group(3)],
                                                  join_cond_match.group(4)))
                node.add("{0}.{1}={2}.{3}".format(p_conditions_dict[join_cond_match.group(3)],
                                                  join_cond_match.group(4), p_conditions_dict[join_cond_match.group(1)],
                                                  join_cond_match.group(2)))
            else:
                print("error")

        # 获取原sql的投影信息
        project_items_dict = dict()
        select_clause = re.search("select(.*)from", query)
        for project_item in select_clause.group(1).strip().split(","):
            left, right = project_item.strip().split(".")
            project_items_dict.setdefault(p_conditions_dict[left], list())
            project_items_dict[p_conditions_dict[left]].append(right)
        node = frozenset(node)
        print(node)
        print(o_conditions_dict)
        print(s_conditions_dict)
        print(project_items_dict)

        if node in self.materialized_view:
            # 在self.node2tname[node]上查
            # select 子句
            select_sql = "select "
            select_list = list()
            for key, value in project_items_dict.items():
                for project_cond in value:
                    select_list.append(key + project_cond)
            select_sql += ", ".join(select_list)
            select_sql += " "

            # from 子句
            from_sql = "from " + self.node2tname[node]
            from_sql += " "

            # where 子句
            where_sql = "where "
            where_list = list()
            for key, value in o_conditions_dict.items():
                where_list.append(key + "o=" + "'{0}'".format(value))
            for key, value in s_conditions_dict.items():
                where_list.append(key + "s=" + "'{0}'".format(value))
            where_sql += " and ".join(where_list)

            sql = select_sql + from_sql + where_sql + ";"
            print(sql)
        else:
            stack = list()
            for parent in self.graph[node]:
                stack.append(parent)
            max_count = 0
            max_node = None
            # 测试用：max_node = frozenset({'gender.s=follow.s', 'gender', 'follow' 'follow.s=gender.s' 'topic'})
            # 测试用：self.node2tname[max_node] = "t2"
            while len(stack) > 0:
                pop_elem = stack.pop()
                if pop_elem in self.materialized_view and len(pop_elem) > max_count:
                    max_count = len(pop_elem)
                    max_node = pop_elem
            if max_node:  # 基于max_node和t0查

                sql = self.__construct_sql_with_m_and_node(node, max_node, project_items_dict) + ";"
            else:  # 在t0上查
                sql = query
                print("在t0上查", sql)
            return sql

    def __join_cost(self, node, m):
        conn = MySQLdb.connect(host='127.0.0.1', user=self.username, passwd=self.password, port=self.port,
                               db='mydb',
                               charset='utf8')  # 连接数据库
        cur = conn.cursor()
        if (node, m) in self.join_cost_cache.keys():
            return self.join_cost_cache[(node, m)]
        '''
        node:   {'comment', 'topic', 'comment.o=topic.s', 'type', 'topic.s=comment.o', 'comment.s=type.s', 'type.s=comment.s'}
        m:      {'comment', 'topic', 'comment.o=topic.s', 'topic.s=comment.o'}        
        difference: {'type', 'comment.s=type.s', 'type.s=comment.s'}
        '''

        sql = self.__construct_sql_with_m_and_node(node, m) + ";"
        start_time = time.time()
        cur.execute(sql)
        end_time = time.time()
        self.join_cost_cache[(node, m)] = end_time - start_time

        cur.close()
        conn.close()
        return self.join_cost_cache[(node, m)]

    def __space(self, table):
        """
        查询表table所占的内存空间
        :param table: 表名
        :return:
        """
        conn = MySQLdb.connect(host='127.0.0.1', user=self.username, passwd=self.password, port=self.port,
                               db='information_schema',
                               charset='utf8')  # 连接数据库
        cur = conn.cursor()
        cur.execute(
            "select round(sum(data_length/1024/1024), 4) from tables where table_schema='mydb' and table_name = %s",
            (table,))
        result = cur.fetchone()
        return result[0]

    def __construct_sql_with_m_and_node(self, node, m, project_items_dict=None):
        d = node.difference(m)

        # from子句
        from_sql = "from "
        p2char = dict()
        i = 0
        for cond in d:
            if "=" not in cond:
                p2char[cond] = chr(97 + i)
                i += 1
        from_list = ["t0 {0}".format(chr(97 + x)) for x in range(i)]
        if m:
            from_list.append(self.node2tname[m])
        from_sql += ", ".join(from_list) + " "

        # where 子句
        where_sql = "where "
        # 谓词条件
        p_cond_list = list()
        for k, v in p2char.items():
            p_cond_list.append("{0}.p='{1}'".format(v, k))
        where_sql += " and ".join(p_cond_list)
        # 连接条件
        join_cond_set = set()
        for cond in d:
            if "=" in cond:
                join_cond_match = re.match("(\\S+)\\.(\\w)\\s*=\\s*(\\S+)\\.(\\w)", cond)
                if join_cond_match:
                    p1 = join_cond_match.group(1)
                    cond1 = join_cond_match.group(2)
                    p2 = join_cond_match.group(3)
                    cond2 = join_cond_match.group(4)
                    if p1 in p2char:
                        cond = cond.replace(p1, p2char[p1])
                    else:
                        cond = cond.replace(p1 + "." + cond1, self.node2tname[m] + "." + p1 + cond1)
                    if p2 in p2char:
                        cond = cond.replace(p2, p2char[p2])
                    else:
                        cond = cond.replace(p2 + "." + cond2, self.node2tname[m] + "." + p2 + cond2)
                left, right = cond.split("=")
                if cond not in join_cond_set and (right + "=" + left) not in join_cond_set:
                    join_cond_set.add(cond)
        if join_cond_set:
            where_sql += " and " + " and ".join(join_cond_set)

        # select 子句
        select_sql = "select "
        if project_items_dict:
            select_list = list()
            for key, value in project_items_dict.items():
                if key in m:
                    for project_item in value:
                        select_list.append(key + project_item)
                else:
                    for project_item in value:
                        select_list.append(p2char[key] + "." + project_item)
            select_sql += ", ".join(select_list)
            select_sql += " "
        else:
            select_sql += "* "
        return select_sql + from_sql + where_sql

    def __construct_sql_from_node(self, node):
        """
        根据节点node，构造查询行数sql
        :param node:
        :return:
        """
        # select子句
        select_sql = "select count(*) "

        # from子句
        from_sql = "from "
        p2char = dict()
        i = 0
        for cond in node:
            if "=" not in cond:
                p2char[cond] = chr(97 + i)
                i += 1
        from_list = ["t0 {0}".format(chr(97 + x)) for x in range(i)]
        from_sql += ", ".join(from_list) + " "

        # where 子句
        where_sql = "where "
        # 谓词条件
        p_cond_list = list()
        for k, v in p2char.items():
            p_cond_list.append("{0}.p='{1}'".format(v, k))
        where_sql += " and ".join(p_cond_list)
        # 连接条件
        join_cond_set = set()
        for cond in node:
            if "=" in cond:
                join_cond_match = re.match("(\\S+)\\.\\w\\s*=\\s*(\\S+)\\.\\w", cond)
                if join_cond_match:
                    p1 = join_cond_match.group(1)
                    p2 = join_cond_match.group(2)
                    cond = cond.replace(p1, p2char[p1]).replace(p2, p2char[p2])
                left, right = cond.split("=")
                if cond not in join_cond_set and (right + "=" + left) not in join_cond_set:
                    join_cond_set.add(cond)
        if join_cond_set:
            where_sql += " and " + " and ".join(join_cond_set)

        return select_sql + from_sql + where_sql + ";"

    @staticmethod
    def get_p_conditions_dict(sql):
        p_conditions_dict = dict()
        p_conditions = re.findall("\\S+\\.p\\s*=\\s*'\\S+'", sql)
        for p_cond in p_conditions:
            p_cond_match = re.match("(\\S+)\\.p\\s*=\\s*'(\\S+)'", p_cond.strip())
            if p_cond_match:
                p_conditions_dict[p_cond_match.group(1)] = p_cond_match.group(2)
            else:
                print("error")
        return p_conditions_dict

    def choose_materialized_view(self):
        """
        选择并构造物化视图
        :return:
        """
        # 约束
        LIMITED_SPACE = 1000  # MB

        # 初始化MV
        # todo 初始化时是否要加入t0

        # 修改原偏序图，使之成为基于CV的偏序图PCV
        temp_keys = list(self.graph.keys())
        for node in temp_keys:
            if node not in self.candidate_view.keys():
                del self.graph[node]
                for k, v in self.graph.items():
                    if node in v:
                        self.graph[k].remove(node)

        self.LOGGING.write("生成PCV完成（覆盖原偏序图）。", True)

        self.__init_tables_rows()  # 初始化当前所有节点的行数

        # MV中元素所占总空间
        space_total = self.__space("t0")

        # 循环计数
        i = 0
        too_much = set()
        while space_total < LIMITED_SPACE and i <= len(self.graph.keys()):
            self.LOGGING.write("当前循环计数：" + str(i), True)
            self.LOGGING.write("当前MV：" + str(self.materialized_view), True)
            self.LOGGING.write("当前空间：" + str(space_total), True)
            self.LOGGING.write("当前空间溢出黑名单：" + str(too_much), True)

            # 计算PCV中没有被物化的节点的收益
            # 从中选择收益最大的节点v
            max_benefit = 0
            v = None
            for node in set(self.graph.keys()).difference(self.materialized_view):
                b = self.benefit(node)
                print('b:', b)
                if b > max_benefit and b not in too_much:
                    v = node
                    max_benefit = b
            print('max_benefit, v:', max_benefit, v)
            print('')
            # v的收益小于0，没有更多的节点应该被物化，函数返回
            if max_benefit < 0:
                return
            self.LOGGING.write("最大收益节点：" + str(v))
            self.__create_table(v)  # 建表
            self.LOGGING.write("建表完成。")
            v_space = self.__space("t" + str(self.max_tnum))  # 获取空间
            if space_total + v_space <= LIMITED_SPACE:  # 满足空间约束
                self.materialized_view.add(v)  # 待物化
                space_total += v_space
                self.node2tname[v] = "t" + str(self.max_tnum)
                self.LOGGING.write("物化成功，保留。")
            else:  # 不满足空间约束，删表
                conn = MySQLdb.connect(host='127.0.0.1', user=self.username, passwd=self.password, port=self.port,
                                       db='mydb',
                                       charset='utf8')  # 连接数据库
                cur = conn.cursor()
                cur.execute("drop table t" + str(self.max_tnum))
                conn.commit()
                cur.close()
                conn.close()
                self.max_tnum -= 1
                too_much.add(v)
                self.LOGGING.write("物化失败，删除。")
            i += 1
        return

    def __create_table(self, node):
        conn = MySQLdb.connect(host='127.0.0.1', user=self.username, passwd=self.password, port=self.port, db='mydb',
                               charset='utf8')  # 连接数据库
        cur = conn.cursor()

        # select子句
        select_sql = "select * "

        # from子句
        from_sql = "from "
        p2char = dict()
        char2p = dict()
        i = 0
        for cond in node:
            if "=" not in cond:
                p2char[cond] = chr(97 + i)
                char2p[chr(97 + i)] = cond
                i += 1
        from_list = ["t0 {0}".format(chr(97 + x)) for x in range(i)]
        from_sql += ", ".join(from_list) + " "

        # where 子句
        where_sql = "where "
        # 谓词条件
        p_cond_list = list()
        for k, v in p2char.items():
            p_cond_list.append("{0}.p='{1}'".format(v, k))
        where_sql += " and ".join(p_cond_list)
        # 连接条件
        join_cond_set = set()
        for cond in node:
            if "=" in cond:
                left, right = cond.split("=")
                if cond not in join_cond_set and (right + "=" + left) not in join_cond_set:
                    join_cond_match = re.match("(\\S+)\\.\\w\\s*=\\s*(\\S+)\\.\\w", cond)
                    if join_cond_match:
                        p1 = join_cond_match.group(1)
                        p2 = join_cond_match.group(2)
                        cond = cond.replace(p1, p2char[p1]).replace(p2, p2char[p2])
                    join_cond_set.add(cond)
        if join_cond_set:
            where_sql += " and " + " and ".join(join_cond_set)

        query_sql = select_sql + from_sql + where_sql + ";"

        self.LOGGING.write('sql in __create_table: ' + query_sql)
        cur.execute(query_sql)

        results = cur.fetchall()
        conn.commit()

        self.max_tnum += 1
        tname = "t" + str(self.max_tnum)
        varchar_not_null = "VARCHAR(255) NOT NULL"
        pattern_list = list()
        for x in range(i):
            pattern_list.append(char2p[chr(97 + x)] + "p " + varchar_not_null)
            pattern_list.append(char2p[chr(97 + x)] + "s " + varchar_not_null)
            pattern_list.append(char2p[chr(97 + x)] + "o " + varchar_not_null)
        create_sql = "create table if not exists " + tname + " (" + ", ".join(pattern_list) + ");"

        cur.execute(create_sql)
        conn.commit()

        insert_sql = "insert into " + tname + " values(" + ",".join(list("%s" for x in pattern_list)) + ");"

        cur.executemany(insert_sql, results)
        conn.commit()

        cur.close()
        conn.close()

    def __construct_node_from_sql(self, sql):
        """
        根据sql，构造节点FROZENSET
        :param sql:
        :return:
        """

        node = set()
        join_conditions = re.findall("\\S+\\.\\w\\s*=\\s*\\S+\\.\\w", sql)
        p_conditions = re.findall("\\S+\\.p\\s*=\\s*'\\S+'", sql)
        p_conditions_dict = dict()
        for p_cond in p_conditions:
            p_cond_match = re.match("(\\S+)\\.p\\s*=\\s*'(\\S+)'", p_cond.strip())
            if p_cond_match:
                p_conditions_dict[p_cond_match.group(1)] = p_cond_match.group(2)
                node.add(p_cond_match.group(2))  # p加入节点
            else:
                print("error")
        for join_cond in join_conditions:
            join_cond_match = re.match("(\\S+)\\.(\\w)\\s*=\\s*(\\S+)\\.(\\w)", join_cond.strip())
            if join_cond_match:
                node.add("{0}.{1}={2}.{3}".format(p_conditions_dict[join_cond_match.group(1)],
                                                  join_cond_match.group(2), p_conditions_dict[join_cond_match.group(3)],
                                                  join_cond_match.group(4)))
                node.add("{0}.{1}={2}.{3}".format(p_conditions_dict[join_cond_match.group(3)],
                                                  join_cond_match.group(4), p_conditions_dict[join_cond_match.group(1)],
                                                  join_cond_match.group(2)))
            else:
                print("error")
        return frozenset(node)


if __name__ == '__main__':
    materialized_view = MView("../../data/workload_graph_test")
    # materialized_view.join_cost(
    #     frozenset({'comment', 'topic', 'comment.o=topic.s', 'type', 'topic.s=comment.o', 'comment.s=type.s',
    #                'type.s=comment.s'})
    #     , frozenset())
    # materialized_view.partial_order_graph()
    # materialized_view.choose_candidate_view()
    # materialized_view.choose_materialized_view()
