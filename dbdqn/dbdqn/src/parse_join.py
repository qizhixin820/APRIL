'''
    工作负载转换类。原工作负载都是join格式连接条件，将其转化为where格式连接条件。
    todo 此类计划交给张昊然使用，本代码暂时用这个转换原负载
'''

import re


class JoinParser:
    def __init__(self, origin_name):
        self.origin_name = origin_name  # 转换之后的初始表名

    def __parse_from(self, sql):
        searcher = re.search("from(.*)where", sql)
        if searcher is not None:
            return searcher.group(1).strip()
        else:
            return None

    def parse_transfer(self, sql):
        '''
        解析sql，将join格式的连接条件改为where格式。转换后，from子句中的表明均以“, ”分割，连接条件均附加在原where子句之后，sql尾部仍保留分号。
        :param sql: 原sql
        :return: 转换后的sql
        '''
        sql = sql.replace("Select", "select")
        from_clause = self.__parse_from(sql)  # 解析from子句
        if from_clause:
            # 获取表名列表、连接条件列表
            table_names = re.findall("graph\\s+(\\S+)", from_clause)
            conditions = re.findall("(\\S+\\s+=\\s+\\S+)", from_clause)

            # 构造新的from子句
            new_from_clause = ""
            for i in range(len(table_names)):
                new_from_clause += self.origin_name + " " + table_names[i] + ", "
            if new_from_clause.endswith(", "):
                new_from_clause = new_from_clause[:-2]

            # from子句更新
            sql = sql.replace(from_clause, new_from_clause)

            # 构造连接条件字串，附加在新的where子句之后
            append_where_clause = ""
            for i in range(len(conditions)):
                append_where_clause += " and " + conditions[i]
            if sql.endswith(";"):
                sql = sql.strip()[:-1]
            sql = sql.strip() + append_where_clause
            if not sql.endswith(";"):
                sql += ";"
        return sql


if __name__ == '__main__':
    join_parser = JoinParser('t0')
    sql = "Select a.s from graph a join graph b on a.s = b.s where a.p = 'type' and a.o = 'senior user' and b.p = 'comment' and c.p = 'topic' and c.o = '抖音';"
    join_parser.parse_transfer(sql)
