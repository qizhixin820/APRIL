import re
import queue

JOIN_TO_TYPE = {
    ("s", "s"): 1,
    ("s", "o"): 2,
    ("o", "s"): 3,
    ("o", "o"): 4
}
TYPE_TO_JOIN = ["OCCUPIED", ("s", "s"), ("s", "o"), ("o", "s"), ("o", "o")]


class Entity:
    def __init__(self, s=None, p=None, o=None, t=None):
        self.s = s
        self.p = p
        self.o = o
        self.t = t

    def __str__(self):
        return "%s: <%s, %s, %s>" % (self.t, self.s, self.p, self.o)


def __parse_rdf(sparql):
    '''
    解析 rdf
    :param sparql:
    :return:
    '''
    rdf_match = re.search("PREFIX rdf: <(.*)>", sparql, re.M)
    if rdf_match:
        return rdf_match.group(1)
    else:
        return None


def __parse_ub(sparql):
    '''
    解析 ub
    :param sparql:
    :return:
    '''
    ub_match = re.search("PREFIX ub: <(.*)>\n", sparql, re.M)
    if ub_match:
        return ub_match.group(1)
    else:
        return None


def __parse_select(sparql):
    '''
    解析 select，返回一个 list
    :param sparql:
    :return: e.g. ['X', 'Y']
    '''
    select_match = re.search("SELECT (.*)\n", sparql, re.M)
    if select_match:
        select_clause = select_match.group(1)
    else:
        select_clause = None
    select_conditions = select_clause.split(",")
    for i in range(len(select_conditions)):
        select_conditions[i] = select_conditions[i].replace("?", "").strip()
    return select_conditions


def __parse_where(sparql):
    '''
    解析 where，返回一个 list
    :param sparql:
    :return: e.g. ['?X rdf:type ub:Chair', '?Y rdf:type ub:Department', '?X ub:worksFor ?Y', '?Y ub:subOrganizationOf <http://wwwUniversity0edu>']
    '''
    where_match = re.search("WHERE\n{(.*)}", sparql, re.S)
    if where_match:
        where_clause = where_match.group(1)
    else:
        where_clause = None
    wheres = where_clause.split(".\n")
    for i in range(len(wheres)):
        wheres[i] = wheres[i].strip()
    return wheres


def __construct_entities(wheres_list, rdf, ub):
    '''
    根据解析 where 子句得到的结果，以及 rdf 和 ub，构造实体列表，返回一个 list
    :param wheres_list:
    :param rdf:
    :param ub:
    :return: a list of entities. e.g. a: <?X, http://www.w3.org/1999/02/22-rdf-syntax-ns#type, http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Chair>
    '''
    entities = []
    char = ord('a')
    for i in range(len(wheres_list)):
        info = wheres_list[i].split()
        assert len(info) == 3
        for j in range(len(info)):
            info[j] = info[j].replace("rdf:", rdf)
            info[j] = info[j].replace("ub:", ub)
            info[j] = re.sub("[<|>]", "", info[j])
        entities.append(Entity(info[0], info[1], info[2], chr(char)))
        char += 1
    return entities


def __construct_queue_map(entities):
    '''
    根据实体列表，初始化队列映射，返回一个 map
    :param entities:
    :return: used for constructing sql about join. e.g. {'X': queue.Queue(), 'Y': queue.Queue()}
    '''
    queue_map = {}
    for i in range(len(entities)):
        if entities[i].s.startswith("?"):
            c = entities[i].s.replace("?", "")
            if c not in queue_map:
                queue_map[c] = queue.Queue()
        if entities[i].p.startswith("?"):
            c = entities[i].p.replace("?", "")
            if c not in queue_map:
                queue_map[c] = queue.Queue()
        if entities[i].o.startswith("?"):
            c = entities[i].o.replace("?", "")
            if c not in queue_map:
                queue_map[c] = queue.Queue()
    return queue_map


def __construct_join_conditions(entities, queue_map):
    '''
    根据实体列表和初始化的队列映射，构造连接条件列表，其中每一个元素都是一个三元组，返回一个 list
    :param entities:
    :param queue_map:
    :return: e.g. [('a', 'c', 1), ('b', 'c', 2), ('c', 'd', 3)]
    '''
    join_conditions = []
    for i in range(len(entities)):
        if entities[i].s.startswith("?"):
            c = entities[i].s.replace("?", "")
            if not queue_map[c].empty():
                last_tuple = queue_map[c].get()
                join_conditions.append((last_tuple[0], entities[i].t, JOIN_TO_TYPE[(last_tuple[1], "s")]))
            queue_map[c].put((entities[i].t, "s"))
        if entities[i].p.startswith("?"):
            c = entities[i].p.replace("?", "")
            if not queue_map[c].empty():
                last_tuple = queue_map[c].get()
                join_conditions.append((last_tuple[0], entities[i].t, JOIN_TO_TYPE[(last_tuple[1], "p")]))
            queue_map[c].put((entities[i].t, "p"))
        if entities[i].o.startswith("?"):
            c = entities[i].o.replace("?", "")
            if not queue_map[c].empty():
                last_tuple = queue_map[c].get()
                join_conditions.append((last_tuple[0], entities[i].t, JOIN_TO_TYPE[(last_tuple[1], "o")]))
            queue_map[c].put((entities[i].t, "o"))
    return join_conditions


def __construct_from_sql(join_conditions):
    '''
    根据连接条件列表，构造 sql 中的 from 子句
    :param join_conditions:
    :return: e.g. t0 c, t0 b, t0 d, t0 a
    '''
    from_sql = ""
    table_set = set()
    for join_condition in join_conditions:
        table_set.add(join_condition[0])
        table_set.add(join_condition[1])
    table_list = list(table_set)
    for i in range(len(table_list)):
        from_sql += "t0 " + table_list[i]
        if i < len(table_list) - 1:
            from_sql += ", "
    from_sql = from_sql.strip()
    return from_sql


def __construct_where_sql(join_conditions, entities):
    '''
    根据连接条件列表和实体列表，构造 sql 中的 where 子句，同时构造一个 map，用于后面 select 子句的构造
    :param join_conditions:
    :param entities:
    :returns where_sql: e.g.  a.s = c.s and b.s = c.o and c.o = d.s and a.p = http://www.w3.org/1999/02/22-rdf-syntax-ns#type and a.o =...[truncated]
    :returns select_map: Besides, it returns a map. e.g. {'X': a, 'Y': b}
    '''
    where_sql = ""
    select_map = {}
    for join_condition in join_conditions:
        where_sql += " and %s.%s = %s.%s" % (
            join_condition[0], TYPE_TO_JOIN[join_condition[2]][0], join_condition[1],
            TYPE_TO_JOIN[join_condition[2]][1])
    for entity in entities:
        if not entity.s.startswith("?"):
            where_sql += " and %s.s = '%s'" % (entity.t, entity.s)
        else:
            c = entity.s.replace("?", "")
            if c not in select_map:
                select_map[c] = "%s.%s" % (entity.t, "s")
        if not entity.p.startswith("?"):
            where_sql += " and %s.p = '%s'" % (entity.t, entity.p)
        else:
            c = entity.p.replace("?", "")
            if c not in select_map:
                select_map[c] = "%s.%s" % (entity.t, "p")
        if not entity.o.startswith("?"):
            where_sql += " and %s.o = '%s'" % (entity.t, entity.o)
        else:
            c = entity.o.replace("?", "")
            if c not in select_map:
                select_map[c] = "%s.%s" % (entity.t, "o")
    where_sql = where_sql.strip()
    if where_sql.startswith("and"):
        where_sql = where_sql.replace("and", "", 1)
    where_sql = where_sql.strip()
    return where_sql, select_map


def __construct_select_sql(select_conditions, select_map):
    '''
    根据之前得到的 select 条件列表和辅助 select 映射，构造 sql 中的 select 子句
    :param select_conditions:
    :param select_map:
    :return: e.g. a.s, b.s
    '''
    select_sql = ""
    for i in range(len(select_conditions)):
        select_sql += select_map[select_conditions[i]]
        if i < len(select_conditions) - 1:
            select_sql += ", "
    select_sql = select_sql.strip()
    return select_sql


def sparql2sql(sparql):
    rdf = __parse_rdf(sparql)
    ub = __parse_ub(sparql)

    select_conditions_list = __parse_select(sparql)
    wheres_list = __parse_where(sparql)

    entities = __construct_entities(wheres_list, rdf, ub)

    queue_map = __construct_queue_map(entities)

    join_conditions = __construct_join_conditions(entities, queue_map)

    from_sql = __construct_from_sql(join_conditions)
    if from_sql == "":
        for entity in entities:
            if entity.s.startswith("?") or entity.p.startswith("?") or entity.o.startswith("?"):
                from_sql += "t0 " + entity.t + ", "
        if from_sql.endswith(", "):
            from_sql = from_sql[:-2]
    where_sql, select_map = __construct_where_sql(join_conditions, entities)
    select_sql = __construct_select_sql(select_conditions_list, select_map)

    sql = "select %s from %s where %s" % (select_sql, from_sql, where_sql)
    print(sql)


if __name__ == '__main__':
    test = None
    with open("../../data/test_sparql2sql/test12.txt", "r") as f:
        test = f.read()
        f.close()
    sparql2sql(test)
