'''
    对工作负载进行统计，统计谓词条件和连接条件。
    结果存储到：
        data/preprocess/workload_stats.dat
'''
import re
import shelve

def main():
    join_type_dict = {
        ("o", "o"): 1,
        ("o", "s"): 2,
        ("s", "o"): 3,
        ("s", "s"): 4
    }
    workloads = None
    with open("../data/sql_queries", "r", encoding="utf8") as f:
        workloads = f.readlines()
    f.close()
    if workloads:
        p_conditions_set = set()
        join_conditions_set = set()
        for i in range(len(workloads)):
            join_conditions = re.findall("\\w+\\.\\w\\s*=\\s*\\w+\\.\\w", workloads[i])
            p_conditions = re.findall("\\S+\\.p\\s*=\\s*'\\S+'", workloads[i])
            p_conditions_dict = dict()
            for p_cond in p_conditions:
                p_cond_match = re.match("(\\S+)\\.p\\s*=\\s*'(\\S+)'", p_cond.strip())
                if p_cond_match:
                    p_conditions_dict[p_cond_match.group(1)] = p_cond_match.group(2)
                    p_conditions_set.add(p_cond_match.group(2))
                else:
                    print("第 {0} 行中的谓词条件 {1} 匹配失败".format(i + 1, p_cond))
            for join_cond in join_conditions:
                join_cond_match = re.match("(\\S+)\\.(\\w)\\s*=\\s*(\\S+)\\.(\\w)", join_cond.strip())
                if join_cond_match:
                    join_conditions_set.add((p_conditions_dict[join_cond_match.group(1)],
                                    p_conditions_dict[join_cond_match.group(3)],
                                    join_type_dict[(join_cond_match.group(2), join_cond_match.group(4))]))
                else:
                    print("第 {0} 行中的连接条件 {1} 匹配失败".format(i + 1, join_cond))
        file = shelve.open("../data/preprocess/workload_stats")
        file["p_conditions_set"] = p_conditions_set
        file["join_conditions_set"] = join_conditions_set
        file.close()
        ### 现在使用shelve对象持久化方式
        ### 以下是把对象以字符串的形式保存，无用。
        # with open("../../data/preprocess/workload_p_stats", "w", encoding="utf8") as f:
        #     for p_cond in p_conditions_set:
        #         f.write(p_cond)
        #         f.write("\n")
        # f.close()
        # with open("../../data/preprocess/workload_condi_stats", "w", encoding="utf8") as f:
        #     for join_cond in join_conditions_set:
        #         f.write("(")
        #         f.write(','.join(str(x) for x in join_cond))
        #         f.write(")")
        #         f.write("\n")
        # f.close()
    else:
        print("读文件为空")

if __name__ == "__main__":
    main()