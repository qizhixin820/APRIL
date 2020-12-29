import re
import shelve
import sys


class FrequencyStats:
    def __init__(self, workload_filepath, savepath):
        self.filepath = workload_filepath
        self.savepath = savepath

    def statistics(self):
        workloads = None
        join_type_dict = {
            ("o", "o"): 1,
            ("o", "s"): 2,
            ("s", "o"): 3,
            ("s", "s"): 4
        }
        with open(self.filepath, "r", encoding="utf8") as f:
            workloads = f.readlines()

        # 负载去重
        # workload_set = set()
        # for workload in workloads:
        #     workload_set.add(workload)

        if workloads:
            overall = []
            p_conditions_set = set()

            print("负载条数：", len(workloads))
            for i in range(len(workloads)):
                join_conditions = re.findall("\\S+\\.\\w\\s*=\\s*\\S+\\.\\w", workloads[i])
                p_conditions = re.findall("\\S+\\.p\\s*=\\s*\"\\S+\"", workloads[i])
                p_conditions_dict = dict()
                for p_cond in p_conditions:
                    p_cond_match = re.match("(\\S+)\\.p\\s*=\\s*\"(\\S+)\"", p_cond.strip())
                    if p_cond_match:
                        p_conditions_dict[p_cond_match.group(1)] = p_cond_match.group(2)
                        p_conditions_set.add(p_cond_match.group(2))
                    else:
                        print("第 {0} 行中的谓词条件 {1} 匹配失败".format(i + 1, p_cond))
                for join_cond in join_conditions:
                    join_cond_match = re.match("(\\S+)\\.(\\w)\\s*=\\s*(\\S+)\\.(\\w)", join_cond.strip())
                    if join_cond_match:
                        overall.append((p_conditions_dict[join_cond_match.group(1)],
                                        p_conditions_dict[join_cond_match.group(3)],
                                        join_type_dict[(join_cond_match.group(2), join_cond_match.group(4))]))
                    else:
                        print("第 {0} 行中的连接条件 {1} 匹配失败".format(i + 1, join_cond))
            frequency = dict()
            for item in overall:
                frequency[item] = frequency.get(item, 0) + 1
            print("frequency:", frequency)
            print("overall:", overall)
            print("p_conditions_set", p_conditions_set)
            file = shelve.open(self.savepath)
            file['p_conditions_set'] = p_conditions_set
            file['join_conditions_tuple_list'] = overall
            file['frequency'] = frequency
            file.close()
        else:
            print("读文件失败")


if __name__ == '__main__':
    pass