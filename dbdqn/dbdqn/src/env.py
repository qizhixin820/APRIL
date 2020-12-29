from mydb2 import Mydb
import numpy as np

class ENV:
    """仿照gym中的Env类，定义step，reset函数"""

    def __init__(self, max_tnum, max_len, mydb_dump_number):
        self.max_tnum = max_tnum
        self.max_len = max_len

        # 与数据库连接并初始化数据库表
        self.db = Mydb(mydb_dump_number, 'testlubm')
        self.last_total_time = self.db.plain_search()
        print("t0 time: ", self.last_total_time)
        print("===== init db finished =====")
        
        # 初始化环境状态，并保存初始状态以方便reset函数
        self.init_state = self.embedding(()) # 初始状态init_state 是全0向量
        self.state = self.init_state
        
        # 将单词映射到数字，方便embedding
        self.word_dic, self.p_list = self.get_dic()
        return 
    
    def save_mydb2(self, episode):
        self.db.save_mydb2(episode)

    def get_dic(self):
        """从初始表中得到p的词典，并将每个词映射到一个数
        Input: init_table 是列表的列表，每个元素的格式是p s o
        Return  p_dic: 将p映射到1到...的整数
                p_list: p的列表
        """
        p_list = self.db.get_t0_p_list()
        
        p_dic = dict()
        for p in p_list:
            p_dic[p] = p_list.index(p) + 1
        return p_dic, p_list
    
    def embedding(self, hashtp):
        # 将表格集合嵌入到向量, 并零填充到固定的维度
        vector = list()
        for i in range(1, len(hashtp)):
            for p in hashtp[i]:
                vector.append(self.word_dic[p]) # 只加入p
            vector.append(len(self.p_list)+1) # 表示表的间隔
        # 零填充
        state = np.pad(np.array(vector), (0,self.max_len-len(vector)), 'constant', constant_values=(0,0))
        return state
    
    def parse_action(self, action_id):
        # 解析动作，将action从一个值转换为一个列表，维度为1或2，维度为1时，表示分表，维度为2时，表示合表
        p_num = len(self.p_list)
        if action_id < p_num:
            return [self.p_list[action_id]]
        else:
            action_id -= p_num
            tnum2 = 0
            choice = 0
            for tnum1 in range(2, self.max_tnum + 1):   
                if action_id - (tnum1 - 1)*16 < 0:
                    tnum2 = action_id // 16 + 1
                    choice = action_id % 16 + 1
                    break
                else:
                    action_id -= (tnum1 - 1)*16
                
            '''
            p_num = 3
            max_tnum = 4
                0 1 2 :p
                21[3] 21[4] 21[5]
                31[6] 31[7] 31[8] 32[9] 32[10] 32[11]
                41[12] 41[13] 41[14] 42[15] 42[16] 42[17] 43[18] 43[19] 43[20]
            '''
            return [tnum1, tnum2, choice-1]

    def step(self, action_id):
        """ Parse action_id, and do the action in database(call self.db).
        Args: an integer of action id
        Return: a tuple of (next_state, reward, if final，parsed action)
        """
        action = self.parse_action(action_id)
        if len(action) == 3:
            self.db.merge(action[0], action[1], action[2])
        elif len(action) == 1:
            self.db.divide(action[0])

        # final 表示是否到达终止状态s
        final = self.check_final(self.db.hashtp)
        if final == True:
            return np.zeros(self.max_len), 0, True, action

        # 运行负载计算效率
        total_time = self.db.get_total_time() # total_time 单位s
        reward = (self.last_total_time - total_time) * 1000 # reward 单位ms
        self.last_total_time = total_time

        # 计算新的状态
        self.state = self.embedding(self.db.hashtp)

        return self.state, reward, final, action

    def reset(self):
        """init self.state, init database(only have t0), init self.db 
        Return: init state of env
        """
        self.state = self.init_state    
        self.db.tearDown()              
        return self.state

    def check_final(self, hashtp):
        """ Check if env reaches a final state.
        Final state is a state when total table numbers > max_tnum(set by dqn main method)
                                 or total_p_num > max length of state(set by dqn main method)
        """

        t_num = len(hashtp)
        total_p_num = 0
        for value in hashtp.values():
            total_p_num += len(value)
        
        if t_num <= self.max_tnum and total_p_num <= self.max_len:
            return False
        return True
    
    def judge_legal(self):
        return self.db.judge_legal(self.max_tnum, self.p_list)

    def get_dqn_params(self):
        """Return input_dim of DQN, output_dim of DQN, and p_list of t0.
        数据库中最多有 (t0,) t1, ..., t_max_tnum, 即最多共max_tnum+1个表
        合表的表对最多有 max_tum-1 + ... + 1 = self.max_tnum * (self.max_tnum-1) // 2
        每个合表对可以有最多16种合法(2个表均有2个属性时2*2*4=16), 实际上共60种情况
        """
        return self.max_len, len(self.p_list) + self.max_tnum * (self.max_tnum-1) // 2 * 16, len(self.p_list)