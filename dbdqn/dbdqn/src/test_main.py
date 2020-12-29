import os
import time
import random
import numpy as np
from collections import deque
from tensorflow.keras import models, layers, optimizers
from tensorflow.keras.models import load_model
from env import ENV
from logger import MyLogger

from constants import *
import sys


class DQN:
    def __init__(self, state_dim, action_dim, logger, model_num):
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.logger = logger

        self.model = load_model(PATH + 'model/model' + str(model_num) + '.h5')
        self.target_model = load_model(PATH + 'model/target_model' + str(model_num) + '.h5')

    def greedy(self, s, env):
        predict_vals = self.model.predict(np.array([s]))[0]
        agent.logger.write(['choose action by q_vals: ', predict_vals, '\n'])

        abandon_list = env.judge_legal()
        for i in abandon_list:
            predict_vals[i] = -1 * np.inf

        return np.argmax(predict_vals)


'''
drop table t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15,t16,t17,t18,t19,t20;
'''
if __name__ == '__main__':

    model_number = 420  # the file number of model.h5 to load by agent
    mydb_dump_number = None  # the file number of mydb2_dump to load empty_action_list by mydb2
    date = '8-21'
    max_tnum = 25  # max number of tables in database
    max_len = 100  # length of state, max number of p database
    max_step_per_episode = 50

    if os.path.exists(PATH + "data/log/lubm.txt"):
        sys.exit(0)
        os.remove(PATH + "data/log/lubm.txt")

    # create env and agent
    env = ENV(max_tnum, max_len, mydb_dump_number)
    state_dim, action_dim, p_num = env.get_dqn_params()
    agent = DQN(state_dim, action_dim, MyLogger(path=PATH + 'data/test_dqn_log' + date + '.txt'), model_number)

    print('state_dim, action_dim, p_num:', state_dim, action_dim, p_num)
    agent.logger.write(['state_dim = ', state_dim, ', action_dim = ', action_dim, '\n'])

    print('start=-=-=-=-=-=-=')
    episode_start_time = time.time()

    # 初始化环境, 初始化该episode的信息
    state = env.reset()  # 第0轮需要手动删除所有表, 因为reset无法检测到数据库实际有多少表, 只能跟踪已经插入多少表
    agent.logger.write(['init_state = ', state, '\n'])

    cur_return = 0  # 到当前时刻的return
    max_return = -1 * np.inf  # 最大return
    max_return_step = -1  # 最大return所处的step

    step = 0
    t = False
    # 判断循环是否已终止: 到达终止状态, 或step到达最大限度
    while t == False and step < max_step_per_episode:
        print("step:", step)
        agent.logger.write(['step = ', step, '\n'])

        # 每个step重复以下几件事: 
        # 1. 动作(agent): agent选择动作
        a = agent.greedy(s=state, env=env)

        agent.logger.write(['action = ', a, '\n'])

        # 2. 环境: env执行动作, 并返回新的状态,奖励,是否终止,额外信息
        #   (仿照gym中的Env类, step函数返回一个四元组: 下一个状态、奖励信息、是否Episode终止，以及一些额外的信息)
        s, r, t, parsed_action = env.step(a)

        print("reward/once improve is (", r, ")ms")
        agent.logger.write(['parsed_action = ', parsed_action, '\n'])
        agent.logger.write(['reward = ', r, '\n'])
        agent.logger.write(['state = ', s, '\n'])

        # 3. 经验(agent): 暂时存储该step的experience于all_exp(与一般dqn的不同: 每轮的最后, 用max_return_step选择进入经验回放池replay buffer的经验experience)
        # all_exp.append([state, a, s, r])

        # 4. 该episode的其他信息: 更新 cur_return; 更新 max_return, max_return_step
        cur_return += r
        if cur_return > max_return:
            max_return = cur_return
            max_return_step = step
        print("max improve is (", max_return, ")ms\n")

        # 5. 时刻: 更新状态, 并进入下一个step
        state = s
        step += 1
    with open(PATH + "data/log/lubm.txt", "a+", encoding="utf8") as f:
        f.write(str(env.db.get_total_time()))

    agent.logger.write(['last epoch max return is ', max_return, 'at step ', max_return_step, '\n'])

    # 记录该episode所用时间
    episode_end_time = time.time()
    agent.logger.write(['using time: ', episode_end_time - episode_start_time, 's', '\n'])
    print('using time: {0} s'.format(episode_end_time - episode_start_time))
    agent.logger.close()
    env.save_mydb2(model_number)
