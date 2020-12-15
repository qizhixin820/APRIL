import os
import time
import shelve
import random
import numpy as np

from collections import deque
from tensorflow.keras import models, layers, optimizers
from tensorflow.keras.models import load_model

from myindex import Myindex
from logger import MyLogger

PATH = "D:/download/dbdqn/dqn-qindex/"


class DQN:
    def __init__(self, state_dim, action_dim, logger, model_num):
        self.step = 0
        self.state_dim = state_dim
        self.action_dim = action_dim

        self.logger = logger

        self.model = load_model(PATH+'model/model' + str(model_num) + '.h5')
        self.target_model = load_model(PATH+'model/target_model' + str(model_num) + '.h5')

    def greedy(self, s, env):
        predict_vals = self.model.predict(np.array([s]))[0]
        agent.logger.write(['choose action by q_vals: ', predict_vals, '\n'])

        return np.argmax(predict_vals)


if __name__ == '__main__':

    model_number = 990

    max_step_per_episode = 100
    date = 'test-model-' + str(model_number)

    if os.path.exists(PATH + "data/log/lubm.txt"):
        os.remove(PATH + "data/log/lubm.txt")
    #create env and agent
    env = Myindex()  # 环境

    state_dim = len(env.columns)
    action_dim = 3 * len(env.columns)

    agent = DQN(state_dim, action_dim, MyLogger(path=PATH+'data/qindex_test_dqn_log' + date + '.txt'), model_number)

    agent.logger.write(['state_dim = ', state_dim, ', action_dim = ', action_dim, '\n'])

    print('start=-=-=-=-=-=-=')
    print('model_number:', model_number)

    rewards_exp = list()

    episode_start_time = time.time()

    # 每个episode开始时初始化环境, 初始化该episode的信息
    state = env.reset()  # 第0轮需要手动删除所有表, 因为reset无法检测到数据库实际有多少表, 只能跟踪已经插入多少表
    print(state)
    agent.logger.write(['init_state = ', state, '\n'])

    all_exp = list()  # 存放这一轮的所有经验

    cur_return = 0  # 到当前时刻的return
    max_return = -1 * np.inf  # 最大return
    max_return_step = -1  # 最大return所处的step

    step = 0
    t = False
    # 判断循环是否已终止: 到达终止状态, 或step到达最大限度
    while t == False and step < max_step_per_episode:
        print("step:", step)

        # 每个step重复以下几件事: 
        # 1. 动作(agent): agent选择动作
        a = agent.greedy(s=state, env=env)

        print('action:', a)
        agent.logger.write(['action = ', a, '\n'])

        # 2. 环境: env执行动作, 并返回新的状态,奖励,是否终止,额外信息
        #   (仿照gym中的Env类, step函数返回一个四元组: 下一个状态、奖励信息、是否Episode终止，以及一些额外的信息)
        s, r, t = env.step(a)

        print("reward/once improve is (", r, ")ms")
        agent.logger.write(['reward = ', r, '\n'])
        agent.logger.write(['state = ', s, '\n'])

        # 3. 经验(agent): 暂时存储该step的experience于all_exp(与一般dqn的不同: 每轮的最后, 用max_return_step选择进入经验回放池replay buffer的经验experience)
        all_exp.append([state, a, s, r])
        rewards_exp.append(r)

        # 4. 该episode的其他信息: 更新 cur_return; 更新 max_return, max_return_step
        cur_return += r
        if cur_return > max_return:
            max_return = cur_return
            max_return_step = step
        print("max improve is (", max_return, ")ms\n")

        # 5. 时刻: 更新状态, 并进入下一个step
        state = s
        step += 1

    #with open(PATH + "data/log/lubm.txt", "w+", encoding="utf8") as f:
     #   f.write(str(env.get_total_time()))
    with open(PATH + "data/log/lubm.txt", "a", encoding="utf8") as f:
        for name in env.table_names:
            env.cur.execute("show index from {0}".format(name))
            indices_on_tname = env.cur.fetchall()
            for each_index in indices_on_tname:
                f.write("create {0} index on {1}({2})\n".format(each_index[-3], each_index[0], each_index[4]))
        f.write(str(env.get_total_time()))

    agent.logger.write(['max reward in all episodes: ', max(rewards_exp), '\n'])
    agent.logger.write(['max return is ', max_return, 'at step ', max_return_step, '\n'])
    # 一个episode终止后, 用max_return_step选择进入replay buffer的经验, 并训练agent,

    # 记录该episode所用时间
    episode_end_time = time.time()
    agent.logger.write(['test using time: ', episode_end_time - episode_start_time, 's', '\n'])
    # 每4轮保存模型核mydb2的成员变量

    agent.logger.close()
