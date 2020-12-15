import shelve
import time
from datetime import datetime

import numpy as np
import random

from myindex import Myindex


class QLearning:
    def __init__(self, action_dim, epsilon, episode):
        self.LOGGING = QLogger()

        if episode:
            with shelve.open("../data/qdict/qdict{0}".format(str(episode))) as f:
                self.LOGGING.write("读取第{0}轮的训练结果".format(episode), True)
                self.qfunc = f["q"]
                self.episodes = f["episodes"]
                self.episode = f["episode"]
                self.max_step = f["max_step"]
        else:
            self.qfunc = dict()
            self.episodes = 50  # 训练轮数
            self.episode = 0
            self.max_step = 100  # 每轮训练最大步

        self.actions = range(action_dim)
        self.epsilon = epsilon


    def greedy(self, state):
        # 遍历动作, 得到最大动作值(q)函数qmax及其对应的动作amax
        amax = 0
        key = ''.join([str(i) for i in state]) + '_' + str(self.actions[0])
        if key not in self.qfunc:
            self.qfunc[key] = 0
        qmax = self.qfunc[key]
        for i in range(len(self.actions)):
            key = ''.join([str(i) for i in state]) + '_' + str(self.actions[0])
            if key not in self.qfunc:
                self.qfunc[key] = 0
            q = self.qfunc[key]
            if qmax < q:
                qmax = q
                amax = i
        return self.actions[amax]

    def epsilon_greedy(self, state):
        # 遍历动作, 得到最大动作值(q)函数qmax及其对应的动作amax
        amax = 0
        key = ''.join([str(i) for i in state]) + '_' + str(self.actions[0])
        if key not in self.qfunc:
            self.qfunc[key] = 0
        qmax = self.qfunc[key]
        for i in range(len(self.actions)):
            key = ''.join([str(i) for i in state]) + '_' + str(self.actions[0])
            if key not in self.qfunc:
                self.qfunc[key] = 0
            q = self.qfunc[key]
            if qmax < q:
                qmax = q
                amax = i

        # 设置概率并以epsilon-greedy随机选择动作
        pro = np.zeros(len(self.actions))
        pro[amax] += 1 - self.epsilon
        pro += self.epsilon / len(self.actions)

        r = random.random()
        s = 0.0
        for i in range(len(self.actions)):
            s += pro[i]
            if s >= r:
                return self.actions[i]

        # return actions[len(actions)-1]

    def train(self, env, alpha, gamma):

        for episode in range(self.episode, self.episodes):
            s = env.reset()
            self.LOGGING.write("init_state = " + str(s), True)
            step = 0
            t = False

            cur_return = 0  # 到当前时刻的return
            max_return = -1 * np.inf  # 最大return
            episode_start_time = time.time()
            while t == False and step < self.max_step:
                self.LOGGING.write("episode = {0}, step = {1}".format(episode, step), True)
                if step == 0:
                    a = random.randint(0, len(self.actions) - 1)
                else:
                    a = self.epsilon_greedy(s)
                self.LOGGING.write("action = " + str(a), True)
                s1, r, t = env.step(a)
                self.LOGGING.write("reward = {0}s".format(str(r)), True)
                a1 = self.greedy(s1)
                key = ''.join([str(i) for i in s]) + '_' + str(a)
                key1 = ''.join([str(i) for i in s1]) + '_' + str(a1)
                # Qsa = Qsa + alpha * (reward + γ * max(Qsa') - Qsa)
                if key not in self.qfunc:
                    self.qfunc[key] = 0
                if key1 not in self.qfunc:
                    self.qfunc[key1] = 0
                self.qfunc[key] = self.qfunc[key] + alpha * (r + gamma * self.qfunc[key1] - self.qfunc[key])
                self.LOGGING.write("q function[key0] = {0}".format(str(self.qfunc[key])), True)
                self.LOGGING.write("q function[key1] = {0}".format(str(self.qfunc[key1])), True)
                cur_return += r
                if cur_return > max_return:
                    max_return = cur_return
                self.LOGGING.write("max improve is {0}s".format(str(max_return)), True)

                # 转到下个状态
                s = s1
                step += 1
            episode_end_time = time.time()
            self.LOGGING.write("last epoch max return is {0}s".format(str(max_return)), True)
            self.LOGGING.write("Final state in this episode: " + str(s), True)
            self.LOGGING.write("episode {0} using time {1}s".format(str(episode), str(episode_end_time - episode_start_time)), True)
            if episode % 10 == 0:
                with shelve.open("../data/qdict/qdict{0}".format(str(episode))) as f:
                    f["q"] = self.qfunc
                    f["episodes"] = self.episodes
                    f["episode"] = episode
                    f["max_step"] = self.max_step
                env.save_myindex(episode)

class QLogger:
    """
    mydb日志管理
    """

    def __init__(self, path='../data/qlearning.txt'):
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


if __name__ == "__main__":
    env = Myindex()  # 环境

    action_dim = 3 * len(env.columns)  # 动作空间

    alpha = 1
    gamma = 0.99
    epsilon = 0.05
    episode = None

    agent = QLearning(action_dim, epsilon, episode)

    agent.train(env, alpha, gamma)


    agent.LOGGING.write("\ntest:\n")
    # test
    s = env.reset()
    step = 0
    t = False

    cur_return = 0  # 到当前时刻的return
    max_return = -1 * np.inf  # 最大return
    max_return_step = -1
    episode_start_time = time.time()

    while t == False and step < agent.max_step:
        a = agent.greedy(s)
        agent.LOGGING.write("action = " + str(a), True)


        s1, r, t = env.step(a)
        agent.LOGGING.write("reward = {0}s".format(str(r)), True)


        key = ''.join([str(i) for i in s]) + '_' + str(a)
        agent.LOGGING.write("q function[key0] = {0}".format(str(agent.qfunc[key])), True)

        cur_return += r
        if cur_return > max_return:
            max_return = cur_return
            max_return_step = step
        agent.LOGGING.write("max improve is {0}s".format(str(max_return)), True)
        agent.LOGGING.write("max improve step is {0}".format(str(max_return_step)), True)

        s = s1
        step += 1

    episode_end_time = time.time()
    agent.LOGGING.write("episode running time:"+str(episode_end_time-episode_start_time))

