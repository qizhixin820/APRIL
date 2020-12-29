import time
import random
import numpy as np
from collections import deque
from tensorflow.keras import models, layers, optimizers
from tensorflow.keras.models import load_model
from env import ENV
from logger import MyLogger


class DQN:
    def __init__(self, state_dim, action_dim, logger, model_num=None):
        self.step = 0
        self.update_freq = 50
        self.replay_size = 200
        self.replay_queue = deque(maxlen=self.replay_size)
        self.state_dim = state_dim
        self.action_dim = action_dim

        self.logger = logger

        if model_num is None:
            self.model = self.create_model()
            self.target_model = self.create_model()
        else:
            self.model = load_model('../model/model' + str(model_num) + '.h5')
            self.target_model = load_model('../model/target_model' + str(model_num) + '.h5')

    def create_model(self):
        STATE_DIM, ACTION_DIM = self.state_dim, self.action_dim
        model = models.Sequential([
            layers.Dense(50, input_dim=STATE_DIM, activation='relu'),
            layers.Dense(ACTION_DIM, activation='linear')
        ])
        model.compile(loss='mean_squared_error',
                      optimizer=optimizers.Adam(0.001))
        return model

    def remember(self, s, a, next_s, r):
        self.replay_queue.append((s, a, next_s, r))
        return

    def epsilon_greedy(self, s, env, epsilon=0.2):

        if np.random.uniform() < epsilon:  # - self.step * 0.0002; delete步数越大时随机性越少
            agent.logger.write(['randomly choose action\n'])
            predict_vals = np.arange(self.action_dim)

            # prob的元素取值均为0或1, 为random.choice用到的概率, 非法动作的概率设为0
            prob = np.ones(self.action_dim)
            abandon_list = env.judge_legal()
            for i in abandon_list:
                prob[i] = 0
            prob /= np.sum(prob)
            return np.random.choice(predict_vals, p=prob)

        # greedy时, 将非法动作的q值设为-inf, 硬约束非法动作
        predict_vals = self.model.predict(np.array([s]))[0]
        agent.logger.write(['choose action by q_vals: ', predict_vals, '\n'])
        abandon_list = env.judge_legal()
        for i in abandon_list:
            predict_vals[i] = -1 * np.inf
        return np.argmax(predict_vals)

    def greedy(self, s, env):
        predict_vals = self.model.predict(np.array([s]))[0]
        agent.logger.write(['choose action by q_vals: ', predict_vals, '\n'])

        abandon_list = env.judge_legal()
        for i in abandon_list:
            predict_vals[i] = -1 * np.inf

        return np.argmax(predict_vals)

    def train(self, batch_size=32, gamma=0.95):
        if len(self.replay_queue) < self.replay_size:
            return
        self.step += 1
        if self.step % self.update_freq == 0:
            self.target_model.set_weights(self.model.get_weights())

        replay_batch = random.sample(self.replay_queue, batch_size)
        s_batch = np.array([replay[0] for replay in replay_batch])
        next_s_batch = np.array([replay[2] for replay in replay_batch])

        Q = self.model.predict(s_batch)
        Q_next = self.target_model.predict(next_s_batch)

        for i, replay in enumerate(replay_batch):
            _, a, _, reward = replay
            Q[i][a] = reward + gamma * np.amax(Q_next[i])

        self.model.fit(s_batch, Q)

    def save_models(self, episode):
        self.model.save('../model/model' + str(episode) + '.h5')
        self.target_model.save('../model/target_model' + str(episode) + '.h5')


if __name__ == '__main__':

    model_number = None  # the file number of model.h5 to load by agent
    mydb_dump_number = None  # the file number of mydb2_dump to load empty_action_list by mydb2
    date = '8-21'  # the date running this python file
    max_tnum = 25  # max number of tables in database
    max_len = 100  # length of state, max number of p database
    episodes = 500
    max_step_per_episode = 50

    # create env and agent
    env = ENV(max_tnum, max_len, mydb_dump_number)
    state_dim, action_dim, p_num = env.get_dqn_params()
    agent = DQN(state_dim, action_dim, MyLogger(path='../data/train_dqn_log' + date + '.txt'), model_number)

    print('state_dim, action_dim, p_num:', state_dim, action_dim, p_num)
    agent.logger.write(['state_dim = ', state_dim, ', action_dim = ', action_dim, '\n'])

    print('start=-=-=-=-=-=-=')

    for episode in range(episodes):
        print("episode:", episode)
        episode_start_time = time.time()

        # 每个episode开始时初始化环境, 初始化该episode的信息
        state = env.reset()  # 第0轮需要手动删除所有表, 因为reset无法检测到数据库实际有多少表, 只能跟踪已经插入多少表
        agent.logger.write(['init_state = ', state, '\n'])

        all_exp = list()  # 存放这一轮的所有经验

        cur_return = 0  # 到当前时刻的return
        max_return = -1 * np.inf  # 最大return
        max_return_step = -1  # 最大return所处的step

        step = 0
        t = False
        # 判断循环是否已终止: 到达终止状态, 或step到达最大限度
        while t == False and step < max_step_per_episode:
            env.save_mydb2(episode)
            print("step:", step)
            agent.logger.write(['episode = ', episode, ',step = ', step, '\n'])

            # 每个step重复以下几件事: 
            # 1. 动作(agent): agent选择动作
            if episode == episodes - 1:
                a = agent.greedy(s=state, env=env)
            else:
                a = agent.epsilon_greedy(s=state, env=env)

            agent.logger.write(['action = ', a, '\n'])

            # 2. 环境: env执行动作, 并返回新的状态,奖励,是否终止,额外信息
            #   (仿照gym中的Env类, step函数返回一个四元组: 下一个状态、奖励信息、是否Episode终止，以及一些额外的信息)
            s, r, t, parsed_action = env.step(a)

            print("reward/once improve is (", r, ")ms")
            agent.logger.write(['parsed_action = ', parsed_action, '\n'])
            agent.logger.write(['reward = ', r, '\n'])
            agent.logger.write(['state = ', s, '\n'])

            # 3. 经验(agent): 暂时存储该step的experience于all_exp(与一般dqn的不同: 每轮的最后, 用max_return_step选择进入经验回放池replay buffer的经验experience)
            all_exp.append([state, a, s, r])

            # 4. 该episode的其他信息: 更新 cur_return; 更新 max_return, max_return_step
            cur_return += r
            if cur_return > max_return:
                max_return = cur_return
                max_return_step = step
            print("max improve is (", max_return, ")ms\n")

            # 5. 时刻: 更新状态, 并进入下一个step
            state = s
            step += 1

        agent.logger.write(['last epoch max return is ', max_return, 'at step ', max_return_step, '\n'])
        # 一个episode终止后, 用max_return_step选择进入replay buffer的经验, 并训练agent,
        for step_, exp in enumerate(all_exp):
            if step_ > max_return_step:
                break
            agent.remember(*exp)
            agent.train()
        # 记录该episode所用时间
        episode_end_time = time.time()
        agent.logger.write(['episode ', episode, 'using time: ', episode_end_time - episode_start_time, 's', '\n'])
        # 每4轮保存模型核mydb2的成员变量
        if episode % 10 == 0:
            agent.save_models(episode)
        env.save_mydb2(episode)

    agent.logger.close()
