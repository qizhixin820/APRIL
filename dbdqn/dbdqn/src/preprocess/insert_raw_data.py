'''
    用于向数据库导入原始数据
'''

import pandas as pd
import pymysql
from constants import *

if __name__ == '__main__':
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd=MYSQL_PWD, port=3319, db='contrast',
                           charset='utf8')  # 连接数据库
    cur = conn.cursor()
    cur.execute(
        "create table if not exists t0 (p varchar(255) not null, s varchar(255) not null, o varchar(255) not null) charset utf8 ")
    conn.commit()
    csv_file_path = "../../data/blog_graph.csv"
    table_name = "t0"
    # 打开csv文件
    data_csv = pd.read_csv(open(csv_file_path, encoding="utf8"), header=None, dtype=str)  # 使用Pandas读取数据文件
    print(data_csv.shape)
    ii = 0  # 用于统计每个文件的数据量
    for i in range(0, data_csv.shape[0]):  # 逐行读取
        row = data_csv.loc[i].values  # 获取第i行数据
        cur.execute("insert into t0 (p, s, o) values (%s, %s, %s)", (str(row[1]), str(row[0]), str(row[2])))
        ii = i + 1
        if ii % 1000 == 0:
            print(ii, data_csv.loc[i].values)
    print(' - 提交数量：', ii, '条')
    conn.commit()
    conn.close()
    cur.close()
