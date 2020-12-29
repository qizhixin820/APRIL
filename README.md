0. 准备工作
(1)	安装指定的npm, Python, Java, MySQL环境
(2)	在MySQL中建立三个数据库：testlubm, priority, contrast；调整MySQL的配置文件(my.ini或my.cnf)设置tmp_table_size及max_heap_table_size为较大值（>=1024MB）
(3)	配置Python==3.8 Conda虚拟环境，安装TensorFlow及numpy==1.19.3
(4)	更改front_end项目下的view_content_settings.vue指定Python文件的输出路径；更改AprilBackend项目下的Constants.java指定Python调用路径、脚本路径及输出路径，指定Java工程的Resource路径，指定MySQL口令；更改dbdqn及dqn-qindex项目下的constants.py，指定Python项目路径及MySQL口令。

1. 运行前端
front_end下 Git Bash: npm run serve
2. 运行后台
将AprilBackend导入SpringToolSuite4, 点击Start
3. 导入Graph Data
university1_2.nt文件 (文件放在D:\Download\dbdqn\dbdqn\data)
4. 导入workload
sql_queries文件 (文件放在D:\Download\dbdqn\dbdqn\data)
5. 根据系统引导使用storage selection, index selection, query optimization功能