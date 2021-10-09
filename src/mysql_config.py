#!/usr/bin/python3.7
# _*_ coding: utf-8 _*_

jdbc_url = 'jdbc:mysql://172.26.231.16:3306/'
mysql_user = 'hdfs'
# sqoop的密码认证文件路径
passfile = '/data/hive/import.password'
# mapreduce task的并发数目
mappers = 1
# 定义不导入的mysql数据库列表
exclude_database = ['blockAll', 'block', 'blockHeight', 'blockf02528',
                    'blockf06']
# 定义不导入的mysql数据库表的名单
exclude_table = []
