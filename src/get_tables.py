#!/usr/bin/python3.7
# _*_ coding: utf-8 _*_
import re
import subprocess
import mysql_config as conf
from get_databases import databases_info


# 根据传入的数据库名称，查询该数据库的表的信息
def tables_info(database_name):
    get_list = []
    sqoop_tool = 'sqoop list-tables'
    cmd_str = '%s --connect %s%s --username %s --password-file %s' % (
        sqoop_tool, conf.jdbc_url, database_name,
        conf.mysql_user, conf.passfile)
    ret, content = subprocess.getstatusoutput(cmd_str)
    if ret != 0:
        print("command %s fail!" % cmd_str)
        exit(1)
    line_msg = content.split('\n')
    for item in line_msg:
        if item in conf.exclude_table:
            continue
        # 只收集以msg字符结尾的表名
        match_str = re.match(r'.*msg$', item)
        if match_str:
            get_list.append(match_str.group(0))
    return get_list


def tables_all():
    tables_data = {}
    databases_msg = databases_info()
    # 遍历获取的数据库列表
    for item in databases_msg:
        # 如果数据库名在排除的列表中，则跳过
        if item in conf.exclude_database:
            continue
        else:
            table_msg = tables_info(item)
            tables_data.update({item: table_msg})
    return tables_data
