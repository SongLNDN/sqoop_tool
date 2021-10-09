#!/usr/bin/python3.7
# _*_ coding: utf-8 _*_
import re
import subprocess
import mysql_config as conf


# 通过sqoop命令查询配置文件中mysql节点的数据库数量
def databases_info():
    get_list = []
    sqoop_tool = 'sqoop list-databases'
    cmd_str = '%s --connect %s --username %s --password-file %s' % (
        sqoop_tool, conf.jdbc_url, conf.mysql_user, conf.passfile)
    ret, content = subprocess.getstatusoutput(cmd_str)
    if ret != 0:
        print("command %s fail!" % cmd_str)
        exit(1)
    line_msg = content.split('\n')
    for item in line_msg:
        # 只收集block开头的数据库列表
        match_str = re.match(r'^block.*', item)
        if match_str:
            get_list.append(match_str.group(0))
    return get_list
