#!/usr/bin/python3.7
# _*_ coding: utf-8 _*_
import subprocess
import mysql_config as conf
from get_tables import tables_all


def hive_jobs():
    sqoop_tool = 'sqoop job --create'
    table_data = tables_all()
    for database, tables in table_data.items():
        hive_db = database.lower()+"2021"
        for table in tables:
            import_str = 'import --connect %s%s --username %s ' \
                         '--password-file %s --table %s ' \
                         '--hive-import --hive-overwrite ' \
                         '--hive-database %s --mysql-delimiters ' \
                         '-m %s --hive-table %s' % (conf.jdbc_url,
                                                    database,
                                                    conf.mysql_user,
                                                    conf.passfile,
                                                    table, hive_db,
                                                    conf.mappers, table)
            cmd_str = '%s job_%s_%s -- %s' % (sqoop_tool, hive_db,
                                              table, import_str)
            ret, content = subprocess.getstatusoutput(cmd_str)
            if ret != 0:
                print("job_%s_%s create fail!" % (hive_db, table))
                exit(1)
            else:
                exec_job = 'sqoop job --exec job_%s_%s' % (hive_db,
                                                           table)
                ret, content = subprocess.getstatusoutput(exec_job)
                if ret != 0:
                    print("job_%s_%s exec fail!" % (hive_db, table))
                    exit(1)
                else:
                    print("job_%s_%s exec success!" % (hive_db, table))


if __name__ == '__main__':
    hive_jobs()
