#!/bin/env python
# -*- coding:utf-8 -*-

import os,sys,re,getopt
import MySQLdb


host = '127.0.0.1'
user = ''
password = ''
port = 3306
start_datetime = '1971-01-01 00:00:00'
stop_datetime = '2037-01-01 00:00:00'
start_position = '4'
stop_position = '18446744073709551615'
database = ''
mysqlbinlog_bin = 'mysqlbinlog -v'
binlog = ''
fileContent = ''
output='rollback.sql'
only_primary = 0


# ----------------------------------------------------------------------------------------
# 功能:获取参数,生成相应的binlog解析文件
# ----------------------------------------------------------------------------------------
def getopts_parse_binlog():
    global host
    global user
    global password
    global port
    global fileContent
    global output
    global binlog
    global start_datetime
    global stop_datetime
    global start_position
    global stop_position
    global database
    global only_primary
    try:
        options, args = getopt.getopt(sys.argv[1:], "f:o:h:u:p:P:d:", ["help","binlog=","output=","host=","user=","password=","port=","start-datetime=", \
                                                                      "stop-datetime=","start-position=","stop-position=","database=","only-primary="])
    except getopt.GetoptError:
        print "参数输入有误!!!!!"
        options = []
    if options == [] or options[0][0] in ("--help"):
        usage()
        sys.exit()
    print "正在获取参数....."
    for name, value in options:
        if name == "-f" or name == "--binlog":
            binlog = value
        if name == "-o" or name == "--output":
            output = value
        if name == "-h" or name == "--host":
            host = value
        if name == "-u" or name == "--user":
            user = value
        if name == "-p" or name == "--password":
            password = value
        if name == "-P" or name == "--port":
            port = value
        if name == "--start-datetime":
            start_datetime = value
        if name == "--stop-datetime":
            stop_datetime = value
        if name == "--start-position":
            start_position = value
        if name == "--stop-position":
            stop_position = value
        if name == "-d" or name == "--database":
            database = value
        if name == "--only-primary" :
            only_primary = value

    if binlog == '' :
        print "错误:请指定binlog文件名!"
        usage()
    if user == '' :
        print "错误:请指定用户名!"
        usage()
    if password == '' :
        print "错误:请指定密码!"
        usage()
    if database <> '' :
       condition_database = "--database=" + "'" + database + "'"
    else:
        condition_database = ''
    print "正在解析binlog....."
    fileContent=os.popen("%s %s  --base64-output=DECODE-ROWS --start-datetime='%s' --stop-datetime='%s' --start-position='%s' --stop-position='%s' %s\
                   |grep '###' -B 2|sed -e 's/### //g' -e 's/^INSERT/##INSERT/g' -e 's/^UPDATE/##UPDATE/g' -e 's/^DELETE/##DELETE/g' " \
                   %(mysqlbinlog_bin,binlog,start_datetime,stop_datetime,start_position,stop_position,condition_database)).read()
    #print fileContent



# ----------------------------------------------------------------------------------------
# 功能:初始化binlog里的所有表名和列名,用全局字典result_dict来储存每个表有哪些列
# ----------------------------------------------------------------------------------------
def init_col_name():
    global result_dict
    global pri_dict
    global fileContent
    result_dict = {}
    pri_dict = {}
    table_list = re.findall('`.*`\\.`.*`',fileContent)
    table_list = list(set(table_list))
    #table_list 为所有在这段binlog里出现过的表
    print "正在初始化列名....."
    for table in table_list:
        sname = table.split('.')[0].replace('`','')
        tname = table.split('.')[1].replace('`','')
        #连接数据库获取列和列id
        try:
            conn = MySQLdb.connect(host=host,user=user,passwd=password,port=int(port))
            cursor = conn.cursor()
            cursor.execute("select ordinal_position,column_name \
                                                       from information_schema.columns \
                                                       where table_schema='%s' and table_name='%s' " %(sname,tname))

            result=cursor.fetchall()
            if result == () :
                print 'Warning:'+sname+'.'+tname+'已删除'
                #sys.exit()
            result_dict[sname+'.'+tname]=result
            cursor.execute("select ordinal_position,column_name   \
                               from information_schema.columns \
                               where table_schema='%s' and table_name='%s' and column_key='PRI' " %(sname,tname))
            pri=cursor.fetchall()
            #print pri
            pri_dict[sname+'.'+tname]=pri
            cursor.close()
            conn.close()
        except MySQLdb.Error, e:
            try:
                print "Error %d:%s" % (e.args[0], e.args[1])
            except IndexError:
                print "MySQL Error:%s" % str(e)

            sys.exit()
    #print result_dict
    #print pri_dict

# ----------------------------------------------------------------------------------------
# 功能:拼凑回滚sql,逆序
# ----------------------------------------------------------------------------------------
def gen_rollback_sql():
    global only_primary
    fileOutput = open(output, 'w')
    #先将文件根据'--'分块,每块代表一个sql
    area_list=fileContent.split('--\n')
    #逆序读取分块
    print "正在开始拼凑sql....."
    for area in area_list[::-1]:
        #由于一条sql可能影响多行,每个sql又可以分成多个逐条执行的sql
        sql_list = area.split('##')
        #先将pos点和timestamp传入输出文件中
        for sql_head in sql_list[0].splitlines():
            sql_head = '#'+sql_head+'\n'
            fileOutput.write(sql_head)
        #逐条sql进行替换更新,逆序
        for sql in sql_list[::-1][0:-1]:
            try:
                if sql.split()[0] == 'INSERT':
                    rollback_sql = re.sub('^INSERT INTO', 'DELETE FROM', sql, 1)
                    rollback_sql = re.sub('SET\n', 'WHERE\n', rollback_sql, 1)
                    tablename_pos = 2
                    table_name = rollback_sql.split()[tablename_pos].replace('`', '')
                    # 获取该sql中的所有列
                    col_list = sorted(list(set(re.findall('@\d+', rollback_sql))))
                    # 因为第一个列前面没有逗号或者and,所以单独替换
                    rollback_sql = rollback_sql.replace('@1=', result_dict[table_name][0][1]+'=')
                    for col in col_list[1:]:
                        i = int(col[1:]) - 1
                        rollback_sql = rollback_sql.replace(col+'=', 'AND ' + result_dict[table_name][i][1]+'=',1)
                    # 如果only_primary开启且存在主键,where条件里就只列出主键字段
                    if int(only_primary) == 1 and pri_dict[table_name] <> ():
                        sub_where = ''
                        for primary in pri_dict[table_name]:
                            primary_name = primary[1]
                            for condition in rollback_sql.split('WHERE', 1)[1].splitlines():
                                if re.compile('^\s*'+primary_name).match(condition) or re.compile('^\s*AND\s*'+primary_name).match(condition):
                                    sub_where = sub_where + condition + '\n'
                        sub_where = re.sub('^\s*AND', '', sub_where, 1)
                        rollback_sql = rollback_sql.split('WHERE', 1)[0] + 'WHERE\n' + sub_where
                if sql.split()[0] == 'UPDATE':
                    rollback_sql = re.sub('SET\n', '#SET#\n', sql, 1)
                    rollback_sql = re.sub('WHERE\n', 'SET\n', rollback_sql, 1)
                    rollback_sql = re.sub('#SET#\n', 'WHERE\n', rollback_sql, 1)
                    tablename_pos = 1
                    table_name = rollback_sql.split()[tablename_pos].replace('`', '')
                    # 获取该sql中的所有列
                    col_list = sorted(list(set(re.findall('@\d+', rollback_sql))))
                    # 因为第一个列前面没有逗号或者and,所以单独替换
                    rollback_sql = rollback_sql.replace('@1=', result_dict[table_name][0][1] + '=')
                    for col in col_list[1:]:
                        i = int(col[1:]) - 1
                        rollback_sql = rollback_sql.replace(col+'=', ',' + result_dict[table_name][i][1]+'=', 1).replace(col+'=','AND ' +result_dict[table_name][i][1]+'=')
                    # 如果only_primary开启且存在主键,where条件里就只列出主键字段
                    if int(only_primary) == 1 and pri_dict[table_name] <> ():
                        sub_where = ''
                        for primary in pri_dict[table_name]:
                            primary_name = primary[1]
                            for condition in rollback_sql.split('WHERE', 1)[1].splitlines():
                                if re.compile('^\s*' + primary_name).match(condition) or re.compile('^\s*AND\s*'+primary_name).match(condition):
                                    sub_where = sub_where + condition + '\n'
                        sub_where = re.sub('^\s*AND', '', sub_where, 1)
                        rollback_sql = rollback_sql.split('WHERE', 1)[0] + 'WHERE\n' + sub_where

                if sql.split()[0] == 'DELETE':
                    rollback_sql = re.sub('^DELETE FROM', 'INSERT INTO', sql, 1)
                    rollback_sql = re.sub('WHERE\n', 'SET\n', rollback_sql, 1)
                    tablename_pos = 2
                    table_name = rollback_sql.split()[tablename_pos].replace('`', '')
                    # 获取该sql中的所有列
                    col_list = sorted(list(set(re.findall('@\d+', rollback_sql))))
                    # 因为第一个列前面没有逗号或者and,所以单独替换
                    rollback_sql = rollback_sql.replace('@1=', result_dict[table_name][0][1] + '=')
                    for col in col_list[1:]:
                        i = int(col[1:]) - 1
                        rollback_sql = rollback_sql.replace(col+'=', ',' + result_dict[table_name][i][1]+'=',1)

                rollback_sql = re.sub('\n$',';\n',rollback_sql)
                #print rollback_sql
                fileOutput.write(rollback_sql)
            except IndexError,e:
                print "Error:%s" % str(e)
                sys.exit()
    print "done!"

def usage():
    help_info="""==========================================================================================
Command line options :
    --help                  # OUT : print help info
    -f, --binlog            # IN  : binlog file. (required)
    -o, --outfile           # OUT : output rollback sql file. (default 'rollback.sql')
    -h, --host              # IN  : host. (default '127.0.0.1')
    -u, --user              # IN  : user. (required)
    -p, --password          # IN  : password. (required)
    -P, --port              # IN  : port. (default 3306)
    --start-datetime        # IN  : start datetime. (default '1970-01-01 00:00:00')
    --stop-datetime         # IN  : stop datetime. default '2070-01-01 00:00:00'
    --start-position        # IN  : start position. (default '4')
    --stop-position         # IN  : stop position. (default '18446744073709551615')
    -d, --database          # IN  : List entries for just this database (No default value).
    --only-primary          # IN  : Only list primary key in where condition (default 0)

Sample :
   shell> python binlog_rollback.py -f 'mysql-bin.000001' -o '/tmp/rollback.sql' -h 192.168.0.1 -u 'user' -p 'pwd' -P 3307 -d dbname
=========================================================================================="""

    print help_info
    sys.exit()



if __name__ == '__main__':
    getopts_parse_binlog()
    init_col_name()
    gen_rollback_sql()
