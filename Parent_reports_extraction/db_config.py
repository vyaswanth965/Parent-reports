# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 15:18:23 2019

@author: Vanshika Juneja
"""

import pymysql

username_staging = 'meenakshi.p'
pwd_staging = 'ABKWJZ'
staging_ip = '10.0.6.154'

username_slave = 'vanshika.juneja'
pwd_slave = 'Vansh2231'
slave_ip = '192.168.0.15'

fortythree_ip = '192.168.0.43'
fortythree_user = 'root'
fortythree_pwd = ''

localhost = '127.0.0.1'
localhost_user = 'root'
localhost_pwd = ''
charset_read_sql = 'utf8'

redshift_dbname='sessions'
redshift_host='ei-datascience.ccf2enlidivo.ap-southeast-1.redshift.amazonaws.com'
redshift_port='5439'
redshift_user='meenakshi_p'
redshift_password='m268P}]dsm'

aurora_dbname='teacheradmindb'
aurora_host='teacheradmin-testdb-instance-1.cun3oxmoyrtk.ap-southeast-1.rds.amazonaws.com'
aurora_port='3306'
aurora_user='ei_datascience'
aurora_password='ei{[2020'


mongo_access_uri = ''
es_ip = ''
size_of_results_per_org = 10000


def getConnection(server_ip, db):
    username = ''
    pwd = ''
    if server_ip==fortythree_ip:
        username = fortythree_user
        pwd = fortythree_pwd
    elif server_ip == staging_ip:
        username = username_staging
        pwd = pwd_staging
    elif server_ip == localhost:
        username = localhost_user
        pwd = localhost_pwd
    elif server_ip == slave_ip:
        username = username_slave
        pwd = pwd_slave
    else:
        return 'unknown server. Please configure db_config.py'
    cnxn = pymysql.connect(server_ip, username, pwd, db, charset=charset_read_sql)
    print('CONNECTED:'+ server_ip + ' database:'+db)
    return cnxn
    