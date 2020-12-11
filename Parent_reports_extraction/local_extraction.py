# -*- coding: utf-8 -*-
"""
Created on Tue Jul  7 10:08:56 2020

@author: EI-LAP-7240
"""
import pandas as pd
import numpy as np
import datetime
from jinja2 import Environment, FileSystemLoader
import pdfkit
import matplotlib.pyplot as plt
from  matplotlib import image
import boto3
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError
import psycopg2
import time
from multiprocessing import Pool
import multiprocessing
from functools import partial
import os
from joblib import Parallel, delayed
from functools import reduce
from matplotlib.transforms import Bbox
import sys
from glob import glob
import pymysql

cwd="/home/yashwanth.kumar/Parent_reports/"
os.chdir(cwd)
import db_config


def livecon_r():
    global redshift
    global mysql                
    redshift = psycopg2.connect(dbname=db_config.redshift_dbname, host=db_config.redshift_host, port=db_config.redshift_port, user=db_config.redshift_user, password=db_config.redshift_password)
    mysql = pymysql.connect(host=db_config.staging_ip, user=db_config.username_staging, password=db_config.pwd_staging, db="educatio_adepts")

def get_topics_name():
    query = '''select contentid,contentname from msm_topic_flow    '''        
    livecon_r()
    #df = pd.read_csv('topic_names.csv')
    logging.info(query)

    df = pd.read_sql_query(query.format(), redshift)
    return df

def get_b2b2c(week_st_date,week_en_date):
    query = '''SELECT distinct concat(CAST(A.MS_userID AS char(100)) ,'_',cast(A.class AS char(100)) ) as MS_userID
    FROM educatio_educat.common_user_details A, mapping.user_management_mapping B,
    educatio_adepts.adepts_registrationDetailsLog C WHERE A.id=B.oldID AND A.userName=C.username
    AND enddate>'{}' AND category='STUDENT' AND subCategory='School'
    AND A.MS_userID not in (SELECT MS_userID from educatio_educat.common_user_details where startDate > '{}')
    AND (A.childEmail is not null or A.additionalEmail is not null or A.contactno_cel is not null)'''.format(week_st_date,week_en_date)  
    logging.info(query)
    livecon_r()
    df = pd.read_sql(query, mysql)
    return df 



def get_b2c(week_st_date,week_en_date):
    query = '''SELECT distinct concat(CAST(MS_userID AS char(100)) ,'_',cast(class AS char(100)) ) as MS_userID
    FROM educatio_educat.common_user_details
    WHERE enddate>='{}'
    AND category='STUDENT'
    AND subCategory='Individual'
    AND MS_userID not in (SELECT MS_userID from educatio_educat.common_user_details where startDate > '{}')
    AND (childEmail is not null or additionalEmail is not null or contactno_cel is not null)'''.format(week_st_date,week_en_date)  
    logging.info(query)
    livecon_r()
    df = pd.read_sql(query, mysql)
    return df     

def get_b2b_b2b2c(week_st_date,week_en_date):
    query = '''SELECT distinct concat(CAST(MS_userID AS char(100)) ,'_',cast(class AS char(100)) ) as MS_userID
    FROM educatio_educat.common_user_details
    WHERE enddate>='{}'
    AND category='STUDENT'
    AND subCategory='School'
    AND MS_userID not in (SELECT MS_userID from educatio_educat.common_user_details where startDate > '{}')
    AND (childEmail is not null or additionalEmail is not null or contactno_cel is not null)'''.format(week_st_date,week_en_date)  
    logging.info(query)
    livecon_r()
    df = pd.read_sql(query, mysql)
    return df 
    
def get_usage_data(mon,mon_first_day_str,mon_last_day_str):
    query = '''select oldupid as upid,childname,oldschoolcode,class,gender,monthly_correct_ques,monthly_login_days,monthly_total_ques,monthly_accuracy,monthly_usage_time from msm_usage_monthly where calendar_month ='{}' 
    and upid in (select upid from userdetails_dim where category='STUDENT'and enabled in (1,0) and enddate>= '{}' and (parentemail is not null or contactno_cel is not null or childemail is not null)
    and upid not in (select upid from userdetails_dim where category='STUDENT' and startdate > '{}') ) and  upid=1066686
    '''.format(mon,mon_first_day_str,mon_last_day_str)
    livecon_r()
    logging.info(query)
    df = pd.read_sql(query, redshift)
    return df 

def get_topic_data(mon,mon_first_day_str,mon_last_day_str):
    query = '''select oldupid as upid,class,contentid,contenttype,monthly_ques_attempts,progress_of_first_attempt,monthly_accuracy from msm_moduleprogress_monthly where calendar_month ={}  
    and upid in (select upid from userdetails_dim where category='STUDENT'and enabled in (1,0) and enddate>= '{}' and (parentemail is not null or contactno_cel is not null or childemail is not null)
    and upid not in (select upid from userdetails_dim where category='STUDENT' and startdate > '{}') ) and  upid=1066686
    '''.format(mon,mon_first_day_str,mon_last_day_str)
    livecon_r()
    logging.info(query)
    df = pd.read_sql_query(query, redshift)
    return df 

def get_concepts_data(mon_first_day_str,mon_last_day_str):
    query = '''select old_upid as upid,class,contentid,contentattemptnumber,mapped_ttid,accuracy from msm_moduleprogress_attemptid where startdate>='{}' and contenttype='concept' and startdate <='{}'  
    and upid in (select upid from userdetails_dim where category='STUDENT'and enabled in (1,0) and startdate>= '{}' and (parentemail is not null or contactno_cel is not null or childemail is not null)
    and upid not in (select upid from userdetails_dim where category='STUDENT' and startdate > '{}') ) and  upid=1066686
    '''.format(mon_first_day_str,mon_last_day_str,mon_first_day_str,mon_last_day_str)
    livecon_r()
    logging.info(query)
    df = pd.read_sql_query(query, redshift)
    return df

def get_topic_names(contentids):
    livecon_r()
    query=f"""select  contentid,contentname from msm_topic_flow where contentid in {contentids}"""
    topic_name_1 = pd.read_sql(query,redshift)
    livecon_r()
    query=f"""select  oldcontentid contentid,contentname from msm_topic_flow where oldcontentid in {contentids}"""
    topic_name_2 = pd.read_sql(query,redshift)
    topic_name = pd.concat([topic_name_1, topic_name_2])
    topic_name = topic_name.drop_duplicates() 
    return topic_name
        
def split_and_save(no_of_instances):
    b2b2c_users = get_b2b2c(mon_first_day_str,mon_last_day_str)
    b2b2c_users.MS_userID = b2b2c_users.MS_userID.astype(str)
    b2b2c_users = set(b2b2c_users['MS_userID'])
    b2c_users = get_b2c(mon_first_day_str,mon_last_day_str)
    b2c_users.MS_userID = b2c_users.MS_userID.astype(str)
    b2c_users = set(b2c_users['MS_userID'])
    b2b_b2b2c_users = get_b2b_b2b2c(mon_first_day_str,mon_last_day_str)
    b2b_b2b2c_users.MS_userID = b2b_b2b2c_users.MS_userID.astype(str)                                                                     
    b2b_b2b2c_users = set(b2b_b2b2c_users['MS_userID'])
    b2b_users = b2b_b2b2c_users - b2b2c_users
    
    b2b_users = list(b2b_users)
    b2b2c_users = list(b2b2c_users)
    b2c_users = list(b2c_users)
    
    usage = get_usage_data(mon,mon_first_day_str,mon_last_day_str)
    topic_prog = get_topic_data(mon,mon_first_day_str,mon_last_day_str)
    usage['upid_class'] = usage['upid'].apply(str) + '_' + usage['class'].apply(str)
    usage['monthly_usage_time']=usage['monthly_usage_time']//60
    grd_usg = usage.groupby(['class']).agg({'monthly_total_ques':'sum','upid':'nunique'}).reset_index()
    grd_usg['cls_avg'] = grd_usg['monthly_total_ques']//grd_usg['upid']
    grd_usg.to_csv('grd_usg.csv',index=False)
    
    topic_prog['upid_class'] = topic_prog['upid'].apply(str) + '_' + topic_prog['class'].apply(str)
    #topic_names=get_topics_name()
    #topic_prog = topic_prog.merge(topic_names,on='contentid',how='left')
    topic_prog['contentid'] = topic_prog['contentid'].map(lambda x: x.lstrip('p'))
    contentids = topic_prog.contentid.to_list()
    contentids = tuple(np.unique(contentids)) 
    topic_name = get_topic_names(contentids)    
    topic_prog = topic_prog.merge(topic_name[['contentid','contentname']],on='contentid',how='left') 
       

    c_df=get_concepts_data(mon_first_day_str,mon_last_day_str)
    c_df['upid_class'] = c_df['upid'].apply(str) + '_' + c_df['class'].apply(str)
    c_df['mapped_ttid'] = c_df['mapped_ttid'].map(lambda x: x.lstrip('p'))
    contentids = c_df.mapped_ttid.to_list()
    contentids = tuple(np.unique(contentids)) 
    topic_name = get_topic_names(contentids)    
    topic_name = topic_name.rename(columns={'contentid':'mapped_ttid'})
    c_df = c_df.merge(topic_name[['mapped_ttid','contentname']],on='mapped_ttid',how='left') 
    c_df = c_df.rename(columns={'contentname':'topicname'})
    contentids = c_df.contentid.to_list()
    contentids = tuple(np.unique(contentids)) 
    topic_name = get_topic_names(contentids)  
    c_df = c_df.merge(topic_name[['contentid','contentname']],on='contentid',how='left') 
    c_df = c_df.rename(columns={'contentname':'conceptname'})
    
    #c_df = c_df.merge(topic_names,on='contentid',how='left')
    #c_df = c_df.rename(columns={'contentname':'conceptname','contentid': 'conceptid','mapped_ttid':'contentid'})
    #c_df = c_df.merge(topic_names,on='contentid',how='left')
    #c_df = c_df.rename(columns={'contentname':'topicname'})
    idx = c_df.groupby(['upid_class','contentid'])['contentattemptnumber'].transform(max) == c_df['contentattemptnumber']
    c_df=c_df[idx]
    
    total_students = len(np.unique(usage['upid']))
    upid_class = np.unique(usage['upid_class'])
    l_upid_class = len(upid_class)



    logging.info('Total students {}'.format(total_students))
    logging.info('Distinct student and class count {}'.format(l_upid_class))
    logging.info('usage data length {}'.format(len(usage)))
    logging.info('topics data length {}'.format(len(topic_prog)))
    logging.info('concepts data length {}'.format(len(c_df)))

    usage_b2b2c = usage.loc[usage['upid_class'].isin(b2b2c_users)]
    usage_b2c = usage.loc[usage['upid_class'].isin(b2c_users)]
    usage_b2b = usage.loc[usage['upid_class'].isin(b2b_users)]
    
    logging.info('b2b2c distinct student and class count {}'.format(len(usage_b2b2c)))
    logging.info('b2c distinct student and class count {}'.format(len(usage_b2c)))
    logging.info('b2b distinct student and class count {} n'.format(len(usage_b2b)))

    logging.info('no of splits {}'.format(no_of_instances))
    
    #split_factor = total_students//20000
    userids_sets = np.array_split(b2b2c_users,no_of_instances)   
    usg={}
    tpg={}
    cpg={}
    for i,userids in zip(l_instances,userids_sets):
        print(i)
        us = usage.loc[usage['upid_class'].isin(userids)]
        usg["usage_b2b2c{0}".format(i)]=us
        tp = topic_prog.loc[topic_prog['upid_class'].isin(userids)]
        tpg["topic_b2b2c_prog{0}".format(i)]=tp
        cp = c_df.loc[c_df['upid_class'].isin(userids)]
        cpg["concept_b2b2c_prog{0}".format(i)]=cp    
    for key,df in usg.items():
        df.to_csv(str(key)+'.csv',index=False)
    for key, df in tpg.items():
        df.to_csv(str(key)+'.csv',index=False)
    for key, df in cpg.items():
        df.to_csv(str(key)+'.csv',index=False)      

    userids_sets = np.array_split(b2c_users,no_of_instances)   
    usg={}
    tpg={}
    cpg={}
    for i,userids in zip(l_instances,userids_sets):
        print(i)
        us = usage.loc[usage['upid_class'].isin(userids)]
        usg["usage_b2c{0}".format(i)]=us
        tp = topic_prog.loc[topic_prog['upid_class'].isin(userids)]
        tpg["topic_b2c_prog{0}".format(i)]=tp
        cp = c_df.loc[c_df['upid_class'].isin(userids)]
        cpg["concept_b2c_prog{0}".format(i)]=cp    
    for key,df in usg.items():
        df.to_csv(str(key)+'.csv',index=False)
    for key, df in tpg.items():
        df.to_csv(str(key)+'.csv',index=False)
    for key, df in cpg.items():
        df.to_csv(str(key)+'.csv',index=False)  
        
    userids_sets = np.array_split(b2b_users,no_of_instances)   
    usg={}
    tpg={}
    cpg={}
    for i,userids in zip(l_instances,userids_sets):
        print(i)
        us = usage.loc[usage['upid_class'].isin(userids)]
        usg["usage_b2b{0}".format(i)]=us
        tp = topic_prog.loc[topic_prog['upid_class'].isin(userids)]
        tpg["topic_b2b_prog{0}".format(i)]=tp
        cp = c_df.loc[c_df['upid_class'].isin(userids)]
        cpg["concept_b2b_prog{0}".format(i)]=cp    
    for key,df in usg.items():
        df.to_csv(str(key)+'.csv',index=False)
    for key, df in tpg.items():
        df.to_csv(str(key)+'.csv',index=False)
    for key, df in cpg.items():
        df.to_csv(str(key)+'.csv',index=False)  
        
import logging
import logging.handlers
Filename='extract.log'
#Filename='log_12_10_2019_20_46_13.log'
logging.basicConfig(filename=Filename,
                            filemode='a',
                            format='%(asctime)s %(levelname)s %(lineno)d: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S',
                            level=logging.INFO)

try:
    t1 = datetime.datetime.now()
    
    td=datetime.datetime.today()
    day=td.day
    mon_last_day = td - datetime.timedelta(day)
    day=mon_last_day.day
    mon=mon_last_day.month
    month=mon_last_day.strftime("%B")
    mon_first_day = mon_last_day - datetime.timedelta(day-1)
    mon_last_day_str = str(mon_last_day.strftime('%Y-%m-%d'))
    mon_first_day_str = str(mon_first_day.strftime('%Y-%m-%d'))
    logging.info('month  {}'.format(month))
    
    for f in glob('usage*.csv'):
        os.remove(f)        
    for f in glob('topic_*.csv'):
        os.remove(f)
    for f in glob('concept_*.csv'):
        os.remove(f)         
    for f in glob('no_usage*.csv'):
        os.remove(f)  
    for f in glob('userid*.csv'):
        os.remove(f)       
    for f in glob('not_done*.csv'):
        os.remove(f)
    for f in glob('done_*.csv'):
        os.remove(f)
    for f in glob('process*.log'):
        os.remove(f)        

    no_of_instances=int(sys.argv[1])

    split_and_save(no_of_instances)
except Exception as e:
    logging.exception('extraction failed due to '.format(e))