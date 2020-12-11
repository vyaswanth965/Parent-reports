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
from sqlalchemy import create_engine
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib
cwd="/home/ei_datascience/Parent_reports/"
os.chdir(cwd)
import db_config

def email_notification(usage_df,dir,b2c_reports,total_zero):
    try:
        msg = MIMEMultipart()
        msg['Subject'] = "Automated mail - Weekly MS Maths Parent Reports"
        msg['From'] = 'ei.datascience@ei-india.com'
        msg['To'] = ', '.join(['yashwanth.kumar@ei-india.com','gaurav.shukla@ei-india.com'])
        msg['Cc'] = ', '.join([])

        #recipients = db_config.to+db_config.cc

        html = """\
            <html>
            <head></head>
            <body>
            Hi,
            <br><br>
            The weekly reports are generated and saved in the S3 bucket ei-marketingdata/{1}. <br><br>
            There are a total of {2} reports generated and  {3} zero usage records. The breakdown is given below.<br><br>
            {0}
            </body>
            </html>
            """.format(usage_df,dir,b2c_reports,total_zero)

        part1 = MIMEText(html, 'html')
        msg.attach(part1)

        # server = SMTP('smtp.zoho.in', 465)
        # server.sendmail(msg['From'], db_config.recipients , msg.as_string())
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login('ei.datascience@ei-india.com', 'KSTF0r3v3r@EI')
        server.sendmail(msg['From'], msg['To'] , msg.as_string())
        # server.sendmail(sender, recipients, email_text)
        server.close()
    
        logging.info('Email sent!')
    except Exception as e:
        logging.info(e)
        logging.info('Something  went wrong...')
        
def livecon_r():
    global redshift
    global mysql
    global adepts
    redshift = psycopg2.connect(dbname=db_config.redshift_dbname, host=db_config.redshift_host, port=db_config.redshift_port, user=db_config.redshift_user, password=db_config.redshift_password)
    mysql = pymysql.connect(host=db_config.staging_ip, user=db_config.username_staging, password=db_config.pwd_staging)
    adepts = pymysql.connect(host=db_config.staging_ip, user=db_config.username_staging, password=db_config.pwd_staging, db="educatio_adepts")


def get_homework_name():
    query = '''select contentid,'Home Work - '||contentname as contentname from hw_content_stg    '''        
    livecon_r()
                                        

    df = pd.read_sql_query(query.format(), redshift)
    return df
    
def get_b2b2c(week_st_date,week_en_date):
    query = '''SELECT distinct A.MS_userID
    FROM educatio_educat.common_user_details A, mapping.user_management_mapping B,
    educatio_adepts.adepts_registrationDetailsLog C WHERE A.id=B.oldID AND A.userName=C.username
    AND enddate>'{}' AND category='STUDENT' AND subCategory='School'
    AND A.MS_userID not in (SELECT MS_userID from educatio_educat.common_user_details where startDate > '{}')
    AND (A.childEmail is not null or A.additionalEmail is not null or A.contactno_cel is not null)'''.format(week_st_date,week_en_date)  
    logging.info(query)
    livecon_r()
    df = pd.read_sql(query, adepts)
    return df 



def get_b2c(week_st_date,week_en_date):
    query = '''SELECT distinct MS_userID
    FROM educatio_educat.common_user_details
    WHERE enddate>='{}'
    AND category='STUDENT'
    AND subCategory='Individual'
    AND MS_userID not in (SELECT MS_userID from educatio_educat.common_user_details where startDate > '{}')
    AND (childEmail is not null or additionalEmail is not null or contactno_cel is not null)'''.format(week_st_date,week_en_date)  
    logging.info(query)
    livecon_r()
    df = pd.read_sql(query, adepts)
    return df     

def get_b2b_b2b2c(week_st_date,week_en_date):
    query = '''SELECT distinct MS_userID
    FROM educatio_educat.common_user_details
    WHERE enddate>='{}'
    AND category='STUDENT'
    AND subCategory='School'
    AND MS_userID not in (SELECT MS_userID from educatio_educat.common_user_details where startDate > '{}')
    AND (childEmail is not null or additionalEmail is not null or contactno_cel is not null)'''.format(week_st_date,week_en_date)  
    logging.info(query)
    livecon_r()
    df = pd.read_sql(query, adepts)
    return df  
    
def get_usage_data(week_st_date,week_en_date):
    query = '''select oldupid as upid,upid as newupid,childname as child_name,class,gender,round(weekly_accuracy) as weekly_accuracy,weekly_total_ques as weekly_ques_attempts,weekly_correct_ques as weekly_correct_ques_attempts,usage_time_fri,usage_time_sat,usage_time_sun,usage_time_mon,usage_time_tue,usage_time_wed,usage_time_thu from msm_usage_weekly where week_start_date ='{}' 
    and upid in (select upid from userdetails_dim where category='STUDENT'and enabled in (1,0) and enddate>= '{}' and (parentemail is not null or contactno_cel is not null or childemail is not null)
    and upid not in (select upid from userdetails_dim where category='STUDENT' and startdate > '{}') )'''.format(week_st_date,week_st_date,week_en_date)  
    logging.info(query)
    livecon_r()
    df = pd.read_sql(query, redshift)
    return df 

def get_topic_data(week_st_date,week_en_date):
    #query = '''select upid,contentid,weekly_ques_attempts,progress_of_first_attempt,total_correct_ques_attempts	,total_ques_attempts	,round(ttid_accuracy*100) as total_accuracy from msm_moduleprogress_weekly where week_start_date ='{}'   '''.format(week_st_date)    
    query = '''select oldupid as upid,contentid,weekly_correct_ques_attempts,weekly_ques_attempts,progress_of_first_attempt,round(weekly_accuracy*100) as weekly_accuracy from msm_moduleprogress_weekly where week_start_date ='{}'  
    and upid in (select upid from userdetails_dim where category='STUDENT'and enabled in (1,0) and enddate>= '{}' and (parentemail is not null or contactno_cel is not null or childemail is not null)
    and upid not in (select upid from userdetails_dim where category='STUDENT' and startdate > '{}') )'''.format(week_st_date,week_st_date,week_en_date)
    logging.info(query)
    livecon_r()
    df = pd.read_sql_query(query.format(), redshift)
    return df
    

def get_supertest(week_st_date,week_en_date):
    query = '''Select a.userID as upid, a.paperCode,d.testName,b.class, b.topicName , sum(a.R) as correct_qns  from educatio_adepts.da_questionAttemptDetails a, educatio_adepts.da_paperCodeMaster b, educatio_educat.da_testRequest d where  b.paperCode=d.paper_code and a.attemptdate between '{}' and '{}' and a.paperCode = b.paperCode    group by a.userID, a.paperCode, b.topicName,b.class ,d.testName
    '''.format(week_st_date,week_en_date)  
    livecon_r()
    df = pd.read_sql(query, mysql)
    return df

def get_paper_qns_cnt(papercodes):
    query = '''Select papercode as paperCode,qcode_list from educatio_educat.da_paperDetails a where papercode in {}''' .format(papercodes)
    livecon_r()
    df = pd.read_sql(query, mysql)
    return df
    
def split_and_save(no_of_instances):
    b2b2c_users = get_b2b2c(last_fri_str,thur_str)
    b2b2c_users.MS_userID = b2b2c_users.MS_userID.astype(str)
    b2b2c_users = set(b2b2c_users['MS_userID'])
    b2c_users = get_b2c(last_fri_str,thur_str)
    b2c_users.MS_userID = b2c_users.MS_userID.astype(str)
    b2c_users = set(b2c_users['MS_userID'])
    b2b_b2b2c_users = get_b2b_b2b2c(last_fri_str,thur_str)
    b2b_b2b2c_users.MS_userID = b2b_b2b2c_users.MS_userID.astype(str)                                                                     
    b2b_b2b2c_users = set(b2b_b2b2c_users['MS_userID'])
    b2b_users = b2b_b2b2c_users - b2b2c_users

    query = '''    select userID as upid  from educatio_adepts.freeTrialDetail where endDate>='{}' and startDate<='{}' 
    '''
    livecon_r()
    ms2_retail_users = pd.read_sql(query.format(last_fri_str,thur_str), adepts)
    l=tuple(ms2_retail_users['upid'].unique())

    query = '''select distinct oldupid from userdetails_dim ud where oldupid in {} and subcategory='School' '''
    livecon_r()
    ms2_wrongly_tagged_users = pd.read_sql(query.format(l), redshift)
    ms2_wrongly_tagged_users.oldupid = ms2_wrongly_tagged_users.oldupid.astype(str)
    ms2_wrongly_tagged_users_list = set(ms2_wrongly_tagged_users['oldupid'])
    
    logging.info('b2b '+str(len(b2b_users)))
    logging.info('b2c '+str(len(b2c_users)))
    logging.info('b2b2c '+str(len(b2b2c_users)))
    
    b2c_users = b2c_users.union(ms2_wrongly_tagged_users_list)
    b2b_users = b2b_users - ms2_wrongly_tagged_users_list
    
    logging.info('wrong b2c '+str(len(ms2_wrongly_tagged_users_list)))
    

    logging.info('b2b '+str(len(b2b_users)))
    logging.info('b2c '+str(len(b2c_users)))
    logging.info('b2b2c '+str(len(b2b2c_users)))
    
    #logging.info('wrong b2c sample '+ str(ms2_wrongly_tagged_users_list))
    
    
    b2b_users = list(b2b_users)
    b2b2c_users = list(b2b2c_users)
    b2c_users = list(b2c_users)
    
    usage = get_usage_data(last_fri_str,thur_str)
    topic_prog = get_topic_data(last_fri_str,thur_str)
    super_df = get_supertest(last_fri_str,thur_str)
    papercodes = tuple(super_df['paperCode'].unique())
    p_df = get_paper_qns_cnt(papercodes)
    p_df['qns_cnt']=p_df['qcode_list'].str.count(',')+1
    super_df=super_df.merge(p_df[['paperCode','qns_cnt']],on='paperCode')
    
    #topic_names=get_topics_name()
    #topic_prog = topic_prog.merge(topic_names,on='contentid',how='left')
    topic_prog['contentid'] = topic_prog['contentid'].map(lambda x: x.lstrip('p'))
    contentids = topic_prog.contentid.to_list()
    contentids = tuple(np.unique(contentids))   
    livecon_r()
    query=f"""select contentid,case when type='worksheet' then 'Work Sheet - '||contentname else contentname end as contentname from msm_topic_flow  where contentid in {contentids}"""
    topic_name_1 = pd.read_sql(query,redshift)
    livecon_r()
    query=f"""select oldcontentid contentid,case when type='worksheet' then 'Work Sheet - '||contentname else contentname end as contentname from msm_topic_flow  where oldcontentid in {contentids}"""
    topic_name_2 = pd.read_sql(query,redshift)
    homework_name = get_homework_name()
    topic_name = pd.concat([topic_name_1, topic_name_2,homework_name])
    topic_name = topic_name.drop_duplicates()
    topic_prog = topic_prog.merge(topic_name[['contentid','contentname']],on='contentid',how='left') 
    
    students= np.unique(usage['upid'])
    total_students= len(students)

    logging.info('Total students {}'.format(total_students))
    logging.info('usage data length {}'.format(len(usage)))
    logging.info('topics data length {}'.format(len(topic_prog)))
    logging.info('super_df data length {}'.format(len(super_df)))
    super_df.to_csv('super_df.csv',index=False)
    usage_b2b2c = usage.loc[usage['upid'].isin(b2b2c_users)]
    usage_b2c = usage.loc[usage['upid'].isin(b2c_users)]
    usage_b2b = usage.loc[usage['upid'].isin(b2b_users)]
    
    if len(super_df)>0:
        super_df_b2b2c = super_df.loc[super_df['upid'].isin(b2b2c_users)]
        super_df_b2c = super_df.loc[super_df['upid'].isin(b2c_users)]
        super_df_b2b = super_df.loc[super_df['upid'].isin(b2b_users)]

        logging.info('super b2b2c students {}'.format(len(super_df_b2b2c)))
        logging.info('super b2c students {}'.format(len(super_df_b2c)))
        logging.info('super b2b students {} (not processing now)'.format(len(super_df_b2b)))  
        
    logging.info('b2b2c students {}'.format(len(b2b2c_users)))
    logging.info('b2c students {}'.format(len(b2c_users)))
    logging.info('b2b students {} (not processing now)'.format(len(b2b_users)))
    
    pd.DataFrame({'oldupid':b2b2c_users}).to_csv('b2b2c.csv',index=False)
    pd.DataFrame({'oldupid':b2c_users}).to_csv('b2c.csv',index=False)
    pd.DataFrame({'oldupid':b2b_users}).to_csv('b2b.csv',index=False)
   
    logging.info('b2b2c students {}'.format(len(usage_b2b2c)))
    logging.info('b2c students {}'.format(len(usage_b2c)))
    logging.info('b2b students {} (not processing now)'.format(len(usage_b2b)))

    
    #split_factor = total_students//20000
    userids_sets = np.array_split(b2b2c_users,no_of_instances)    
    usg={}
    tpg={}
    for i,userids in zip(l_instances,userids_sets):
        print(i)
        us = usage.loc[usage['upid'].isin(userids)]
        usg["usage_b2b2c_{0}".format(i)]=us
        tp = topic_prog.loc[topic_prog['upid'].isin(userids)]
        tpg["topic_prog_b2b2c_{0}".format(i)]=tp   
    for key,df in usg.items():
        df.to_csv(str(key)+'.csv',index=False)
    for key, df in tpg.items():
        df.to_csv(str(key)+'.csv',index=False) 

    userids_sets = np.array_split(b2c_users,no_of_instances)    
    usg={}
    tpg={}
    for i,userids in zip(l_instances,userids_sets):
        us = usage.loc[usage['upid'].isin(userids)]
        usg["usage_b2c_{0}".format(i)]=us
        tp = topic_prog.loc[topic_prog['upid'].isin(userids)]
        tpg["topic_prog_b2c_{0}".format(i)]=tp  
    for key,df in usg.items():
        df.to_csv(str(key)+'.csv',index=False)
    for key, df in tpg.items():
        df.to_csv(str(key)+'.csv',index=False)   
    '''userids_sets = np.array_split(b2b_users,no_of_instances)    
    usg={}
    tpg={}
    for i,userids in zip(l_instances,userids_sets):
        print(i)
        us = usage.loc[usage['upid'].isin(userids)]
        usg["usage_b2b_{0}".format(i)]=us
        tp = topic_prog.loc[topic_prog['upid'].isin(userids)]
        tpg["topic_prog_b2b_{0}".format(i)]=tp   
    for key,df in usg.items():
        df.to_csv(str(key)+'.csv',index=False)
    for key, df in tpg.items():
        df.to_csv(str(key)+'.csv',index=False) '''       

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
    #td = datetime.datetime.strptime('2020-09-26','%Y-%m-%d')
    this_monday_date = td - datetime.timedelta((td.weekday()) % 7)
    last_fri = this_monday_date - datetime.timedelta(3)
    last_fri_str = str(last_fri.strftime('%Y-%m-%d'))
    last_fri_str2=last_fri_str.replace('-','_')
    dir = 'parentReport_weekly_'+last_fri_str2
    logging.info('week start date {}'.format(last_fri_str))
    thur = last_fri + datetime.timedelta(6)
    thur_str = str(thur.strftime('%Y-%m-%d'))
    
    for f in glob('usage*.csv'):
        os.remove(f)        
    for f in glob('topic_*.csv'):
        os.remove(f)
    for f in glob('concept_*.csv'):
        os.remove(f)
    for f in glob('userid*.csv'):
        os.remove(f)       
    for f in glob('not_done*.csv'):
        os.remove(f)
    for f in glob('done_*.csv'):
        os.remove(f)
    for f in glob('process*.log'):
        os.remove(f)        
    for f in glob('no_usage*.csv'):
        os.remove(f) 
    for f in glob('concept_prog*.csv'):
        os.remove(f)  
    for f in glob('super*.csv'):
        os.remove(f)
    no_of_instances=int(sys.argv[1])

    aws_access_key_id= 'AKIA4TXJH6XFLQANY3FQ'
    aws_secret_access_key= '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'
    ec2 = boto3.resource('ec2',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    instances = ec2.create_instances(
         ImageId='ami-0b006b4d49e34bb2f',
         #ImageId='ami-078aadfc242f667eb',
         MinCount=1,  
         MaxCount=no_of_instances,  # number of instance to be launched
    #     InstanceType='t2.micro',
         InstanceType='t3.2xlarge',
         KeyName='autoLaunchKeypair',
         TagSpecifications=[
        {
            'ResourceType': 'instance',
            'Tags': [
                {
                    'Key': 'Name',
                    'Value': 'weekly worker'
                }
				]
				}
				],
         Monitoring={
        'Enabled': True
    }       
     )

    l_instances = [ec2_instance.instance_id for ec2_instance in instances]  
    logging.info(l_instances)  
    time.sleep(120)
    
    split_and_save(no_of_instances)    

    ec2 = boto3.client('ec2',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    response=ec2.describe_instances()
    #print(response)

    d_instances={}
    for x in response['Reservations']:
        for y in x['Instances']:
            if y['InstanceId'] in l_instances:
                d_instances[y['InstanceId']]=y['PublicDnsName']
    logging.info(d_instances)
    
    def start_process(k,v):
        os.system('scp -o StrictHostKeyChecking=no -i ~/Parent_reports/autoLaunchKeypair.pem ~/Parent_reports/usage_b2b2c_{}.csv  ec2-user@{}:/home/ec2-user/'.format(k,v))
        os.system('scp -o StrictHostKeyChecking=no -i ~/Parent_reports/autoLaunchKeypair.pem ~/Parent_reports/topic_prog_b2b2c_{}.csv  ec2-user@{}:/home/ec2-user/'.format(k,v))
        #os.system('scp -o StrictHostKeyChecking=no -i ~/Parent_reports/autoLaunchKeypair.pem ~/Parent_reports/super_df_b2b2c_{}.csv  ec2-user@{}:/home/ec2-user/'.format(k,v))
        os.system('scp -o StrictHostKeyChecking=no -i ~/Parent_reports/autoLaunchKeypair.pem ~/Parent_reports/usage_b2c_{}.csv  ec2-user@{}:/home/ec2-user/'.format(k,v))
        os.system('scp -o StrictHostKeyChecking=no -i ~/Parent_reports/autoLaunchKeypair.pem ~/Parent_reports/topic_prog_b2c_{}.csv  ec2-user@{}:/home/ec2-user/'.format(k,v))
        #os.system('scp -o StrictHostKeyChecking=no -i ~/Parent_reports/autoLaunchKeypair.pem ~/Parent_reports/super_df_b2c_{}.csv  ec2-user@{}:/home/ec2-user/'.format(k,v))
        os.system('scp -o StrictHostKeyChecking=no -i ~/Parent_reports/autoLaunchKeypair.pem ~/Parent_reports/super_df.csv  ec2-user@{}:/home/ec2-user/'.format(v))
        #os.system('scp -o StrictHostKeyChecking=no -i ~/Parent_reports/autoLaunchKeypair.pem ~/Parent_reports/usage_b2b_{}.csv  ec2-user@{}:/home/ec2-user/'.format(k,v))
        #os.system('scp -o StrictHostKeyChecking=no -i ~/Parent_reports/autoLaunchKeypair.pem ~/Parent_reports/topic_prog_b2b_{}.csv  ec2-user@{}:/home/ec2-user/'.format(k,v))
        os.system('ssh -o StrictHostKeyChecking=no -i ~/Parent_reports/autoLaunchKeypair.pem  ec2-user@{} python  /home/ec2-user/process4.py {} 1>process{}.log 2>&1'.format(v,last_fri_str,k))
        #os.system('ssh -o StrictHostKeyChecking=no -i ~/Parent_reports/autoLaunchKeypair.pem  ec2-user@{} python  /home/ec2-user/send_logs.py'.format(v))
       
    s3 = boto3.client('s3',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    bucket_name='ei-marketingdata'
    s3.put_object(Bucket=bucket_name, Key=(dir+'/'))
    Parallel(n_jobs=no_of_instances)(delayed(start_process)(k,v) for k,v in d_instances.items())


        #os.system('scp -o StrictHostKeyChecking=no -i ~/Parent_reports/autoLaunchKeypair.pem ~/Parent_reports/userid{}.csv  ec2-user@{}:/home/ec2-user/'.format(k,v))
    # Stop EC2 Instances
    #ec2.start_instances(InstanceIds=l_instances)

    # start EC2 Instances
    #ec2.stop_instances(InstanceIds=l_instances)
    
    #usertypes=['b2b2c','b2b','b2c']
    df4=pd.DataFrame(columns=['oldupid','category','report_status'])
       
    usertypes=['b2b2c','b2c']
    for usertype in usertypes:    
        df1=pd.DataFrame()
        df2=pd.DataFrame()
        df3=pd.DataFrame()
        tmp4=pd.DataFrame()
        with open('process.log', 'w') as outfile:
            for k in l_instances:
                try:
                    local_file = 'no_usage_{}_students{}.csv'.format(usertype,k)
                    s3_file = '{}/no_usage_{}_students{}.csv'.format(dir,usertype,k)
                    s3.download_file(bucket_name,s3_file,local_file)
                    tmp1=pd.read_csv('no_usage_{}_students{}.csv'.format(usertype,k))
                    df1=df1.append(tmp1)
                    local_file = 'not_done_{}_students{}.csv'.format(usertype,k)
                    s3_file = '{}/not_done_{}_students{}.csv'.format(dir,usertype,k)
                    s3.download_file(bucket_name,s3_file,local_file)
                    tmp2=pd.read_csv('not_done_{}_students{}.csv'.format(usertype,k))
                    df2=df2.append(tmp2) 
                    local_file = 'done_{}_students{}.csv'.format(usertype,k)
                    s3_file = '{}/done_{}_students{}.csv'.format(dir,usertype,k)
                    s3.download_file(bucket_name,s3_file,local_file)
                    tmp3=pd.read_csv('done_{}_students{}.csv'.format(usertype,k))
                    df3=df3.append(tmp3) 
                except Exception as e:
                    pass
                with open('process{}.log'.format(k)) as infile:
                    outfile.write(infile.read()) 
        df1.to_csv('no_usage_{}_students.csv'.format(usertype),index=False)            
        df2.to_csv('not_done_{}_students.csv'.format(usertype),index=False) 
        df3.to_csv('done_{}_students.csv'.format(usertype),index=False)  
        logging.info('no_usage_{}_students {}'.format(usertype,len(df1))) 
        logging.info('not_done_{}_students {}'.format(usertype,len(df2))) 
        logging.info('done {} students {}'.format(usertype,len(df3)))
        df1['report_status']='zero_usage'
        tmp4=tmp4.append(df1)
        df2['report_status']='failed'
        tmp4=tmp4.append(df2)
        df3['report_status']='completed'        
        tmp4=tmp4.append(df3)
        tmp4['category']=usertype
        df4=df4.append(tmp4)
    
        s3 = boto3.client('s3',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key) 
        local_file = 'no_usage_{}_students.csv'.format(usertype)
        bucket = 'ei-marketingdata'
        s3_file = '{}/no_usage_{}_students.csv'.format(dir,usertype)
        s3.upload_file(local_file, bucket, s3_file,ExtraArgs={'ACL':'public-read'})
        
    df4.to_csv('students.csv',index=False)  
    logging.info('students {}'.format(len(df4))) 
    df4['week_str_date']=last_fri_str
    df4['week_str_date']=pd.to_datetime(df4['week_str_date'],format='%Y-%m-%d')
    df4.to_parquet('students.parquet',index=False) 
    
    s3 = boto3.client('s3',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key) 
    local_file = 'students.csv'.format(usertype)
    bucket = 'ei-marketingdata'
    s3_file = '{}/students.csv'.format(dir,usertype)
    s3.upload_file(local_file, bucket, s3_file,ExtraArgs={'ACL':'public-read'})
    local_file = 'students.parquet'.format(usertype)
    bucket = 'ei-pr-studentdata'
    s3.put_object(Bucket=bucket, Key=(dir+'/'))
    s3_file = '{}/students.parquet'.format(dir,usertype)
    s3.upload_file(local_file, bucket, s3_file,ExtraArgs={'ACL':'public-read'})    
    
    t2 = datetime.datetime.now()
    logging.info('The whole thing took: '+str(round((t2-t1).total_seconds()/60 , 2))+ ' min')
    
    ##Email
    b2b2c=df4[df4['category']=='b2b2c']
    b2b2c_zero = len(b2b2c[b2b2c['report_status']=='zero_usage'])  
    b2b2c_reports = len(b2b2c[b2b2c['report_status']=='completed'])       
    b2c=df4[df4['category']=='b2c']
    b2c_zero = len(b2c[b2c['report_status']=='zero_usage'])  
    b2c_reports = len(b2c[b2c['report_status']=='completed'])    
    total_reports=b2c_reports+b2b2c_reports
    total_zero=b2b2c_zero+b2c_zero
    usage_list = [[b2b2c_zero, b2c_zero,total_zero],[b2b2c_reports,b2c_reports, total_reports],[len(b2b2c),len(b2c), len(b2b2c)+len(b2c)]]
    usage_df = pd.DataFrame(usage_list,columns=['B2B2C','B2C','Total'])
    usage_df = usage_df.rename(index={0 : 'Zero Usage', 1 : 'Reports Generated', 2 : 'Total'})

    html=usage_df.to_html()
    html=html.replace('class="dataframe"',' class="dataframe"  cellspacing="0" ')
    html=html.replace('<th>','<th style="padding:5px;text-align: centre;">')
    html=html.replace('<td>','<td style="padding:5px;text-align: right;">')
    email_notification(html,dir,total_reports,total_zero)
    
    #Logs to table
    t3 = datetime.datetime.now()
    redshift_user = "ei_user"
    redshift_pass = "Ei-india2019"
    redshift_endpoint = "ei-datascience.ccf2enlidivo.ap-southeast-1.redshift.amazonaws.com"
    port = 5439
    dbname = "sessions"

    engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" \
    % (redshift_user, redshift_pass, redshift_endpoint, port, dbname)
    engine = create_engine(engine_string)

    sql="""

    DROP TABLE IF EXISTS weekly_parent_report_logs_stg;

    CREATE TABLE IF NOT EXISTS weekly_parent_report_logs_stg(
    category VARCHAR(100),oldupid int8,
    report_status VARCHAR(100),
    week_str_date date);


    COPY  weekly_parent_report_logs_stg
    FROM 's3://ei-pr-studentdata/{}/students.parquet'
    IAM_ROLE 'arn:aws:iam::866998810058:role/myRedshiftRole'
    Format as parquet;


    CREATE TABLE IF NOT EXISTS weekly_parent_report_logs(
    category VARCHAR(100),oldupid int8,
    report_status VARCHAR(100),
    week_str_date date,
    lastModified datetime default sysdate);

    INSERT INTO "public"."weekly_parent_report_logs"
    (SELECT category,oldupid,report_status,trunc(week_str_date) as week_str_date
    FROM "public"."weekly_parent_report_logs_stg");
     
    DROP TABLE IF EXISTS weekly_parent_report_logs_stg;

    commit;
    """.format(dir)
    engine.execute(sql)
    
    t4 = datetime.datetime.now()

    logging.info('Table loading took : '+str(round((t4-t3).total_seconds()/60 , 2))+ ' min')
except Exception as e:
    logging.exception('extraction failed due to '.format(e))