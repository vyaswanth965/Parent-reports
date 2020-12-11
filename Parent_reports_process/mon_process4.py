# -*- coding: utf-8 -*-
"""
Created on Tue Jul  7 10:22:24 2020

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
import pdfkit
import psycopg2
import time
from multiprocessing import Pool
import multiprocessing
from functools import partial
import os
from joblib import Parallel, delayed
from functools import reduce
from matplotlib.transforms import Bbox
import math
from glob import glob
from ec2_metadata import ec2_metadata
import s3fs
from multiprocessing import Manager
import traceback
import sys

cwd="/home/ec2-user/"
os.chdir(cwd)
#import db_config

my_id = ec2_metadata.instance_id
aws_access_key_id= 'AKIA4TXJH6XFLQANY3FQ'
aws_secret_access_key= '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'
region_name='ap-south-1'

def upload_to_aws(grade,upid,usertype,oldschoolcode):
    s3 = boto3.client('s3',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key) 
    local_file = 'reports/Grade{}_{}_{}.pdf'.format(grade,month,upid)
    bucket = 'ei-marketingdata'
    #if usertype[0]=='b2b':
    #    s3_file = '{}/Grade{}_{}_{}.pdf'.format(dir_b2b,grade,month,upid)
    #else:
    if oldschoolcode==3886270:
        s3_file = '{}/{}_Grade{}_{}_{}.pdf'.format(str(oldschoolcode)+month,oldschoolcode,grade,month,upid)
        s3.upload_file(local_file, bucket, s3_file, ExtraArgs={'ACL':'public-read','ContentType': 'application/pdf'})

    s3_file = '{}/Grade{}_{}_{}.pdf'.format(dir,grade,month,upid)
    s3.upload_file(local_file, bucket, s3_file, ExtraArgs={'ACL':'public-read','ContentType': 'application/pdf'})

def pdfGeneration(html_out,grade,upid):
    path=r'/home/ec2-user/wkhtmltopdf'
    config = pdfkit.configuration(wkhtmltopdf=path)
    text = open('{}parent_report_{}.html'.format(grade,upid), 'w')
    text.write(html_out)   
    text.close()
    options = {
        'quiet': '',
        'page-size': 'A4',
        #'margin-top': '1.0in',
        #'margin-bottom': '1.0in',
        #'margin-right': '0.5in',
        #'margin-left': '0.5in',
        'encoding': "UTF-8",
        'no-outline': None
    }
    pdfkit.from_file('{}parent_report_{}.html'.format(grade,upid), 'reports/Grade{}_{}_{}.pdf'.format(grade,month,upid), configuration = config,options=options)
    os.remove('{}parent_report_{}.html'.format(grade,upid))
    

def save_fig(progress):
    data = [progress, 100-progress, 100]
    col=''
    if progress>70:
        col='#3dac53'
    elif progress>50:
        col='#f9cf30'
    else:
        col='#d24841'    
    colors=[col,'#cdceca','white']
    fig, ax = plt.subplots(figsize=(1.5, 1.5))
    plt.pie(data ,startangle=0,colors=colors )
    my_circle=plt.Circle( (0,0), 0.6, color='white')
    p=plt.gcf()
    p.gca().add_artist(my_circle)
    plt.text(0,0 ,'{}%'.format(progress), ha='center', va='bottom',size=13)
    b=Bbox.from_extents(0,0.75,1.5,1.3) # left, bottom, right and top.
    fig.savefig('{}.jpg'.format(progress),bbox_inches=b)
    plt.close(fig) 


def get_topics_covered(upid_class,topic_prog,usage,concept_prog,grd_usg,students_notdone,no_usage,students_done,usertype):
    try:
        #p_df= topic_prog.loc[topic_prog['upid_class']==upid_class,['contentname','monthly_ques_attempts','progress_of_first_attempt','monthly_accuracy']]
        p_df= topic_prog.loc[topic_prog['upid_class']==upid_class]
        t_df = p_df[['contentname','monthly_ques_attempts','progress_of_first_attempt','monthly_accuracy']]    
        c_df = concept_prog[concept_prog['upid_class']==upid_class].reset_index()     
        u_df= usage[usage['upid_class']==upid_class].reset_index()        
    
        if len(u_df)>1:
            print('{} failed due to duplicate rows in usage'.format(upid_class))
            students_notdone.append(upid_class)
            return 0
        upid =  u_df['upid'][0]
        child_name = str(u_df['childname'][0])
        login_days = u_df['monthly_login_days'][0]
        qns = u_df['monthly_total_ques'][0]
        acc = int(u_df['monthly_accuracy'][0])
        usg = u_df['monthly_usage_time'][0]
        child_class = u_df['class'][0]
        correct_ques = u_df['monthly_correct_ques'][0]
        oldschoolcode = int(u_df['oldschoolcode'][0])
        if usertype[0]=='b2b':
            if child_class in [1,2,3]:
                qns_cutoff=400
            elif child_class in [4,5,6,7]:
                qns_cutoff=250    
            else:
                qns_cutoff=150    
        else:
            if child_class in [1,2,3]:
                qns_cutoff=250
            elif child_class in [4,5,6,7]:
                qns_cutoff=200    
            else:
                qns_cutoff=100
        #cls_avg = grd_usg.loc[grd_usg['class']==child_class,'cls_avg'].reset_index()
        #cls_avg = cls_avg['cls_avg'][0]
        cls_avg = qns_cutoff
        l=child_name.split()
        for i in l:
            if len(i)>2:
                name=i
                break
        else:
            name=child_name
        
        def hasNumbers(inputString):
            return any(char.isdigit() for char in inputString)
        flag=hasNumbers(child_name)
        if child_name.upper()=='OM':
            pass
        elif (len(child_name)<3) or (flag) or child_name=='nan':
            child_name='Your child'
            name='Your child'
        else:
            pass
            
        if acc>80:
            mon_recom='{} is doing well'.format(name)
        else:
            mon_recom='{} can do better'.format(name)
            
        if usg==0:
            no_usage.append(upid_class)
            print('{} nousage'.format(upid_class))
            return 0

        if qns==0:
            no_usage.append(upid_class)
            print('{} noqns'.format(upid_class))
            return 0        
        
        
        t_df=t_df.rename(columns={'contentname':'Topics Attempted','monthly_ques_attempts':'Questions Attempted','progress_of_first_attempt':'Progress','monthly_accuracy':'Accuracy'})
        t_df['Progress']=t_df['Progress'].fillna(0)
        t_df['Accuracy']=t_df['Accuracy'].fillna(0)
        t_df['Accuracy']=t_df['Accuracy']*100

        df=t_df.copy()
        df['Progress']=df['Progress'].astype('int')
        df['Progress']=df['Progress'].astype('str')+'%'
        df['Accuracy']=df['Accuracy'].astype('int')
        df['Accuracy']=df['Accuracy'].astype('str')+'%'
        
        p_html=df.to_html(index=False)
        p_html=p_html.replace('<table','<table style="background-color: white; width:100%" cellspacing="0"')
        p_html=p_html.replace('<th>Topics','<th style="padding:10px;text-align: left;">Topics')
        p_html=p_html.replace('<th>Questions','<th style="padding:10px;text-align: centre;">Questions')
        p_html=p_html.replace('<th>Progress','<th style="padding:10px;text-align: centre;">Progress')
        p_html=p_html.replace('<th>Accuracy','<th style="padding:10px;text-align: centre;">Accuracy')
        p_html=p_html.replace('style="text-align: right;"','')
                                                                                  
        p_html=p_html.replace('<tr>\n      <td','<tr>\n      <td style="padding:10px;text-align: left;"')
        p_html=p_html.replace('<tr','<tr style="page-break-inside: avoid;"')
        p_html=p_html.replace('<td>','<td style="padding:2px;text-align: centre;">')
        r=len(df)
        #df['x']=df['Topics Attempted'].str.len()//40
        #r_extra = df['x'].sum()
        #df.drop(['x'],axis=1,inplace=True)
        


        for i,row in df.iterrows():
            progress=int(row['Progress'][:-1])
            accuracy=int(row['Accuracy'][:-1])
            if os.path.exists('{}.jpg'.format(progress)):
                p_html=p_html.replace('>'+row['Progress'],'><img src="{}.jpg">'.format(progress))
            else:     
                save_fig(progress)
                p_html=p_html.replace('>'+row['Progress'],'><img src="{}.jpg">'.format(progress))
            if os.path.exists('{}.jpg'.format(accuracy)):
                p_html=p_html.replace('>'+row['Accuracy'],'><img src="{}.jpg">'.format(accuracy))
            else:     
                save_fig(accuracy)
                p_html=p_html.replace('>'+row['Accuracy'],'><img src="{}.jpg">'.format(accuracy))
                
        if u_df['gender'][0]=='Girl':
            pronoun = 'She'
        else:
            pronoun = 'He'
         

        p_nonzero_df=t_df[t_df['Progress']!=0].reset_index()
        p_nonzero_len = len(p_nonzero_df)
        done_well=''
        if (login_days>=20) and (usg>=360) and (qns>=int(qns_cutoff*.8)) and p_nonzero_len>0:
            done_well='Done Well'
            done_well_img='"./Parent Report_files/Monthly reports for Parent-1-05.png"'
            #temp=p_nonzero_df.iloc[p_nonzero_df['Progress'].idxmax()]
            p_max = p_nonzero_df['Progress'].max()
            p_nonzero_df=p_nonzero_df.loc[p_nonzero_df['Progress']==p_max].reset_index()
            temp = p_nonzero_df.iloc[p_nonzero_df['Accuracy'].idxmax()]
            if temp['Accuracy']>80:
                done_well_stmt='{} had a great time on Mindspark this month. {} has done extremely well in the topic {}, achieving {}% progress with {}% accuracy'.format(name,name,temp['Topics Attempted'],int(temp['Progress']),int(temp['Accuracy']))     
            else:
                done_well_stmt='{} had a great time on Mindspark this month. {} has recently attempted the topic {} achieving {}% accuracy with  {} progress'.format(name,name,temp['Topics Attempted'],int(temp['Accuracy']),int(temp['Progress']))    
        else:
            done_well='Need Attention'
            done_well_img='"./Parent Report_files/Group.png"'
            if p_nonzero_len==0:
                done_well_stmt='{} has not completed any topic in the past month. {} should complete the topic within 15 days for better retention'.format(name,name)
            elif (login_days<20) or (usg<360):
                done_well_stmt='Consistency is the key! {} should practice Maths lessons regularly on Mindpsark'.format(name)
            else:
                done_well_stmt="{} has attempted {} questions correctly. {} should spend more time on reading the question's explanations".format(name,correct_ques,name)
        rec_well_stmt='{} did not find any topic difficult.'.format(name)    
        if len(c_df)>0:
            c_df['accuracy']=c_df['accuracy'].fillna(0)
            temp=c_df.iloc[c_df['accuracy'].idxmin()]
            if temp['accuracy']<60:
                temp2= p_df.loc[p_df['contentid']==temp['mapped_ttid']].reset_index()
                if (len(temp2)!=0) and (temp2['progress_of_first_attempt'][0]==100) :
                    rec_well_stmt='Mindspark recommends revising the topic "{}" as {} had difficulty in the concept "{}".'.format(temp['topicname'],name,temp['conceptname'])
                else:
                    rec_well_stmt='Mindspark recommends completing the topic "{}" as {} had difficulty in the concept "{}".'.format(temp['topicname'],name,temp['conceptname'])
            else:
                rec_well_stmt='{} did not find any topic difficult.'.format(name)

        x = len(rec_well_stmt)//33
        recom_hgt=150
        line_hgt=150
        extra=(x-4)*20
        if x>4:
            recom_hgt=recom_hgt+extra
            line_hgt=line_hgt+extra
        recom_hgt=str(recom_hgt)+'px'
        line_hgt=str(line_hgt)+'px'
        
        #r=r+r_extra
        if r<=6:
            rpt_hgt='1200px'
        elif r<30:
            rpt_hgt=250+1200+(r-6)*52
            rpt_hgt=rpt_hgt+extra
            rpt_hgt=str(rpt_hgt)+'px'
        else:
            rpt_hgt=600+1200+(r-6)*52
            rpt_hgt=rpt_hgt+extra
            rpt_hgt=str(rpt_hgt)+'px'          
        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template("Monthly_Parent_Report.html")
        
        if len(p_df)>0:
            template_vars = {"child_name":child_name,"recom_hgt":recom_hgt,"line_hgt":line_hgt,"done_well_img":done_well_img,"child_class":child_class,"done_well_stmt":done_well_stmt,"rec_well_stmt":rec_well_stmt,"done_well":done_well,"month":month,"cls_avg":cls_avg,"mon_recom":mon_recom,"days":day,"login_days": login_days,"usg":usg,"qns":qns,"acc":acc,"table":p_html,"rpt_hgt":rpt_hgt}
        else:
            template_vars = {"child_name":child_name,"recom_hgt":recom_hgt,"line_hgt":line_hgt,"done_well_img":done_well_img,"child_class":child_class,"done_well_stmt":done_well_stmt,"rec_well_stmt":rec_well_stmt,"done_well":done_well,"month":month,"cls_avg":cls_avg,"mon_recom":mon_recom,"days":day,"login_days": login_days,"usg":usg,"qns":qns,"acc":acc,"rpt_hgt":rpt_hgt}

        html_out = template.render(template_vars)
        pdfGeneration(html_out,child_class,upid)
        upload_to_aws(child_class,upid,usertype,oldschoolcode)
        students_done.append(upid_class)
        print('{} completed'.format(upid_class))
    except Exception as e:
        print('{} failed due to {}'.format(upid_class,e))
        traceback.print_exc()
        students_notdone.append(upid_class)
        #logging.exception('{} failed due to {}'.format(upid,e))
                
                




t1 = datetime.datetime.now() 
mon_last_day_str=sys.argv[1]
mon_last_day = datetime.datetime.strptime(mon_last_day_str,'%Y-%m-%d')
#td=datetime.datetime.today()
#day=td.day
#mon_last_day = td - datetime.timedelta(day)
day=mon_last_day.day
mon=mon_last_day.month
month=mon_last_day.strftime("%B")
dir = 'parentReport_monthly_'+month
#dir_b2b = 'parentReport_monthly_b2b_'+month
mon_first_day = mon_last_day - datetime.timedelta(day-1)
mon_last_day_str = str(mon_last_day.strftime('%Y-%m-%d'))
mon_first_day_str = str(mon_first_day.strftime('%Y-%m-%d'))



usertypes=['b2b2c','b2b','b2c']
#usertypes=['b2c']

for usertype in usertypes:
    usage = pd.read_csv('usage_{}{}.csv'.format(usertype,my_id))

    topic_prog = pd.read_csv('topic_{}_prog{}.csv'.format(usertype,my_id))
    concept_prog = pd.read_csv('concept_{}_prog{}.csv'.format(usertype,my_id))                                                              
    grd_usg = pd.read_csv('grd_usg.csv'.format(my_id))


    manager = Manager()
    usertype_v = manager.list()
    usertype_v.append(usertype)

    students_notdone = manager.list()
    no_usage = manager.list()
    students_done = manager.list()

    print('total students {}'.format(len(usage['upid'].unique())))
    print('{} unique upid,class count  {}'.format(usertype,len(usage['upid_class'].unique())))

    Parallel(n_jobs=-1)(delayed(get_topics_covered)(i,topic_prog,usage,concept_prog,grd_usg,students_notdone,no_usage,students_done,usertype_v) for i in usage['upid_class'].unique())
    students_notdone=list(students_notdone)
    no_usage=list(no_usage)
    students_done=list(students_done)
    #pd.DataFrame({'upid_class':students_notdone}).to_csv('not_done_{}_students{}.csv'.format(usertype,my_id),index=False)
    df=pd.DataFrame({'upid_class':students_notdone})
    if len(df)>0:
        df=df['upid_class'].astype(str).str.split('_',expand=True)
        df.columns=['upid','class']
    df.to_csv('not_done_{}_students{}.csv'.format(usertype,my_id),index=False)
    
    df=pd.DataFrame({'upid_class':no_usage})
    if len(df)>0:
        df=df['upid_class'].astype(str).str.split('_',expand=True)
        df.columns=['upid','class']
    df.to_csv('no_usage_{}_students{}.csv'.format(usertype,my_id),index=False)
    
    df=pd.DataFrame({'upid_class':students_done})
    if len(df)>0:
        df=df['upid_class'].astype(str).str.split('_',expand=True)
        df.columns=['upid','class']
    df.to_csv('done_{}_students{}.csv'.format(usertype,my_id),index=False)    

    print('not_done{} upid_class count {}'.format(usertype,len(students_notdone)))
    print('no_usage {} upid_class count {}'.format(usertype,len(no_usage)))
    print('done {} upid_class count {}'.format(usertype,len(students_done)))



    s3 = boto3.client('s3',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key) 
    bucket = 'ei-marketingdata'
    local_file = 'done_{}_students{}.csv'.format(usertype,my_id)
    s3_file = '{}/done_{}_students{}.csv'.format(dir,usertype,my_id)
    s3.upload_file(local_file, bucket, s3_file)
    local_file = 'not_done_{}_students{}.csv'.format(usertype,my_id)
    s3_file = '{}/not_done_{}_students{}.csv'.format(dir,usertype,my_id)
    s3.upload_file(local_file, bucket, s3_file)
    local_file = 'no_usage_{}_students{}.csv'.format(usertype,my_id)
    s3_file = '{}/no_usage_{}_students{}.csv'.format(dir,usertype,my_id)
    s3.upload_file(local_file, bucket, s3_file)	
t2 = datetime.datetime.now()
print('The whole thing took: '+str(round((t2-t1).total_seconds()/60 , 2))+ ' min')   
ec2 = boto3.client('ec2',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)

time.sleep(20)

ec2.terminate_instances(InstanceIds=[my_id])
