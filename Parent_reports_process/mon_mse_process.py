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
import re
import itertools
cwd="/home/ec2-user/"
os.chdir(cwd)
#import db_config

my_id = ec2_metadata.instance_id
aws_access_key_id= 'AKIA4TXJH6XFLQANY3FQ'
aws_secret_access_key= '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'
region_name='ap-south-1'


def upload_to_aws(upid,usertype):
    s3 = boto3.client('s3',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key) 
    local_file = 'reports/{}.pdf'.format(upid)
    bucket = 'ei-marketingdata'
    #if usertype[0]=='b2b':
    #    s3_file = '{}/{}.pdf'.format(dir_b2b,upid)
    #else:
    s3_file = '{}/{}.pdf'.format(dir,upid)
    s3.upload_file(local_file, bucket, s3_file, ExtraArgs={'ACL':'public-read','ContentType': 'application/pdf'})

        
def pdfGeneration(html_out,upid):
    path=r'/home/ec2-user/wkhtmltopdf'
    config = pdfkit.configuration(wkhtmltopdf=path)
    text = open('parent_report_{}.html'.format(upid), 'w')
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
    pdfkit.from_file('parent_report_{}.html'.format(upid), 'reports/{}.pdf'.format(upid), configuration = config,options=options)
    os.remove('parent_report_{}.html'.format(upid))
    
def get_topics_covered(upid,topic,usage,class_df,class_qn_df,qns_df,blank_qn_ans_df,students_notdone,no_usage,students_done,usertype):
    try:
        p_df= topic.loc[topic['upid']==upid].copy()

        u_df= usage[usage['upid']==upid].reset_index()        
        p_df.drop(['upid'],axis=1,inplace=True)   

        topic_r_l =  p_df[(p_df['passageID']!=0) & (p_df['completed']==2.0)] 
        topic_r_l = topic_r_l.groupby(['Skill','form']).agg({'passageID':'nunique'}).reset_index()

        topic2 = p_df.groupby(['Skill','subskillname']).agg({'qcode':'count','correct':'sum'}).reset_index()
        topic2['Acc']=(topic2[ 'correct']/topic2['qcode'])*100

        #topic_g_v =topic2[topic2['Skill'].isin(['3.Grammar','4.Vocabulary'])]

        if len(u_df)>1:
            print('{} failed due to duplicate rows in usage'.format(upid))
            students_notdone.append(upid)
            return 0   
        child_name=str(u_df['childName'][0])
        child_class=u_df['childClass'][0]
        login_days=u_df['login_days'][0]
        is_60mineveryweek=u_df['is_60mineveryweek'][0]
        time_spent=u_df['time_spent'][0]
        qns=u_df['qcode'][0]
        if (time_spent==0) or (qns==0) :
            no_usage.append(upid)
            print('{} nousage'.format(upid))
            return 0 
        acc=u_df['Accuracy'][0]
        #psg=topic_r_l['passageID'].sum()
        cls_avg_qn= class_df.loc[class_df['childClass']==child_class,'cls_avg_qn'].iloc[0]
        cls_acc= class_df.loc[class_df['childClass']==child_class,'Cls_Accuracy'].iloc[0]

        if u_df['gender'][0]=='Girl':
            pronoun = 'She'
            pronoun2 = 'her'
            pronoun3 = 'her'
        else:
            pronoun = 'He'
            pronoun2 = 'him'
            pronoun3 = 'his'
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
        name=name.capitalize()
        done_well_stmt=''
        usg='low'
        strongsubskill='f'
        if (is_60mineveryweek=='Y') or (login_days>14):
            usg='high'
            done_well_stmt="{} has used Mindspark regularly this month. Well done! ".format(name)

        listen_excer_count=len(p_df[p_df['qTemplate']=='speaking'])
        if listen_excer_count>0:   
            done_well_stmt=done_well_stmt+"{} has done {} speaking exercises this month. ".format(name,listen_excer_count)

        temp= topic2[(topic2['qcode']>=10) & (topic2['Acc']>=75)].reset_index()
        if len(temp)>0:
            strongsubskill='t'
            temp = temp.iloc[temp['Acc'].idxmax()]
            done_well_stmt=done_well_stmt+"{} has done well in {}, achieving {}% accuracy. ".format(name,temp['subskillname'],int(temp['Acc']))
            
        if (usg=='low') and (strongsubskill=='f'):
            done_well_stmt=done_well_stmt+"Use Mindspark more to find out what your strengths are!"



        rec_well_stmt=''
        if time_spent/days <15:
            rec_well_stmt="We recommend that you practise on Mindspark for at least 30 minutes each day so that you can attempt questions in all the skills. " 

        temp= topic2[(topic2['qcode']>=10) & (topic2['Acc']<40)].reset_index()
        if len(temp)>0:
            temp = temp.iloc[temp['Acc'].idxmin()]
            rec_well_stmt=rec_well_stmt+"Mindspark recommends practising {}, as {} had difficulty in this concept, with {}% accuracy.".format(temp['subskillname'],name,int(temp['Acc']))

        topic_r = topic_r_l[topic_r_l['Skill']=='1.Reading']
        r_psg=topic_r['passageID'].sum()
        psg = r_psg
        if r_psg==0:
            reading_stmt='{} read no passages.'.format(name)    
        elif r_psg==1:
            reading_stmt='{} read {} passage.'.format(name,r_psg)
        else:
            reading_stmt='{} read {} passages'.format(name,r_psg)
          
        topic_r=topic_r.sort_values(['passageID'],ascending=False).iloc[0:2]

        if len(topic_r)==2 :
            pl=list(topic_r['passageID'])
            if (pl[0]>=2) and (pl[1]>=2):
                reading_stmt = reading_stmt+', which included {} in {} and {} in {}.'.format(topic_r.iloc[0,2],topic_r.iloc[0,1],topic_r.iloc[1,2],topic_r.iloc[1,1])
                
        topic_l = topic_r_l[topic_r_l['Skill']=='2.Listening']
        r_psg=topic_l['passageID'].sum()
        if r_psg==0:
            listening_stmt='{} listened to no audio exercises.'.format(name)    
        elif r_psg==1:
            listening_stmt='{} listened to {} audio exercise.'.format(name,r_psg)
        else:
            listening_stmt='{} listened to {} audio exercises'.format(name,r_psg)
          
        topic_l=topic_l.sort_values(['passageID'],ascending=False).iloc[0:2]

        if len(topic_l)==2 :
            pl=list(topic_l['passageID'])
            if (pl[0]>=2) and (pl[1]>=2):
                listening_stmt = listening_stmt+', which included {} in {} and {} in {}.'.format(topic_l.iloc[0,2],topic_l.iloc[0,1],topic_l.iloc[1,2],topic_l.iloc[1,1])
                 
        topic_g = topic2.loc[topic2['Skill']=='3.Grammar',[ 'subskillname', 'qcode']]    
        r_psg=topic_g['qcode'].sum()
        if r_psg==0:
            grammer_stmt='{} answered no grammar questions.'.format(name)    
        elif r_psg==1:
            grammer_stmt='{} answered {} grammar questions.'.format(name,r_psg)
        else:
            grammer_stmt='{} answered {} grammar questions'.format(name,r_psg)

        topic_g=topic_g.sort_values(['qcode'],ascending=False).iloc[0:3]

        if len(topic_g)==3 :
            pl=list(topic_g['qcode'])
            if (pl[0]>=2) and (pl[1]>=2) and (pl[2]>=2):
                grammer_stmt = grammer_stmt+', which included {} in {}, {} in {}, and {} in {}.'.format(topic_g.iloc[0,1],topic_g.iloc[0,0],topic_g.iloc[1,1],topic_g.iloc[1,0],topic_g.iloc[2,1],topic_g.iloc[2,0])
                 
        topic_v = topic2.loc[topic2['Skill']=='4.Vocabulary',[ 'subskillname', 'qcode']]    
        r_psg=topic_v['qcode'].sum()
        if r_psg==0:
            vocabulary_stmt='{} answered no vocabulary questions.'.format(name)    
        elif r_psg==1:
            vocabulary_stmt='{} answered {} vocabulary questions.'.format(name,r_psg)
        else:
            vocabulary_stmt='{} answered {} vocabulary questions'.format(name,r_psg)

        topic_v=topic_v.sort_values(['qcode'],ascending=False).iloc[0:3]

        if len(topic_v)==3 :
            pl=list(topic_v['qcode'])
            if (pl[0]>=2) and (pl[1]>=2) and (pl[2]>=2):
                vocabulary_stmt = vocabulary_stmt+', which included {} in {}, {} in {}, and {} in {}.'.format(topic_v.iloc[0,1],topic_v.iloc[0,0],topic_v.iloc[1,1],topic_v.iloc[1,0],topic_v.iloc[2,1],topic_v.iloc[2,0])

        p_df_t= p_df[(p_df['passageID']==0) & (p_df['qTemplate'].isin(['mcq', 'blank'])) & (p_df['qtype']!='openEnded') ]
        p_df_t=p_df_t[p_df_t['correct']==0]
        p_df_t=p_df_t[~p_df_t.quesSubType.str.contains('audio', case=False, na=False)]
        p_df_t=p_df_t[~p_df_t.quesSubType.str.contains('image', case=False, na=False)]
        p_df_t=p_df_t[~p_df_t.optSubType.str.contains('image', case=False, na=False)]
        p_df_t=p_df_t[~p_df_t.optSubType.str.contains('image', case=False, na=False)]


        p_df_t = p_df_t.merge(class_qn_df,on=['childClass','qcode'])


        p_df_t = p_df_t[p_df_t['acc']>70]
        p_df_t = p_df_t.sort_values(['acc'],ascending=False).iloc[0:3]


        def cleanhtml(raw_html):
            cleanr = re.compile('<.*?>')
            #print(raw_html)
            cleantext = re.sub(cleanr, '', raw_html)
            cleantext=cleantext.replace('&nbsp;',' ')
            return cleantext


        def get_qn_html(i,p_df_t):       
            if p_df_t['qTemplate'][i]=='blank':
                b_df = blank_qn_ans_df[blank_qn_ans_df['qcode']==p_df_t['qcode'][i]]
                b_df = b_df.sort_values('paramName')
                n=len(b_df)
                u_ans_txt = "{}'s answer: ".format(name)
                c_ans_txt = "Correct answer: "

                for x in range(n):
                    #if 'Blank' in b_df.iloc[0,1]:
                    #    c_ans_txt = c_ans_txt+ str(x+1)+') '+str(b_df.iloc[x,2])+ ' '
                    #else:
                    c_ans_txt = c_ans_txt+ str(x+1)+') '+str(b_df.iloc[x,2].split(';')[0])+ ' '

                x=1
                lt=p_df_t['userResponse'][i].split('|')
                lt=lt[:-1]
                for z in lt:
                    if len(z.split(':'))==2:
                        u_ans_txt = u_ans_txt + str(x)+') '+str(z.split(':')[1])+ ' '
                        x=x+1
                    
                #qn_txt= cleanhtml(p_df_t['quesText'][i])
                qn_txt= p_df_t['quesText'][i]
                qn_txt = str(i+1)+'. '+qn_txt
                u_ans_txt= '<span style="color:red">'+u_ans_txt+'</span>'
                c_ans_txt= '<span style="color:green">'+c_ans_txt+'</span>'
                #explanation= cleanhtml(p_df_t['explanation'][i])
                explanation= p_df_t['explanation'][i]
                explanation = 'Explanation: '+explanation
                html = '<p style="font-size: 16px;margin-left:4%;margin-right:4%;text-align: justify;text-justify: inter-word;"><br>'+qn_txt+'<br><br>'+u_ans_txt+'  &nbsp; &nbsp;&nbsp; &nbsp;  &nbsp;&nbsp; &nbsp;  '+c_ans_txt+ '<br><br>'+  explanation+'</p>'

                return html
            else:
                #qn_txt= cleanhtml(p_df_t['quesText'][i])
                qn_txt= p_df_t['quesText'][i]
                qn_txt = str(i+1)+'. '+qn_txt
                
                #option_a= 'a). '+cleanhtml(p_df_t['option_a'][i])
                #option_b= 'b). '+cleanhtml(p_df_t['option_b'][i])
                #option_c= 'c). '+cleanhtml(p_df_t['option_c'][i])
                #option_d= 'd). '+cleanhtml(p_df_t['option_d'][i])
                #option_e= cleanhtml(p_df_t['option_e'][i])
                option_a= 'a). '+p_df_t['option_a'][i]
                option_b= 'b). '+p_df_t['option_b'][i]
                option_c= 'c). '+p_df_t['option_c'][i]
                option_d= 'd). '+p_df_t['option_d'][i]
                option_e= p_df_t['option_e'][i]              
                if option_e!='':
                    option_e= 'e). '+option_e       
                #option_f= cleanhtml(p_df_t['option_f'][i])
                option_f= p_df_t['option_f'][i]
                if option_f!='':
                    option_f= 'f). '+option_f
                #explanation= cleanhtml(p_df_t['explanation'][i])
                explanation= p_df_t['explanation'][i]
                explanation = 'Explanation: '+explanation
                
                if p_df_t['qtype'][i]=='multiCorrect':

                    u_ans_txt = "{}'s answer: ".format(name)
                    c_ans_txt = "Correct answer: "  
                    
                    lt= p_df_t['correctAnswer'][i].split('~')
                    for x,y in enumerate(lt):
                        c_ans_txt = c_ans_txt+ str(x+1)+') '+str(y)+ ' '
                        
                    lt= p_df_t['userResponse'][i].split('~')            
                    lt=lt[:-1]
                    for x,y in enumerate(lt):
                        u_ans_txt = u_ans_txt+ str(x+1)+') '+str(y)+ ' '
                    u_ans_txt= '<span style="color:red">'+u_ans_txt+'</span>'
                    c_ans_txt= '<span style="color:green">'+c_ans_txt+'</span>'
                    if option_e=='':
                        html = '<p style="font-size: 16px;margin-left:4%;margin-right:4%;text-align: justify;text-justify: inter-word;"><br>'+qn_txt+ '<br>'+ option_a + '<br>'+  option_b + '<br>'+  option_c + '<br>'+  option_d+'<br><br>'+u_ans_txt+'  &nbsp; &nbsp;&nbsp; &nbsp;  &nbsp;&nbsp; &nbsp;  '+c_ans_txt+ '<br><br>'+  explanation+'</p>'
                    elif option_f!='':
                        html ='<p style="font-size: 16px;margin-left:4%;margin-right:4%;text-align: justify;text-justify: inter-word;"><br>' +qn_txt+ '<br>'+ option_a + '<br>'+  option_b + '<br>'+  option_c + '<br>'+  option_d+ '<br>'+  option_e + '<br>'+  option_f + '<br><br>'+u_ans_txt+'  &nbsp; &nbsp;&nbsp; &nbsp;  &nbsp;&nbsp; &nbsp;  '+c_ans_txt+ '<br><br>'+  explanation+'</p>'
                    else:
                        html ='<p style="font-size: 16px;margin-left:4%;margin-right:4%;text-align: justify;text-justify: inter-word;"><br>' +qn_txt+ '<br>'+ option_a + '<br>'+  option_b + '<br>'+  option_c + '<br>'+  option_d+ '<br>'+  option_e + '<br><br>'+u_ans_txt+'  &nbsp; &nbsp;&nbsp; &nbsp;  &nbsp;&nbsp; &nbsp;  '+c_ans_txt+ '<br><br>'+  explanation+'</p>'
                else:    
                    correct_ans= p_df_t['correctAnswer'][i]
                    userResponse= p_df_t['userResponse'][i]
                    
                    if correct_ans=='a':
                        option_a = '<span style="color:green">'+option_a+'</span>'
                    elif correct_ans=='b':
                        option_b = '<span style="color:green">'+option_b+'</span>'
                    elif correct_ans=='c':
                        option_c = '<span style="color:green">'+option_c+'</span>'       
                    elif correct_ans=='d':
                        option_d = '<span style="color:green">'+option_d+'</span>'
                    elif correct_ans=='e':
                        option_e = '<span style="color:green">'+option_e+'</span>'        
                    elif correct_ans=='f':
                        option_f = '<span style="color:green">'+option_f+'</span>'        
                    
                    if userResponse=='a':
                        option_a = '<span style="color:red">'+option_a+'</span>'
                    elif userResponse=='b':
                        option_b = '<span style="color:red">'+option_b+'</span>'
                    elif userResponse=='c':
                        option_c = '<span style="color:red">'+option_c+'</span>'       
                    elif userResponse=='d':
                        option_d = '<span style="color:red">'+option_d+'</span>'
                    elif userResponse=='e':
                        option_e = '<span style="color:red">'+option_e+'</span>'        
                    elif userResponse=='f':
                        option_f = '<span style="color:red">'+option_f+'</span>' 
                    
                    if option_e=='':
                        html = '<p style="font-size: 16px;margin-left:4%;margin-right:4%;text-align: justify;text-justify: inter-word;"><br>'+qn_txt+ '<br>'+ option_a + '<br>'+  option_b + '<br>'+  option_c + '<br>'+  option_d+ '<br><br>'+  explanation+'</p>'
                    elif option_f!='':
                        html ='<p style="font-size: 16px;margin-left:4%;margin-right:4%;text-align: justify;text-justify: inter-word;"><br>' +qn_txt+ '<br>'+ option_a + '<br>'+  option_b + '<br>'+  option_c + '<br>'+  option_d+ '<br>'+  option_e + '<br>'+  option_f + '<br><br>'+ explanation+'</p>'
                    else:
                        html ='<p style="font-size: 16px;margin-left:4%;margin-right:4%;text-align: justify;text-justify: inter-word;"><br>' +qn_txt+ '<br>'+ option_a + '<br>'+  option_b + '<br>'+  option_c + '<br>'+  option_d+ '<br>'+  option_e + '<br><br>'+  explanation+'</p>'
                return html

        if len(p_df_t)>0:
            p_df_t = p_df_t.merge(qns_df[['qcode', 'qType', 'quesText',
               'option_a', 'option_b', 'option_c', 'option_d', 'option_e', 'option_f',
               'explanation', 'correctAnswer']],on='qcode')
            p_df_t.fillna('', inplace=True)
            qn2_html=''
            qn3_html=''

            for i in range(len(p_df_t)):
                if i==0:
                    qn1_html =  get_qn_html(i,p_df_t)
                elif i==1:
                    qn2_html =  get_qn_html(i,p_df_t)
                else:
                    qn3_html =  get_qn_html(i,p_df_t)
                    
            
            
            line= '<div style="margin-left:2%;width: 820px;height: 1px;background-color: #0a0b09;"> </div>'
            if qn2_html=='':
                qn_html = qn1_html
            elif qn3_html=='':
                qn_html = qn1_html+line+qn2_html    
            else:
                qn_html = qn1_html+line+qn2_html+line+qn3_html  
                
            env = Environment(loader=FileSystemLoader('.'))
            template = env.get_template("MSE_Monthly_Parent_Report3.html")
            language_tip='"./{}/{}_Gr{}.png"'.format(month,small_month,child_class)

            template_vars = {"qn_html":qn_html,"language_tip":language_tip,"done_well_stmt":done_well_stmt,"rec_well_stmt":rec_well_stmt,"reading_stmt":reading_stmt,"grammer_stmt":grammer_stmt,"listening_stmt":listening_stmt,"vocabulary_stmt":vocabulary_stmt,"days":days,"login_days":login_days,"is_60mineveryweek":is_60mineveryweek,"time_spent":time_spent,"qns":qns,"acc":acc,"psg":psg,"cls_avg_qn":cls_avg_qn,"cls_acc":cls_acc,"child_name":child_name,"child_class":child_class,"name":name,"month":month}
        else:
            env = Environment(loader=FileSystemLoader('.'))
            template = env.get_template("MSE_Monthly_Parent_Report2.html")
            language_tip='"./{}/{}_Gr{}.png"'.format(month,small_month,child_class)

            template_vars = {"language_tip":language_tip,"done_well_stmt":done_well_stmt,"rec_well_stmt":rec_well_stmt,"reading_stmt":reading_stmt,"grammer_stmt":grammer_stmt,"listening_stmt":listening_stmt,"vocabulary_stmt":vocabulary_stmt,"days":days,"login_days":login_days,"is_60mineveryweek":is_60mineveryweek,"time_spent":time_spent,"qns":qns,"acc":acc,"psg":psg,"cls_avg_qn":cls_avg_qn,"cls_acc":cls_acc,"child_name":child_name,"child_class":child_class,"name":name,"month":month}
        html_out = template.render(template_vars)
                             
        pdfGeneration(html_out,upid)
        upload_to_aws(upid,usertype)
        students_done.append(upid)
        print('{} completed'.format(upid))
    except Exception as e:
        print('{} failed due to {}'.format(upid,e))
        traceback.print_exc()
        students_notdone.append(upid)
        #logging.exception('{} failed due to {}'.format(upid,e))
                
                




t1 = datetime.datetime.now() 
mon_last_day_str=sys.argv[1]
mon_last_day = datetime.datetime.strptime(mon_last_day_str,'%Y-%m-%d')
month=mon_last_day.strftime("%B")
small_month=mon_last_day.strftime("%b")
days=mon_last_day.day
dir = 'MSE_parentReport_monthly_'+month
#dir_b2b = 'MSE_parentReport_monthly_b2b_'+month




usertypes=['b2b2c_b2c','b2b']
#usertypes=['b2b']

for usertype in usertypes:
    usage = pd.read_csv('usage_{}{}.csv'.format(usertype,my_id))

    topic_prog = pd.read_csv('topic_{}{}.csv'.format(usertype,my_id))
    class_df = pd.read_csv('class_df.csv'.format(my_id))
    class_qn_df = pd.read_csv('class_qn_df.csv'.format(my_id))
    qns_df = pd.read_csv('qns_df.csv'.format(my_id))
    blank_qn_ans_df = pd.read_csv('blank_qn_ans_df.csv'.format(my_id))


    manager = Manager()
    usertype_v = manager.list()
    usertype_v.append(usertype)

    students_notdone = manager.list()
    no_usage = manager.list()
    students_done = manager.list()

    print('total {} students {}'.format(usertype,len(usage['upid'].unique())))

    Parallel(n_jobs=-1)(delayed(get_topics_covered)(i,topic_prog,usage,class_df,class_qn_df,qns_df,blank_qn_ans_df,students_notdone,no_usage,students_done,usertype_v) for i in usage['upid'].unique())
    students_notdone=list(students_notdone)
    no_usage=list(no_usage)
    students_done=list(students_done)
    #pd.DataFrame({'upid':students_notdone}).to_csv('not_done_{}_students{}.csv'.format(usertype,my_id),index=False)
    df=pd.DataFrame({'upid':students_notdone})
    df.to_csv('not_done_{}_students{}.csv'.format(usertype,my_id),index=False)
    
    df=pd.DataFrame({'upid':no_usage})
    df.to_csv('no_usage_{}_students{}.csv'.format(usertype,my_id),index=False)
    
    df=pd.DataFrame({'upid':students_done})
    df.to_csv('done_{}_students{}.csv'.format(usertype,my_id),index=False)    

    print('not_done{} upid count {}'.format(usertype,len(students_notdone)))
    print('no_usage {} upid count {}'.format(usertype,len(no_usage)))
    print('done {} upid count {}'.format(usertype,len(students_done)))

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

time.sleep(20)
ec2 = boto3.client('ec2',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)

ec2.terminate_instances(InstanceIds=[my_id])