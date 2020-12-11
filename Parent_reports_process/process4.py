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
#sudo yum install python36-devel postgresql-devel
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

def upload_to_aws(grade,upid):
    s3 = boto3.client('s3',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key) 
    local_file = 'reports/{}.pdf'.format(upid)
    bucket = 'ei-marketingdata'
    s3_file = '{}/{}.pdf'.format(dir,upid)
    s3.upload_file(local_file, bucket, s3_file, ExtraArgs={'ACL':'public-read','ContentType': 'application/pdf'})

        
def pdfGeneration(html_out,grade,upid):
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
    os.remove('{}.jpg'.format(upid))
    

def save_bar_chart(upid,data,colors):
    fig,ax=plt.subplots(figsize=(8,2))
    bar=plt.bar(days, data,color=colors,width=0.5, alpha=0.6)
    plt.axhline(15,linewidth=1, color='grey',linestyle='--')
    #plt.ylabel('Minutes of Usage')
    #plt.xlabel('Days')
    plt.xticks(fontsize =12)
    #ax.set_xticks([], minor=True)
    ax.yaxis.set_visible(False)    
    #ax.xaxis.set_visible(False)    
    #ax.plot([-.5], [15], 'o',color='grey')
    #ax.plot([6.5], [15], 'o',color='grey')
    #ax.text(0, 16, 'Recommended Usage: 15 mins/day',fontsize=10,fontweight='bold',color='b')
    #ax.text(-0.45, 16, 'Recommended Usage:',fontsize=12,color='black')
    #ax.text(1.5, 16, '15 mins/day',fontsize=12,fontweight='bold',color='black')
    ax.tick_params(axis=u'both', which=u'both',length=0)
    #ax.text(0.5, 0.5, 'matplotlib', rotation=90,horizontalalignment='center',verticalalignment='center', transform=ax.transAxes)
    plt.autoscale(enable=True, axis=u'both')   
    xrng=plt.xlim()
    yrng=plt.ylim()
    #ymax=yrng[1]+(yrng[1]/9)
    ymax=yrng[1]+60
    yrng=(yrng[0],ymax)
    im = image.imread(r"{}/Parent Report_files/absent2.png".format(cwd))
    for rect in bar:
        height = rect.get_height()
        if height==0:
            ax.imshow(im, aspect='auto', extent=((rect.get_x() + rect.get_width()/2.0)-0.2,(rect.get_x() + rect.get_width()/2.0)+.2,1,ymax/5) )

        elif height!=0.01:
            #plt.text(rect.get_x() + rect.get_width()/2.0, height/2, '%d' % int(height), ha='center', va='bottom',size=12)
            plt.text(rect.get_x() + rect.get_width()/2.0, height, '%d' % int(height), ha='center', va='bottom',size=12)

    
    plt.xlim(xrng)
    plt.ylim(yrng)
    plt.tight_layout()
    #ax.set_axis_off()
    #plt.setp( ax.get_xticks(), visible=False)
    b=Bbox.from_extents(.45,0,7.6,1.83) # left, bottom, right and top.

    fig.savefig('{}.jpg'.format(upid),bbox_inches=b)
    plt.close(fig)



def get_topics_covered(upid,topic_prog,usage,super_df,students_notdone,students_done,no_usage):
    try:
        #p_df= topic_prog.loc[topic_prog['upid']==upid,['upid','contentname','weekly_ques_attempts','progress_of_first_attempt','total_correct_ques_attempts'	,'total_ques_attempts'	,'total_accuracy']]
        p_df= topic_prog.loc[topic_prog['upid']==upid,['upid','contentname','weekly_ques_attempts','progress_of_first_attempt','weekly_correct_ques_attempts','weekly_accuracy']]
        u_df= usage[usage['upid']==upid].reset_index()        
   
        if len(u_df)>1:
            print(upid+' duplicate rows in usage')
            u_df=u_df.groupby(['upid']).agg({'child_name': 'min','class':'min','gender':'min', 'weekly_correct_ques_attempts':'sum','weekly_ques_attempts' : 'sum','usage_time_fri': 'sum','usage_time_sat': 'sum','usage_time_sun': 'sum','usage_time_mon': 'sum','usage_time_tue': 'sum','usage_time_wed': 'sum','usage_time_thu': 'sum'}).reset_index()
            u_df['weekly_accuracy']=u_df['weekly_correct_ques_attempts']/u_df['weekly_ques_attempts']
            u_df['weekly_accuracy']=u_df['weekly_accuracy']*100
            u_df['weekly_accuracy']=u_df['weekly_accuracy'].astype('int')
            #p_df=p_df.groupby(['upid','contentname']).agg({'progress_of_first_attempt': 'max','weekly_ques_attempts':'sum', 'total_correct_ques_attempts':'sum','total_ques_attempts' : 'sum'}).reset_index()
            #p_df['total_accuracy']=p_df['total_correct_ques_attempts']/p_df['total_ques_attempts']
            #p_df['total_accuracy']=p_df['total_accuracy']*100
            #p_df['total_accuracy']=p_df['total_accuracy'].astype('int')
            p_df=p_df.groupby(['upid','contentname']).agg({'progress_of_first_attempt': 'max','weekly_ques_attempts':'sum', 'weekly_correct_ques_attempts':'sum'}).reset_index()
            p_df['weekly_accuracy']=p_df['weekly_correct_ques_attempts']/p_df['weekly_ques_attempts']
            p_df['weekly_accuracy']=p_df['weekly_accuracy']*100
            p_df['weekly_accuracy']=p_df['weekly_accuracy'].astype('int')
        #p_df.drop(['upid','total_ques_attempts','total_correct_ques_attempts'],axis=1,inplace=True)   
        p_df.drop(['upid','weekly_correct_ques_attempts'],axis=1,inplace=True)   
        child_name=str(u_df['child_name'][0])
        child_class=u_df['class'][0]
        l=child_name.split()
        for i in l:
            if len(i)>2:
                name=i
                break
        else:
            name=child_name
            #if l=='Om':
            #    name=child_name
            #else:
                #name=child_name
            #    return 0
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
            
        weekly_accuracy = int(u_df['weekly_accuracy'][0])
        weekly_ques_attempts = u_df['weekly_ques_attempts'][0]
        data =list(u_df.loc[0,['usage_time_fri','usage_time_sat','usage_time_sun','usage_time_mon','usage_time_tue','usage_time_wed','usage_time_thu']]/60)
        
        data = [0.01 if math.isnan(i) else int(i) for i in data]
        
        total_usage = reduce(lambda a,b:a+b,data)  
        total_usage = int(total_usage)
        if total_usage==0:
            no_usage.append(upid)
            print('{} nousage'.format(upid))
            return 0
        colors = [ 'g' if i >15 else 'y' for i in data]
        save_bar_chart(upid,data,colors)
        #p_df =p_df.rename(columns={'contentname':'Topics Attempted','weekly_ques_attempts':'Questions done','progress_of_first_attempt':'Progress','total_accuracy':'Accuracy'})
        p_df =p_df.rename(columns={'contentname':'Topics Attempted','weekly_ques_attempts':'Questions done','progress_of_first_attempt':'Progress','weekly_accuracy':'Accuracy'})
        p_df['Topics Attempted'] = p_df['Topics Attempted'].str.lower()
        p_df['Topics Attempted'] = p_df['Topics Attempted'].str.capitalize()

        t_df=p_df.loc[(p_df['Progress']>50) & (p_df['Accuracy']<60)].reset_index()
        topic_recom=''

        if len(t_df)>0:
            t_df=t_df.iloc[t_df['Accuracy'].idxmin()]
            topic_recom=' {} is struggling in the topic {}.'.format(name.capitalize(),t_df['Topics Attempted'])
            if t_df['Progress']==100:
                topic_recom= topic_recom+' {} should revise this topic.'.format(name.capitalize())
            else:
                topic_recom= topic_recom+' {} should revisit the explanation of questions for this topic.'.format(name.capitalize())
        p_df['Progress']=p_df['Progress'].astype('str')+'%'
        p_df['Accuracy']=p_df['Accuracy'].astype('int')
        p_df['Accuracy']=p_df['Accuracy'].astype('str')+'%'
        r=len(p_df)
        
        p_df['x']=p_df['Topics Attempted'].str.len()//47
        r_extra = p_df['x'].sum()
        p_df.drop(['x'],axis=1,inplace=True)   
        
        r=r+r_extra
        tb_hgt=115+r*28
        tb_hgt=str(tb_hgt)+'px'
        if r<=10:
            rpt_hgt='1150px'
        else:
            rpt_hgt=1150+(r-10)*27
            rpt_hgt=str(rpt_hgt)+'px'
        p_html=p_df.to_html(index=False)
        p_html=p_html.replace('<th>Topics','<th style="padding:2px;text-align: left;">Topics')
        p_html=p_html.replace('<th>Questions','<th style="padding:2px;text-align: centre;">Questions')
        p_html=p_html.replace('<th>Progress','<th style="padding:2px;text-align: centre;">Progress')
        p_html=p_html.replace('<th>Accuracy','<th style="padding:2px;text-align: centre;">Accuracy')
        p_html=p_html.replace('class="dataframe"',' class="dataframe" width="800px"   cellspacing="0" ')
        p_html=p_html.replace('style="text-align: right;"','')
                                                                                  
        p_html=p_html.replace('<tr>\n      <td','<tr>\n      <td style="padding:2px;text-align: left;"')
        p_html=p_html.replace('<tr','<tr style="page-break-inside: avoid;"')
        p_html=p_html.replace('<td>','<td style="padding:2px;text-align: centre;">')


        
        if u_df['gender'][0]=='Girl':
            pronoun = 'She'
        else:
            pronoun = 'He'
#        if total_usage>=90:
#            recommendation="{}'s total usage was {} minutes for this week. {} is diligently using Mindspark.".format(child_name,total_usage,pronoun)
#        else:
#            recommendation="{}'s total usage was {} minutes for this week. {} should spend {} more mins every week to achieve proficiency in Maths subject.".format(child_name,total_usage,pronoun,105-total_usage)
        
        if total_usage>=90:
            usg_flag='high'
        else:
            usg_flag='low'
            
        days_login = len(list(filter(lambda x:x>=1,data)))
        if days_login>=7:
            days_flag='high'
        else:
            days_flag='low'   
        
        if (usg_flag=='high') and ( days_flag=='high'):
            recommendation="{}'s total usage was {} minutes for this week.  {} is diligently using Mindspark.   {} should continue doing the same to achieve proficiency in Maths.".format(name.capitalize(),total_usage,name.capitalize(),name.capitalize())
        elif (usg_flag=='high') and ( days_flag=='low'):
            recommendation="Consistency is the key ! . Encourage {} to practice Maths lessons regularly on Mindspark.".format(name.capitalize())
        elif (usg_flag=='low') and ( days_flag=='low'):
            recommendation="Encourage {} to practice Maths lessons regularly on Mindspark. {}'s total usage was {} minutes for this week.  {} should spend 90 minutes every week on Mindspark to achieve proficiency in Maths.".format(name.capitalize(),name.capitalize(),total_usage,name.capitalize())
        else:
            recommendation="Encourage {} to spend more time on Mindspark. {}'s total usage was {} minutes for this week.  {} should spend 90 minutes every week on Mindspark to achieve proficiency in Maths.".format(name.capitalize(),name.capitalize(),total_usage,name.capitalize())
        
        super_recom=''
        if len(super_df)>0:
            grade_test = super_df[['class','testName','qns_cnt']].drop_duplicates()
            if (len(u_df['newupid'][0])<10) and (child_class in [4,5,6,7,8,9,10]):
                s_df= super_df[super_df['upid']==upid].reset_index()
                if len(s_df)==0:
                    grade_test = grade_test[grade_test['class']==child_class].reset_index()
                    testName = grade_test['testName'][0]
                    qns_cnt = grade_test['qns_cnt'][0]
                    super_recom="{} scored 00/{} in the Test on {} this week. {} should not miss test for better retention of completed topic.".format(name.capitalize(),qns_cnt,testName,name.capitalize())
                elif s_df['correct_qns'][0]/s_df['qns_cnt'][0]<0.8:
                    super_recom="{} scored {}/{} in the Test on {} this week. {} should revise the topic {} with at least 80% accuracy.".format(name.capitalize(),int(s_df['correct_qns'][0]),s_df['qns_cnt'][0],s_df['testName'][0],name.capitalize(),s_df['topicName'][0])
                else :
                    super_recom="{} scored {}/{} in the Test on {} this week. {} performance is upto the mark.".format(name.capitalize(),int(s_df['correct_qns'][0]),s_df['qns_cnt'][0],s_df['testName'][0],name.capitalize())
                
        if (super_recom=='') and (topic_recom!=''):
            topic_recom='2. '+topic_recom
        elif (super_recom!='') and (topic_recom==''):
            super_recom='2. '+super_recom
        elif (super_recom!='') and (topic_recom!=''):   
            topic_recom='2. '+topic_recom
            super_recom='3. '+super_recom
        else:
            pass
        if (super_recom!='') or (topic_recom!=''):
            recommendation='1. '+recommendation
            
        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template("Parent Report.html")
        if len(p_df)>0:
            if topic_recom=='':
                template_vars = {"rpt_hgt":rpt_hgt,"tb_hgt":tb_hgt,"child_name":child_name,"child_class":child_class,"name":name,"month_1":month_1,"day_1":day_1,"month_2":month_2,"day_2":day_2,"year":year,"bar_chart":'{}.jpg'.format(upid),"weekly_accuracy":weekly_accuracy,"weekly_ques_attempts":weekly_ques_attempts,"table":p_html,"recommendation":recommendation,"super_recom":super_recom}
            else:
                template_vars = {"rpt_hgt":rpt_hgt,"tb_hgt":tb_hgt,"child_name":child_name,"child_class":child_class,"name":name,"month_1":month_1,"day_1":day_1,"month_2":month_2,"day_2":day_2,"year":year,"bar_chart":'{}.jpg'.format(upid),"weekly_accuracy":weekly_accuracy,"weekly_ques_attempts":weekly_ques_attempts,"table":p_html,"recommendation":recommendation,"topic_recom":topic_recom,"super_recom":super_recom}
        else:
            if topic_recom=='':
                template_vars = {"rpt_hgt":rpt_hgt,"tb_hgt":tb_hgt,"child_name":child_name,"child_class":child_class,"name":name,"month_1":month_1,"day_1":day_1,"month_2":month_2,"day_2":day_2,"year":year,"bar_chart":'{}.jpg'.format(upid),"weekly_accuracy":weekly_accuracy,"weekly_ques_attempts":weekly_ques_attempts,"recommendation":recommendation,"super_recom":super_recom}
            else:
                template_vars = {"rpt_hgt":rpt_hgt,"tb_hgt":tb_hgt,"child_name":child_name,"child_class":child_class,"name":name,"month_1":month_1,"day_1":day_1,"month_2":month_2,"day_2":day_2,"year":year,"bar_chart":'{}.jpg'.format(upid),"weekly_accuracy":weekly_accuracy,"weekly_ques_attempts":weekly_ques_attempts,"recommendation":recommendation,"topic_recom":topic_recom,"super_recom":super_recom}
            
        html_out = template.render(template_vars)
        pdfGeneration(html_out,child_class,upid)
        upload_to_aws(child_class,upid)
        print('{} completed'.format(upid))
        students_done.append(upid)
    except Exception as e:
        print('{} failed due to {}'.format(upid,e))
        traceback.print_exc()
        students_notdone.append(upid)
        #logging.exception('{} failed due to {}'.format(upid,e))
                




t1 = datetime.datetime.now() 
#td=datetime.datetime.today()
#td=sys.argv[1]
#td = datetime.datetime.strptime(td,'%Y-%m-%d')
#this_monday_date = td - datetime.timedelta((td.weekday()) % 7)
#last_fri = this_monday_date - datetime.timedelta(3)
last_fri_str=sys.argv[1]
last_fri_str2=last_fri_str.replace('-','_')
dir = 'parentReport_weekly_'+last_fri_str2
last_fri = datetime.datetime.strptime(last_fri_str,'%Y-%m-%d')
thur = last_fri + datetime.timedelta(6)

month_1=last_fri.strftime('%B')
day_1=last_fri.day
month_2=thur.strftime('%B')
day_2=thur.day
year=last_fri.year
last_fri_str2=str(day_1)+month_1+str(year)
#year=td.year
#month_no=td.month
#week_no=int(td.strftime("%V"))
#for f in glob('usage*.csv'):


#usertypes=['b2b2c','b2b','b2c']
usertypes=['b2b2c','b2c']
super_df = pd.read_csv('super_df.csv')

for usertype in usertypes:
    usage = pd.read_csv('usage_{}_{}.csv'.format(usertype,my_id))
    topic_prog = pd.read_csv('topic_prog_{}_{}.csv'.format(usertype,my_id))
    #super_df=pd.DataFrame()
    #if usertype=='b2c':
    #super_df = pd.read_csv('super_df_{}_{}.csv'.format(usertype,my_id))


    days=['Friday','Saturday','Sunday','Monday','Tuesday','Wednesday','Thursday']

    manager = Manager()
    students_notdone = manager.list()
    no_usage = manager.list()
    students_done = manager.list()

    print('total students {}'.format(len(usage['upid'].unique())))
    Parallel(n_jobs=-1)(delayed(get_topics_covered)(i,topic_prog,usage,super_df,students_notdone,students_done,no_usage) for i in usage['upid'].unique())
    students_notdone=list(students_notdone)
    no_usage=list(no_usage)
    students_done=list(students_done)

    pd.DataFrame({'oldupid':students_notdone}).to_csv('not_done_{}_students{}.csv'.format(usertype,my_id),index=False)
    pd.DataFrame({'oldupid':no_usage}).to_csv('no_usage_{}_students{}.csv'.format(usertype,my_id),index=False)
    pd.DataFrame({'oldupid':students_done}).to_csv('done_{}_students{}.csv'.format(usertype,my_id),index=False)
    
    print('done {} students count {}'.format(usertype,len(students_done)))
    print('not_done {} students count {}'.format(usertype,len(students_notdone)))
    print('no_usage {} students count {}'.format(usertype,len(no_usage)))
    ec2 = boto3.client('ec2',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    #os.system('scp -o StrictHostKeyChecking=no -i ~/yashwanthKeyPair.pem ~/done_{}_students{}.csv  yashwanth.kumar@ec2-13-234-72-138.ap-south-1.compute.amazonaws.com:/home/yashwanth.kumar/Parent_reports/'.format(usertype,my_id))
    #os.system('scp -o StrictHostKeyChecking=no -i ~/yashwanthKeyPair.pem ~/not_done_{}_students{}.csv  yashwanth.kumar@ec2-13-234-72-138.ap-south-1.compute.amazonaws.com:/home/yashwanth.kumar/Parent_reports/'.format(usertype,my_id))
    #os.system('scp -o StrictHostKeyChecking=no -i ~/yashwanthKeyPair.pem ~/no_usage_{}_students{}.csv  yashwanth.kumar@ec2-13-234-72-138.ap-south-1.compute.amazonaws.com:/home/yashwanth.kumar/Parent_reports/'.format(usertype,my_id))
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

ec2.terminate_instances(InstanceIds=[my_id])