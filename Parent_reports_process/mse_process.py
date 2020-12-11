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
    plt.axhline(30,linewidth=1, color='grey',linestyle='--')
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



def get_topics_covered(upid,topic_prog,usage,subskill_acc,students_notdone,students_done,no_usage):
    try:
        p_df= topic_prog.loc[topic_prog['upid']==upid]
        p_df=p_df.copy()
        u_df= usage[usage['upid']==upid].reset_index()        
        t_df= subskill_acc.loc[subskill_acc['upid']==upid]
        if len(u_df)>1:
            print(upid+' duplicate rows in usage')
            students_notdone.append(upid)
            return 0
        p_df.drop(['upid'],axis=1,inplace=True)   

        weekly_ques_attempts = int(p_df['qn_cnt'].sum())

        data =list(u_df.loc[0,['usage_time_fri','usage_time_sat','usage_time_sun','usage_time_mon','usage_time_tue','usage_time_wed','usage_time_thu']]/60)
        #data =list(u_df.loc[0,['usage_time_mon','usage_time_tue','usage_time_wed','usage_time_thu','usage_time_fri','usage_time_sat','usage_time_sun']]/60)
        data = [0.01 if math.isnan(i) else int(i) for i in data]
        total_usage = reduce(lambda a,b:a+b,data)  
        total_usage = int(total_usage)
        if (total_usage==0) or (weekly_ques_attempts==0) :
            no_usage.append(upid)
            print('{} nousage'.format(upid))
            return 0        

        child_name=str(u_df['childName'][0])
        child_class=u_df['childClass'][0]
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
        weekly_accuracy = (p_df['correct_qns_not_blank'].sum()*100)/(p_df['qn_cnt_not_blank'].sum())    
        if np.isnan(weekly_accuracy):
            weekly_accuracy=0
        else:
            weekly_accuracy = int(weekly_accuracy)

        colors = [ 'g' if i >29 else 'r' for i in data]
        save_bar_chart(upid,data,colors)
        p_df = p_df[['Skill',  'Passages - Qs Done', 'Accuracy', 'Time Spent on Questions']]
        p_html=p_df.to_html(index=False)
        p_html=p_html.replace('class="dataframe"',' class="dataframe" width="800px"   cellspacing="0" ')
        p_html=p_html.replace('<th>Skill','<th style="padding:2px;text-align: left;">&nbsp;&nbsp;Skill')
        p_html=p_html.replace('<th>Passages - Qs Done','<th style="padding:2px;text-align: centre;">Passages - Qs Done')
        p_html=p_html.replace('<th>Time Spent','<th style="padding:2px;text-align: centre;">Time Spent')
        p_html=p_html.replace('<th>Accuracy','<th style="padding:2px;text-align: centre;">Accuracy')
        p_html=p_html.replace('style="text-align: right;"','')

        p_html=p_html.replace('<tr>\n      <td>','<tr>\n      <td style="padding:2px;text-align: left;">&nbsp;&nbsp;')
        p_html=p_html.replace('<tr','<tr style="page-break-inside: avoid;"')
        p_html=p_html.replace('<td>','<td style="padding:2px;text-align: centre;">')

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

        rpt_hgt='1150px'
        tb_hgt = '220px'

        if total_usage<30:
            usage_recom="{}'s total usage was {} minutes this week. {} needs to spend at least 90 minutes a week to get comprehensive practice in English.".format(name.capitalize(),total_usage,name.capitalize())
        elif total_usage<85:
            usage_recom="{}'s total usage was {} minutes this week. {} should spend at least {} more minutes every week to get comprehensive practice in English.".format(name.capitalize(),total_usage,name.capitalize(),round(90-total_usage,-1))
        elif total_usage>120:
            usage_recom="{}'s total usage was {} minutes this week. {} is diligently using Mindspark. {} should continue doing the same to get comprehensive practice in English.".format(name.capitalize(),total_usage,name.capitalize(),name.capitalize())
        else:
            usage_recom="{}'s total usage was {} minutes this week. {} is doing well and can now try to use Mindspark more next week.".format(name.capitalize(),total_usage,name.capitalize())

        acc_recom=''
        t_df=t_df.loc[(t_df['qcode']>10) & (t_df['Accuracy']<30)].reset_index()

        if weekly_accuracy<30:
            acc_recom="2. {} can take time carefully reading the question and the explanations after answering. This will help {} to identify mistakes.".format(name.capitalize(),name.capitalize())
        elif len(t_df)>0:
            t_df=t_df.iloc[t_df['Accuracy'].idxmin()]
            acc_recom="2. {} struggled with {} this week. {} can explore resources and get more practise to improve.".format(name.capitalize(),t_df['skillname'],name.capitalize())
        elif weekly_accuracy>=70:
            acc_recom="2. {} has performed very well on Mindspark English this week. Good job!".format(name.capitalize())

        if acc_recom!='':
            usage_recom='1. '+usage_recom	
        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template("MSE_Parent_Report.html")
        template_vars = {"usage_recom":usage_recom,"acc_recom":acc_recom,"table":p_html,"rpt_hgt":rpt_hgt,"tb_hgt":tb_hgt,"child_name":child_name,"child_class":child_class,"name":name,"month_1":month_1,"day_1":day_1,"month_2":month_2,"day_2":day_2,"year":year,"bar_chart":'{}.jpg'.format(upid),"weekly_accuracy":weekly_accuracy,"weekly_ques_attempts":weekly_ques_attempts}

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
dir = 'MSE_parentReport_weekly_'+last_fri_str2
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


usage = pd.read_csv('usage_{}.csv'.format(my_id))
topic_prog = pd.read_csv('topic_prog_{}.csv'.format(my_id))
subskill_acc = pd.read_csv('subskill_acc_{}.csv'.format(my_id))

#super_df=pd.DataFrame()
#if usertype=='b2c':
#super_df = pd.read_csv('super_df_{}_{}.csv'.format(usertype,my_id))


days=['Friday','Saturday','Sunday','Monday','Tuesday','Wednesday','Thursday']
#days=['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']

manager = Manager()
students_notdone = manager.list()
no_usage = manager.list()
students_done = manager.list()

print('total students {}'.format(len(usage['upid'].unique())))
Parallel(n_jobs=-1)(delayed(get_topics_covered)(i,topic_prog,usage,subskill_acc,students_notdone,students_done,no_usage) for i in usage['upid'].unique())
students_notdone=list(students_notdone)
no_usage=list(no_usage)
students_done=list(students_done)

pd.DataFrame({'oldupid':students_notdone}).to_csv('not_done_students{}.csv'.format(my_id),index=False)
pd.DataFrame({'oldupid':no_usage}).to_csv('no_usage_students{}.csv'.format(my_id),index=False)
pd.DataFrame({'oldupid':students_done}).to_csv('done_students{}.csv'.format(my_id),index=False)

print('done  students count {}'.format(len(students_done)))
print('not_done  students count {}'.format(len(students_notdone)))
print('no_usage  students count {}'.format(len(no_usage)))
ec2 = boto3.client('ec2',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
#os.system('scp -o StrictHostKeyChecking=no -i ~/yashwanthKeyPair.pem ~/done_{}_students{}.csv  yashwanth.kumar@ec2-13-234-72-138.ap-south-1.compute.amazonaws.com:/home/yashwanth.kumar/Parent_reports/'.format(usertype,my_id))
#os.system('scp -o StrictHostKeyChecking=no -i ~/yashwanthKeyPair.pem ~/not_done_{}_students{}.csv  yashwanth.kumar@ec2-13-234-72-138.ap-south-1.compute.amazonaws.com:/home/yashwanth.kumar/Parent_reports/'.format(usertype,my_id))
#os.system('scp -o StrictHostKeyChecking=no -i ~/yashwanthKeyPair.pem ~/no_usage_{}_students{}.csv  yashwanth.kumar@ec2-13-234-72-138.ap-south-1.compute.amazonaws.com:/home/yashwanth.kumar/Parent_reports/'.format(usertype,my_id))
s3 = boto3.client('s3',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key) 
bucket = 'ei-marketingdata'
local_file = 'done_students{}.csv'.format(my_id)
s3_file = '{}/done_students{}.csv'.format(dir,my_id)
s3.upload_file(local_file, bucket, s3_file)
local_file = 'not_done_students{}.csv'.format(my_id)
s3_file = '{}/not_done_students{}.csv'.format(dir,my_id)
s3.upload_file(local_file, bucket, s3_file)
local_file = 'no_usage_students{}.csv'.format(my_id)
s3_file = '{}/no_usage_students{}.csv'.format(dir,my_id)
s3.upload_file(local_file, bucket, s3_file)                                              
t2 = datetime.datetime.now()
print('The whole thing took: '+str(round((t2-t1).total_seconds()/60 , 2))+ ' min')

time.sleep(20)

ec2.terminate_instances(InstanceIds=[my_id])