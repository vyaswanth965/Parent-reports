import datetime  
import pandas as pd
import numpy as np
from google_ngram_downloader import readline_google_store
import json
import os
from multiprocessing import Pool
import boto3
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError
from ec2_metadata import ec2_metadata
import s3fs
from multiprocessing import Manager
import traceback
import sys

cwd="/home/ec2-user/"
os.chdir(cwd)
import db_config

my_id = ec2_metadata.instance_id
aws_access_key_id= 'AKIA4TXJH6XFLQANY3FQ'
aws_secret_access_key= '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'
region_name='ap-south-1'

df= pd.read_csv('remainingIndices{}.csv'.format(my_id))
indices = list(df['ids'])

def processngrams(index):
    length = 3
    try:
                              
        ngram_dict={}
        try:
            name, url, records = next(readline_google_store(ngram_len=length, indices=[index]))
        except:
            print('url not found')
            pass
        for record in records:
                
            if record.ngram in ngram_dict.keys():
                ngram_dict[record.ngram] = ngram_dict[record.ngram]+record.match_count
            else:
                ngram_dict[record.ngram] = record.match_count
        ngram_count={}
                
        for key,value in ngram_dict.items():
            new_key=[]
            for text in key.split():
                new_key.append(text.split('_')[0])
            new_key = ' '.join(new_key)
            if new_key in ngram_count.keys():
                ngram_count[new_key] = ngram_count[new_key]+value
            else:
                ngram_count[new_key] = value
            
            filename = str(length)+'_'+index
            filepath = filename+'.json'
        with open(filepath, 'w') as fp:
            json.dump(ngram_count, fp)              
        print(name)
        
        s3 = boto3.client('s3',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key) 
        bucket = 'ei-marketingdata'
        s3_file = 'parentReport_test/{}'.format(filepath)
        s3.upload_file(filepath, bucket, s3_file)  
        
    except Exception as e:
        print(e)

if __name__ == '__main__':
    with Pool(4) as p:
        p.map(processngrams, indices) 
        
time.sleep(20)
ec2 = boto3.client('ec2',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
ec2.terminate_instances(InstanceIds=[my_id])        