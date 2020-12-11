# -*- coding: utf-8 -*-
"""
Created on Fri Jun 12 14:02:41 2020

@author: EILAP6621

auto launch EC2 instance
"""

import boto3
import time
import os
aws_access_key_id= 'AKIA4TXJH6XFLQANY3FQ'
aws_secret_access_key= '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'
ec2 = boto3.resource('ec2',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)

# dynamic instances
# maxCount = int(input("Enter number of instances to be launched"))
maxCount = 1

# create a new EC2 instance

instances = ec2.create_instances(
     ImageId='ami-0cb6b502ec7875181',
     MinCount=1,  
     MaxCount=maxCount,  # number of instance to be launched
#     InstanceType='t2.micro',
     InstanceType='t3.2xlarge',
     KeyName='autoLaunchKeypair'
 )
print(instances)

l_instances = [ec2_instance.instance_id for ec2_instance in instances]  
print(l_instances)  
# get instance ID
#print(instances[0].instance_id)

#print(instances[0])
#time.sleep(120)
'''

#l_instances=['i-085b9c191332921be', 'i-09975d798721830c8', 'i-0cf70af415fd45434', 'i-072bb615608820ca2', 'i-0a1c9bdebe2ea82ef', 'i-0fe3b18103a000579', 'i-02ad4156e8313aa2a', 'i-0d7b5544d310a2ae4']
#l_instances=['i-042569a867acc400f', 'i-0ecdd791b0a220faa', 'i-07620d79f53a97bfe', 'i-029158e9877578b2d', 'i-0d7383c24a0dd0566', 'i-04ecd8227be8690df', 'i-094a4326655f473a9', 'i-00d46359afd29f27d']
#ec2 = boto3.client('ec2',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
response=ec2.describe_instances()
#print(response)

d_instances={}
for x in response['Reservations']:
    for y in x['Instances']:
        if y['InstanceId'] in l_instances:
            d_instances[y['InstanceId']]=y['PublicDnsName']
print(d_instances)       


for k,v in d_instances.items():
    os.system('scp -i ~/Parent_reports/autoLaunchKeypair.pem ~/Parent_reports/usage{}.csv  ec2-user@{}:/home/ec2-user/'.format(k,v))

#os.system('scp "%s" "%s:%s"' % (localfile, remotehost, remotefile) )'''
#ec2.reboot_instances(InstanceIds=l_instances)

#os.system('ssh -o StrictHostKeyChecking=no -i ~/Parent_reports/autoLaunchKeypair.pem  ec2-user@{} python /home/ec2-user/process.py'.format('ec2-13-235-60-16.ap-south-1.compute.amazonaws.com'))

# Stop EC2 Instances
#ec2.stop_instances(InstanceIds=l_instances)

#time.sleep(120)

# start EC2 Instances
#ec2.start_instances(InstanceIds=l_instances)

#ec2.terminate_instances(InstanceIds=l_instances)