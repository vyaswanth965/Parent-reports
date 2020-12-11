# -*- coding: utf-8 -*-
"""
Created on Fri Jun 12 14:02:41 2020

@author: EILAP6621

auto launch EC2 instance
"""

import boto3

aws_access_key_id= 'AKIA4TXJH6XFLQANY3FQ'
aws_secret_access_key= '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'
ec2 = boto3.resource('ec2',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)

# dynamic instances
# maxCount = int(input("Enter number of instances to be launched"))
maxCount = 1

# create a new EC2 instance
instances = ec2.create_instances(
     ImageId='ami-0418b263587ac2161',
     MinCount=1,  
     MaxCount=maxCount,  # number of instance to be launched
     InstanceType='t2.micro',
     KeyName='autoLaunchKeypair'
 )

# get instance ID
print(instances[0].instance_id)
