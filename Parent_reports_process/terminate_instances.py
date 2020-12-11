# -*- coding: utf-8 -*-
"""
Created on Fri Jun 12 14:13:21 2020

@author: EILAP6621
"""

import s3fs
from ec2_metadata import ec2_metadata
import s3fs
import json
import pandas as pd

my_id = ec2_metadata.instance_id

data = {"instance_id":ec2_metadata.instance_id,
"region":ec2_metadata.region,
"account_id":ec2_metadata.account_id,
"ami_id":ec2_metadata.ami_id,
"instance_profile_arn":ec2_metadata.instance_profile_arn,
"instance_type":ec2_metadata.instance_type,
"security_groups":ec2_metadata.security_groups}

data_to_file = pd.DataFrame.from_dict(data)

print(data_to_file)
# write to file
aws_access_key_id= 'AKIA4TXJH6XFLQANY3FQ'
aws_secret_access_key= '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'
region_name='ap-south-1'
read_bucket= "ei-testbucket/ec2_launch_test"

file = "ei-testbucket/ec2_launch_test/ec2_metadata.json"
print(file)

try:
    print("Write to file")
    bytes_to_write = data_to_file.to_json(orient='records').encode()
    # bytes_to_write = json.dumps(data_to_file)
    fs = s3fs.S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)
    with fs.open(file, 'wb') as f:
        f.write(bytes_to_write)
except Exception as e:
    print(e)
import boto3

#ec2 = boto3.client('ec2',region_name='ap-south-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
#print(my_id)
#ec2.terminate_instances(InstanceIds=[my_id])
