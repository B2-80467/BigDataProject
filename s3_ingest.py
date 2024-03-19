import boto3
import pandas as pd
from OpenSSL import SSL

#checking for ssl
#from OpenSSL import SSL
# pip install pyOpenSSL
# pip install --upgrade pyOpenSSL
# update pip

s3 = boto3.resource(
    service_name =  's3',
    region_name = 'ap-south-1',


)

 # pip3 install s3fs
for bucket in s3.buckets.all():
    print(bucket.name)

#upload file

s3.Bucket('cdacprojectsunbeam').upload_file(Filename = '/home/sunbeam/Downloads/Sales.csv', Key = '/home/sunbeam/Downloads/Sales.csv')

for obj in s3.Bucket('cdacprojectsunbeam').objects.all():
    print(obj)

# Load csv file directly into python
obj = s3.Bucket('cdacprojectsunbeam').Object('part-00000-f264af3e-8bf5-4214-a773-90ebe34c6bab-c000.csv').get()
# foo = pd.read_csv(obj['Body'])
#
# print(foo)

# Download file and read from disc
# s3.Bucket('krishtest1').download_file(Key='foo.csv', Filename='foo2.csv')
# pd.read_csv('foo2.csv', index_col=0)

# for reference
# https://github.com/krishnaik06/AWS/blob/main/boto3%20read%20S3.ipynb
