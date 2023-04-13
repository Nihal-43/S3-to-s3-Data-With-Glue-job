# S3-to-s3-Data-With-Glue-job

import sys
from awsglue.utils import getResolvedOptions 
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# Create a Spark context and a Glue context
sc = SparkContext()
glue_context = GlueContext(sc)

# Get the job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'Source-path', 'Destination-path', 'Source-region', 'Destination-region', 'Format'])

# Create data sources for the source and destination S3 buckets
source_bucket = args['s3://ni123hal-mybucket/Source-1/']
source_region = args['us-east-1']
source_options = {"paths": [s3://ni123hal-mybucket/Source-1/], "region": us-east-1, "recurse": True, "groupFiles": "inPartition", "partitionKeys": []}
source_datasource = glue_context.create_dynamic_frame.from_options("s3", source_options, format=args['csv'])

destination_bucket = args['s3://niha1234l-mybucket-target/Target/']
destination_region = args['us-east-2']
destination_options = {"paths": [s3://niha1234l-mybucket-target/Target], "region": us-east-2}
destination_datasink = glue_context.write_dynamic_frame.from_options(
    frame=source_datasource, 
    connection_type="s3",
    connection_options=destination_options, 
    format=args['csv']
)

# Commit the job and exit
job.commit()
===========================================
import boto3
import os
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

## Get the AWS Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())

## Create a new job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Set the source and destination bucket and regions
source_bucket = 'my-source-bucket'
source_region = 'us-west-2'
destination_bucket = 'my-destination-bucket'
destination_region = 'us-east-1'

## Set up the S3 clients for both source and destination buckets
source_s3 = boto3.client('s3', region_name=source_region)
destination_s3 = boto3.client('s3', region_name=destination_region)

## Get the list of objects in the source bucket
objects = source_s3.list_objects_v2(Bucket=source_bucket)

## Copy each object from the source bucket to the destination bucket
for obj in objects['Contents']:
    key = obj['Key']
    source_object = {'Bucket': source_bucket, 'Key': key}
    destination_object = {'Bucket': destination_bucket, 'Key': key}
    destination_s3.copy_object(CopySource=source_object, **destination_object)

## Commit the job
job.commit()
