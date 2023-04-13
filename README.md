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
=========================================================================================================
import os
import sys
import json
from awsglue.utils import getResolvedOptions
from ReduceReuseRecycle import load_log_config, get_timestamp, InvalidStatus, delete_files
import boto3
import logging.config
import logging
import re
import sys
import json
import time
import subprocess
from datetime import datetime
from dateutil import tz
import zipfile
import os

logger = load_log_config(glue=True)
ARGS = getResolvedOptions(sys.argv, ['metadata_dict'])
logger.info(f'\n**** Argument List ****\n-----------------------')
logger.info(f'ARGS: {ARGS}')
logger.info(f'\n-----------------------\n')

metadata_dict = ARGS.get('metadata_dict', '{}')
metadata_dict = json.loads(metadata_dict)
etl_stp_parms = metadata_dict.get('etl_stp_parms', "{}")
logger.info(f"etl_stp_parms: {json.loads(etl_stp_parms)}")
etl_stp_parms= json.loads(etl_stp_parms)

etl_sfn_parms = metadata_dict.get('etl_sfn_parms', "{}")
logger.info(f"etl_sfn_parms: {json.loads(etl_sfn_parms)}")
etl_sfn_parms= json.loads(etl_sfn_parms)

s3Client = boto3.client('s3')
s3Resource = boto3.resource('s3')
tm_frmt= ['%Y%m%d', '%Y%m%d%H%M%S', '%Y%m', '%Y', '%m%d']

def main():
    """
    Copy Data from Source S3 location to Target S3 location
    """
    global etl_stp_parms, etl_sfn_parms
    etl_stp_parms = parms_override(etl_stp_parms, etl_sfn_parms)
    logger.info(f'etl_stp_parms after override: {etl_stp_parms}')
    s3_tos3_copy(stp_parms= etl_stp_parms)

def s3_tos3_copy(stp_parms= etl_stp_parms):
    src_bkt = etl_stp_parms.get('src_bkt', 'na')
    src_key = etl_stp_parms.get('src_key', 'na')
    src_key_sufx = etl_stp_parms.get('src_key_sufx', '')

    include = etl_stp_parms.get('src_include', ["*"])
    exclude = etl_stp_parms.get('src_exclude', ["*"])
    trgt_bkt = etl_stp_parms.get('trgt_bkt', 'na')
    trgt_key = etl_stp_parms.get('trgt_key', 'na')
    trgt_key_sufx = etl_stp_parms.get('trgt_key_sufx', '')

    src_del_flag = etl_stp_parms.get('src_del_flag', 'n')
    trgt_del_flag = etl_stp_parms.get('trgt_del_flag', 'n')
    try:
        for i in range(len(include)):
            if any(x in include[i].replace('_','').replace('-','') for x in tm_frmt):
                utc_tm, est_tm, est_tm2 = get_timestamp(utc_format_string=include[i], est_format_string=include[i])
                include[i]= est_tm

        for i in range(len(exclude)):
            if any(x in exclude[i].replace('_','').replace('-','') for x in tm_frmt):
                utc_tm, est_tm, est_tm2 = get_timestamp(utc_format_string=src_key_sufx, est_format_string=src_key_sufx)
                exclude[i]= est_tm
        logger.info(f'include values: {include}')
        logger.info(f'exclude values: {exclude}')
        if src_key_sufx and src_key_sufx.replace('_','').replace('-','') in tm_frmt:
            utc_tm, est_tm, est_tm2 = get_timestamp(utc_format_string=src_key_sufx, est_format_string=src_key_sufx)
            src_key_sufx= est_tm
        if trgt_key_sufx and trgt_key_sufx.replace('_','').replace('-','') in tm_frmt:
            utc_tm, est_tm, est_tm2 = get_timestamp(utc_format_string=trgt_key_sufx, est_format_string=trgt_key_sufx)
            trgt_key_sufx= est_tm
        if trgt_del_flag == 'y':
            delete_files(logger, trgt_bkt, trgt_key)
        sync_s3_files(logger, src_bkt+ '/'+ src_key+ '/' + src_key_sufx, trgt_bkt+ '/'+ trgt_key+ '/' + trgt_key_sufx, include, exclude)
        if src_del_flag == 'y':
            delete_files(logger, src_bkt, src_key)
    except Exception as error:
        logger.error(error)
        raise error

def sync_s3_files(logger, s3_source_location, s3_target_location, include: [], exclude: []):
    """
    Function to use AWS CLI S3 Sync to move objects between buckets
    :param logger:
    :param s3_source_location: S3 Bucket Name of Inbound Data
    :param s3_target_location: S3 Key of data to be copied
    """
    command = f"aws s3 sync s3://{s3_source_location} s3://{s3_target_location} --force-glacier-transfer"
    include= ' '.join(['--include ' + sub for sub in include])
    exclude= ' '.join(['--exclude ' + sub for sub in exclude])
    command= command+ ' ' + exclude+ ' ' + include 

    logger.info(f"Syncing source location s3://{s3_source_location} with target s3://{s3_target_location} ")

    if s3_target_location != 'None':
        try:
            run_command(logger, command)

        except Exception as error:
            logger.error(error)
            raise InvalidStatus("Sync s3 failed")

def run_command(log, command):
    """
    Function to execute a subprocess.run command
    :param log: basic logger
    :param command: string of command to be run
    :return: Result if successful
    """
    if isinstance(command, str):
        command = command.split()
    try:
        log.info(f"Running command: \"{' '.join(command)}\"")
        result = subprocess.run(command, stdout=subprocess.PIPE)
        log.info(f'result: {result}')
    except Exception as error:
        log.error(f"*** Failed to run last command: *** \n{' '.join(command)}")
        log.error(format(error))
        raise error
    if result.returncode != 0:
        log.error(f"*** Command failed with exit code: {result.returncode} ***")
    return result

def parms_override(stp_parms, sfn_parms):
    '''
    Function to compare two dictionaries and updates the stp_parms.
    stp_parms is etl_stp_parms/etl_sfn_parms/Adhoc Parms received from input
    '''
    logger.info(f"stp_parms is: {stp_parms}")
    logger.info(f"sfn_parms is: {sfn_parms}")

    if bool(sfn_parms) == True:
        for i in sfn_parms:
            if type(sfn_parms[i]) is dict:
                for j in sfn_parms[i]:
                    stp_parms[i][j]=sfn_parms[i][j]
            else:
                stp_parms[i]=sfn_parms[i]
    return stp_parms

if __name__ == "__main__":
    main()
