import logging
import sys
import boto3
import botocore
import json
import random
import time
import os
from botocore.exceptions import ClientError
import pg8000
from config import parameters
from datetime import datetime

my_session = boto3.session.Session()
secret_name = os.environ['secret_name']
region_name = my_session.region_name
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info('region name '+region_name)

account =boto3.client('sts').get_caller_identity().get('Account')


# Connect via pg8000
def get_connection(database, host, port, user, password):
    conn = None
    try:
        conn = pg8000.connect(database=database, host=host, port=port, user=user, password=password, ssl=True)
    except Exception as err:
        print(err)
    return conn

def get_script(path):
    return open(path, 'r').read()

def getSecretPassword():
    password = "None"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
        
    try:
        logger.info(" the secret name "+secret_name)
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        
    except ClientError as e:
        print(e)
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']

            credentials = json.loads(secret)
            return credentials
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            print("password binary:" + decoded_binary_secret)
            password = decoded_binary_secret.password    
            return password

def execute_query(con,cur,query):
    
    try:
        cur.execute(query)
        cur.close()
        con.commit()
        con.close()
    except Exception as err:
        print(err)
    return 'success'

# Handler function
def lambda_handler(event, context):

    bucket_name = parameters['bucket_name']

    s3 = boto3.resource('s3')

    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))

    s3client = boto3.client('s3')
    objs = s3client.list_objects_v2(Bucket=bucket_name)['Contents']
    last_added_file = [obj['Key'] for obj in sorted(objs, key=get_last_modified)][-1]


    dataStagingPath = 's3://'+bucket_name+'/'+last_added_file

    logger.info('The added file is '+dataStagingPath)

    ## Get credentials

    logger.info("Retrieving Credentials")
   
    credentials=getSecretPassword()

    ## Form DB Connection Info


    dbname = credentials['database'] 
    host = credentials['host']
    port = int(credentials['port'])
    user = credentials['username']
    password = credentials['password']

    copy_table_stmt = """copy {schema_name}.{table_name}
                   from '{dataStagingPath}' 
                   IAM_ROLE '{iam_role}'
                   FORMAT AS {format} ;"""

    sql = copy_table_stmt.format(schema_name = parameters['schema_name']\
                                 ,table_name=parameters['table_name']\
                                 ,dataStagingPath=dataStagingPath\
                                 ,iam_role = parameters['iam_role']%(account)\
                                 ,format=parameters['format'])

    ## prepare response json

    response = dict()

    response['operation'] = 'COPY'
    response['format'] = 'PARQUET'
    response['schema_name'] = parameters['schema_name']
    response['table_name'] = parameters['table_name']
    response['file_added'] = dataStagingPath


    #Connect to Redshift

    logger.info("Connecting to DB Store")
    
    con = get_connection(dbname, host, port, user, password)

    logger.info("Connection Successful")

    response['load_start_time']=str(datetime.now())

    execution_result = execute_query(con,con.cursor(),sql)

    response['load_end_time']=str(datetime.now())

    response['load_result'] = execution_result

    response_json = json.dumps(response)

    logger.info(response_json)

    return response