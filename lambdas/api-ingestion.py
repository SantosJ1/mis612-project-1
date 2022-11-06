import json
import requests
import pandas as pd
from datetime import datetime
import boto3
import os

def write_data_to_s3(file_name, bucket, object_name):
    try:
        session = boto3.session.Session()
    finally: print("Session Established...")
    try:
        s3 = session.resource("s3")
    finally: print("Connected to S3...")
    try:
        print("Uploading file to s3...")
        result = s3.Bucket(bucket).upload_file(file_name,object_name)
        print("File uploaded to s3.")
    except Exception as e:
        raise e

def call_api(table):
    access_key = os.environ['access_key']
    endpoint = f'https://data.nasdaq.com/api/v3/datatables/ZILLOW/{table}?&api_key={access_key}'
    response = requests.get(endpoint).json()
    return response, table

def build_df(response):
    total_columns = len(response["datatable"]["columns"])
    response_data = response["datatable"]["data"]
    column_name_list = []
    for i in range(total_columns):
        column_name_list.append(response["datatable"]["columns"][i]["name"])
    data_df = pd.DataFrame(data=response_data,columns=column_name_list)
    return data_df

def define_variables():
    timestamp = str(datetime.now().strftime("%H%M%S"))
    date = datetime.now().date()
    table_list = ['DATA','INDICATORS','REGIONS']
    bucket = 'raw-market-data-ingestion'
    return bucket,table_list,date,timestamp

def main():
    bucket,table_list,date,timestamp = define_variables()
    for table in table_list:
        file_name = f'/tmp/raw_zillow_{table}_api_{timestamp}utc.csv'
        object_name = f'api/{table}/{date}/{file_name[5:]}'
        response, table = call_api(table)
        data_df = build_df(response)
        data_df.to_csv(file_name)
        write_data_to_s3(file_name, bucket, object_name)

def lambda_handler(event, context):
    main()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
