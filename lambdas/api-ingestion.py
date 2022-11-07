import requests
import pandas as pd
from datetime import datetime, timedelta
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

def call_api(table,next_cursor_id):
    # access_key = os.environ['access_key']
    access_key = 'P-eccevSsBpuzsRTZ1tf'
    if next_cursor_id == 'None':
        endpoint = f'https://data.nasdaq.com/api/v3/datatables/ZILLOW/{table}?&api_key={access_key}'
        response = requests.get(endpoint).json()
    elif next_cursor_id != 'None':
        endpoint = f'https://data.nasdaq.com/api/v3/datatables/ZILLOW/{table}?&api_key={access_key}&qopts.cursor_id={next_cursor_id}'
        response = requests.get(endpoint).json()
    return response, table

def build_df(response,next_cursor_id):
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
    return bucket,table_list,date

def main():
    bucket,table_list,date = define_variables()
    for table in table_list:
        next_cursor_id = 'None'
        response, table = call_api(table,next_cursor_id)
        if table != 'DATA':
            file_name = f'raw_zillow_{table}_api.csv'
            object_name = f'api/{table}/{file_name}'
            if response["meta"]["next_cursor_id"]:
                print(f'Table {table} continues.')
                data_df = pd.DataFrame()
                while response["meta"]["next_cursor_id"]:
                    next_cursor_id = response["meta"]["next_cursor_id"]
                    response, table = call_api(table,next_cursor_id)
                    piece_data_df = build_df(response,next_cursor_id)
                    data_df = pd.concat([data_df, piece_data_df])
                data_df.to_csv(file_name,index=False)
                # write_data_to_s3(file_name, bucket, object_name)
            else:
                print(f'Table {table} does not continue.')
                data_df = build_df(response,next_cursor_id)
                data_df.to_csv(file_name,index=False)
                # write_data_to_s3(file_name, bucket, object_name)
        elif table == 'DATA':
            file_name = f'raw_zillow_{table}_api_{date}.csv'
            object_name = f'api/{date}/{table}/{file_name}'
            date_filter = str((datetime.today().replace(day=1) - timedelta(days=1)).date())
            if response["meta"]["next_cursor_id"]:
                print(f'Table {table} continues.')
                data_df = pd.DataFrame()
                while response["meta"]["next_cursor_id"]:
                    next_cursor_id = response["meta"]["next_cursor_id"]
                    response, table = call_api(table,next_cursor_id)
                    piece_data_df = build_df(response,next_cursor_id)
                    data_df = pd.concat([data_df, piece_data_df])
                    data_df = data_df[data_df["date"] == date_filter]
                data_df.to_csv(file_name,index=False)
                # write_data_to_s3(file_name, bucket, object_name)
            else:
                print(f'Table {table} does not continue.')
                data_df = build_df(response,next_cursor_id)
                data_df.to_csv(file_name,index=False)
                # write_data_to_s3(file_name, bucket, object_name)

def lambda_handler(event, context):
    main()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
