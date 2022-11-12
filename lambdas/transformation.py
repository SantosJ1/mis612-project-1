import json
import os
import boto3
import urllib.parse
import pandas as pd

def lambda_handler(event, context):
    
    def download_s3_file():
        session = boto3.Session()
        s3 = session.resource('s3')
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        obj_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        my_bucket = s3.Bucket(bucket_name)
        if 'manual' not in obj_key:
            file_name = '/tmp/dump.csv'
            s3_client = boto3.client('s3')
            s3_client.download_file(bucket_name, obj_key, file_name)
            file_df = pd.read_csv(file_name)
        else:
            file_name = '/tmp/dump.xlsx'
            s3_client = boto3.client('s3')
            s3_client.download_file(bucket_name, obj_key, file_name)
            file_df = 'None.'
        return file_df,file_name,obj_key

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
    
    def data_transformation(target_bucket):
        s3_file,file_name,obj_key = download_s3_file()
        if 'api' in obj_key:
            obj_key = obj_key.replace('api/','')
            obj_key = obj_key.replace('raw_','')
            write_data_to_s3(file_name,target_bucket,f'zillow/{obj_key}')
        if 'manual' in obj_key:
            column_list = ['Period','Crude oil, average', 'Crude oil, Brent', 'Crude oil, Dubai', 'Crude oil, WTI', 'Coal, Australian',	'Coal, South African,', 'Natural gas, US',	'Natural gas, Europe', 'Liquefied natural gas, Japan', 'Natural gas index', 'Cocoa', 'Coffee, Arabica', 'Coffee, Robusta', 'Tea, avg 3 auctions', 'Tea, Colombo', 'Tea, Kolkata', 'Tea, Mombasa', 'Coconut oil', 'Groundnuts', 'Fish meal', 'Groundnut oil', 'Palm oil', 'Palm kernel oil',	'Soybeans', 'Soybean oil', 'Soybean meal', 'Rapeseed oil', 'Sunflower oil', 'Barley', 'Maize', 'Sorghum', 'Rice, Thai 5% ',	'Rice, Thai 25%', 'Rice, Thai A.1', 'Rice, Viet Namese 5%',	'Wheat, US SRW', 'Wheat, US HRW', 'Banana, Europe', 'Banana, US', 'Orange', 'Beef', 'Meat, chicken', 'Meat, sheep', 'Shrimps, Mexican', 'Sugar, EU', 'Sugar, US', 'Sugar, world', 'Tobacco, US import u.v.', 'Logs, Cameroon', 'Logs, Malaysian', 'Sawnwood, Cameroon', 'Sawnwood, Malaysian', 'Plywood', 'Cotton, A Index', 'Rubber, TSR20', 'Rubber, SGP/MYS', 'Phosphate rock', 'DAP', 'TSP', 'Urea', 'Potassium chloride', 'Aluminum', 'Iron ore, cfr spot', 'Copper', 'Lead', 'Tin', 'Nickel', 'Zinc', 'Gold', 'Platinum', 'Silver']
            excel_data = pd.read_excel(file_name,sheet_name="Monthly Prices",skiprows= 6)
            excel_data.columns = column_list
            excel_data["Period"] = excel_data["Period"].str.replace('M','-')
            new_file_name = f'{file_name.strip(".xlsx")}.csv'
            excel_data.to_csv(new_file_name,index=False,encoding='utf-8-sig')
            write_data_to_s3(new_file_name,target_bucket,f'cmo/monthly_prices/manual_data_commodity_prices.csv')

            column_list = ['Period', 'Energy', 'Non-energy', 'Agriculture', 'Beverages', 'Food', 'Oils & Meals', 'Grains', 'Other Food', 'Raw Materials', 'Timber', 'Other Raw Mat.', 'Fertilizers', 'Metals & Minerals', 'Base Metals (ex. iron ore)', 'Precious Metals']
            excel_data = pd.read_excel(file_name,sheet_name="Monthly Indices",skiprows= 9)
            excel_data.columns = column_list
            excel_data["Period"] = excel_data["Period"].str.replace('M','-')
            new_file_name = f'{file_name.strip(".xlsx")}.csv'
            excel_data.to_csv(new_file_name,index=False,encoding='utf-8-sig')
            write_data_to_s3(new_file_name,target_bucket,f'cmo/monthly_indices/manual_data_indices_prices.csv')

    target_bucket = os.environ['target_bucket_name']
    data_transformation(target_bucket)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
