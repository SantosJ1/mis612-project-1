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
            column_list = ['Period','Crudeoilaverage', 'CrudeoilBrent', 'CrudeoilDubai', 'CrudeoilWTI', 'CoalAustralian',	'CoalSouthAfrican', 'NaturalgasUS',	'NaturalgasEurope', 'LiquefiednaturalgasJapan', 'Naturalgasindex', 'Cocoa', 'CoffeeArabica', 'CoffeeRobusta', 'Teaavg3auctions', 'TeaColombo', 'TeaKolkata', 'TeaMombasa', 'Coconutoil', 'Groundnuts', 'Fishmeal', 'Groundnutoil', 'Palmoil', 'Palmkerneloil',	'Soybeans', 'Soybeanoil', 'Soybeanmeal', 'Rapeseedoil', 'Sunfloweroil', 'Barley', 'Maize', 'Sorghum', 'RiceThai5%',	'RiceThai25%', 'RiceThaiA.1', 'RiceVietNamese5%',	'WheatUSSRW', 'WheatUSHRW', 'BananaEurope', 'BananaUS', 'Orange', 'Beef', 'Meatchicken', 'Meatsheep', 'ShrimpsMexican', 'SugarEU', 'SugarUS', 'Sugarworld', 'TobaccoUSimportu.v.', 'LogsCameroon', 'LogsMalaysian', 'SawnwoodCameroon', 'SawnwoodMalaysian', 'Plywood', 'CottonAIndex', 'RubberTSR20', 'RubberSGP/MYS', 'Phosphaterock', 'DAP', 'TSP', 'Urea', 'Potassiumchloride', 'Aluminum', 'Ironorecfrspot', 'Copper', 'Lead', 'Tin', 'Nickel', 'Zinc', 'Gold', 'Platinum', 'Silver']
            excel_data = pd.read_excel(file_name,sheet_name="Monthly Prices",skiprows= 6)
            excel_data.columns = column_list
            excel_data["Period"] = excel_data["Period"].str.replace('M','-')
            new_file_name = f'{file_name.strip(".xlsx")}.csv'
            excel_data.to_csv(new_file_name,index=False,encoding='utf-8-sig')
            write_data_to_s3(new_file_name,target_bucket,f'cmo/monthly_prices/manual_data_commodity_prices.csv')

            column_list = ['Period', 'Energy', 'Non-energy', 'Agriculture', 'Beverages', 'Food', 'Oils&Meals', 'Grains', 'OtherFood', 'RawMaterials', 'Timber', 'OtherRawMat.', 'Fertilizers', 'Metals&Minerals', 'BaseMetals(ex.ironore)', 'PreciousMetals']
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
