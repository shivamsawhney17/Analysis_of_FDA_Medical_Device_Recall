import pandas as pd
import boto3
import os
from io import StringIO,BytesIO
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError



def load_object(object,path):
    aws_access_key_id = os.getenv('aws_access_key_id')
    aws_secret_access_key = os.getenv('aws_secret_access_key')
    upload_to_cloud(object,path,aws_access_key_id, aws_secret_access_key)


def get_object(file):
    aws_access_key_id = os.getenv('aws_access_key_id')
    aws_secret_access_key = os.getenv('aws_secret_access_key')
    df = get_from_cloud(file, aws_access_key_id, aws_secret_access_key)
    return df



def upload_to_cloud(data,path,aws_access_key_id, aws_secret_access_key):
    bucket_name = 'dsc-bucket-1'
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)

    s3 = session.client('s3')

    # Upload the CSV string to the specified S3 bucket and folder
    try:
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=path)
        print(f"File uploaded successfully to '{bucket_name}/{path}'.")
    except Exception as e:
        print(f"An error occurred: {e}")


def get_from_cloud(file,aws_access_key_id, aws_secret_access_key):
    bucket_name = 'dsc-bucket-1'
    session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)

    # Create an S3 client
    s3 = session.client('s3')

    try:
        response = s3.get_object(Bucket=bucket_name, Key=file)
        data = response['Body'].read()
        df = pd.read_csv(BytesIO(data))
        return df
    except Exception as e:
        print(f"File does not exist: {e}")
        return None
    
def data_from_warehouse(table):

    conn_str = 'postgresql+psycopg2://postgres:Attackontitan1@redshift-cluster-1.cbjh50ytpqj5.us-east-2.redshift.amazonaws.com:5439/open-fda'

    try:
        engine = create_engine(conn_str)
        table_name = table
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, engine)
    except OperationalError as e:
        df = pd.DataFrame()
    return df



