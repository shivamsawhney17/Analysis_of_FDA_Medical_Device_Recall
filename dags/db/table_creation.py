import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
import os


def create_table():
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = 'redshift-cluster-1.cbjh50ytpqj5.us-east-2.redshift.amazonaws.com'
    DB_PORT = 5439
    
    conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME)




    table_queries = ["""CREATE TABLE IF NOT EXISTS firm(
    firm_id INT PRIMARY KEY,
    firm VARCHAR,
    state VARCHAR,
    city VARCHAR,
    address_1 VARCHAR,
    address_2 VARCHAR,
    postal_code VARCHAR
    );""",

    """CREATE TABLE IF NOT EXISTS status(
    status_id INT PRIMARY KEY,
    status VARCHAR
    );""",

    """CREATE TABLE IF NOT EXISTS cause (
    cause_id INT PRIMARY KEY,
    cause VARCHAR
    );""",

    """CREATE TABLE IF NOT EXISTS recall(
    recall_id INT PRIMARY KEY,
    firm_id INT,
    status_id INT,
    cause_id INT,
    product_quantity VARCHAR,
    product_code VARCHAR,
    device_class INT
    );"""
    ]

    cursor = conn.cursor()

    for query in table_queries:
        cursor.execute(query)
    print('Tables have been created')
    conn.commit()
    cursor.close()
    conn.close()
