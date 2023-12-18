from etl.utils import get_object
from sqlalchemy import create_engine
import os


def load_data(**kwargs):
    date = kwargs['execution_date'].strftime('%Y-%m-%d')
    df = get_object(f'transformed/{date}_recall_fct.csv')
    df_firm = get_object(f'transformed/{date}_firm_dim.csv')
    df_status = get_object(f'transformed/{date}_status_dim.csv')
    df_cause = get_object(f'transformed/{date}_cause_dim.csv')

    db_url = 'postgresql+psycopg2://postgres:Attackontitan1@redshift-cluster-1.cbjh50ytpqj5.us-east-2.redshift.amazonaws.com:5439/open-fda'

    engine = create_engine(db_url)
    
    df.to_sql('recall', engine, index=False,if_exists='replace')

    df_firm.to_sql('firm', engine, index=False,if_exists='replace')
    df_status.to_sql('status', engine, index=False,if_exists='replace')
    df_cause.to_sql('cause', engine, index=False,if_exists='replace')








