from etl.utils import get_object,load_object, data_from_warehouse
from db.table_creation import create_table
import pandas as pd


def transform_data(**kwargs):
    create_table()
    date = kwargs['execution_date'].strftime('%Y-%m-%d')
    path = f'raw/{date}_open_fda_raw.csv'
    data = get_object(path)
    data['recall_id'] = data['cfres_id']
    data['firm'] = data['recalling_firm']
    data['cause'] = data['root_cause_description']
    data['status'] = data['recall_status']
    cols_to_drop = ['recalling_firm','root_cause_description','recall_status','cfres_id']
    data.drop(cols_to_drop,axis=1,inplace=True)
    
    df_firm = data_from_warehouse('firm')
    firm_cols = ['firm','address_1','address_2','city','state','postal_code']
    df_firm = check_for_updates(data[firm_cols], df_firm,'firm')
    ordered_cols = ['firm_id','firm','state','city','address_1','address_2','postal_code']
    df_firm = df_firm[ordered_cols]
    data = data.merge(df_firm)


    df_cause = data_from_warehouse('cause')
    df_cause = check_for_updates(data['cause'], df_cause,'cause')
    cause_cols = ['cause_id','cause']
    df_cause = df_cause[cause_cols]
    data = data.merge(df_cause)

    
    df_status = data_from_warehouse('status')
    df_status = check_for_updates(data['status'], df_status,'status')
    status_cols = ['status_id','status']
    df_status = df_status[status_cols]
    data = data.merge(df_status)

    fact_cols = ['recall_id','firm_id','status_id','cause_id','product_quantity','product_code','device_class']

    data = data[fact_cols]
    date = kwargs['execution_date'].strftime('%Y-%m-%d')
    load_object(data, f'transformed/{date}_recall_fct.csv')
    load_object(df_firm, f'transformed/{date}_firm_dim.csv')
    load_object(df_status, f'transformed/{date}_status_dim.csv')
    load_object(df_cause, f'transformed/{date}_cause_dim.csv')


def check_for_updates(df,df_old,col):
    
    df_new = df.drop_duplicates().reset_index()
    df_new[f'{col}_id'] = df_new.index + 1
    df_new.drop('index',axis=1,inplace=True)
    if len(df_old) == 0:
        return df_new

    new_values = df_new.loc[~df_new[col].isin(df_old[col])]

    max_id = df_old[f'{col}_id'].max()

    new_values[f'{col}_id'] = range(max_id + 1, max_id + 1 + len(new_values))

    df_old = pd.concat([df_old, new_values],ignore_index=True)
    return df_old




    











