import requests
import pandas as pd
from etl.utils import load_object

# Define the API endpoint and parameters

def get_data(**kwargs):
    api_endpoint = "https://api.fda.gov/device/recall.json"
    params = {'search': "root_cause_description.exact:'Device Design'", 'limit': 1000}

    # Initialize an empty dataframe to store the data
    final_df = pd.DataFrame()

    # Make 10 API requests and append data to the dataframe each time
    for i in range(10):
        response = requests.get(api_endpoint, params=params)

        if response.status_code == 200:
            data = response.json()
            data = data['results']
            df = pd.DataFrame(data)
            df['device_class'] = data[0]['openfda']['device_class']
            final_df = pd.concat([final_df,df], ignore_index=True)

    field_names = ["cfres_id",
                   "recall_status",
                   "recalling_firm",
                   "address_1",
                   "address_2",
                   "city",
                   "state",
                   "postal_code",
                   "product_code",
                   "root_cause_description",
                   "product_quantity",
                   "device_class"]
    
    # Export the final dataframe to a CSV file
    final_df = final_df[field_names]
    date = kwargs['execution_date'].strftime('%Y-%m-%d')
    path = f'raw/{date}_open_fda_raw.csv'
    load_object(final_df, path)
