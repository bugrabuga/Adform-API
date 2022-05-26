import json
from urllib import response
import requests
import time
import pandas as pd
from google.cloud import bigquery
import os
import datetime as dt

client_id = "eapi.accuen.omd.tr@clients.adform.com"
client_secret = "NP806yk_Pl3oFkDdgtBf2CT3_eaLKzSbc0k--Xza"


def create_access_token(client_id, client_secret):
    response = requests.post("https://id.adform.com/sts/connect/token", data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://api.adform.com/scope/buyer.stats"
    })

    access_token = response.json()['access_token']
    with open(r"C:\Users\bugra.buga\.vscode\Main Projects\client_secret.json", "r") as j:
        file_data = json.loads(j.read())
    file_data.update({"access_token": access_token})
    with open('client_secret.json', mode='w', encoding='utf-8') as json_file:
        json.dump(file_data, json_file)
    return access_token



def read_access_token():
    with open(r"C:\Users\bugra.buga\.vscode\Main Projects\client_secret.json", "r") as jsonFile:
        data = json.loads(jsonFile.read())


    access_token = data["access_token"]
    return access_token


def update_access_token(client_id, client_secret, access_token):
    access_token = create_access_token(client_id, client_secret)

    return access_token


def post_operation(client_id, client_secret, scope):
    access_token = read_access_token()
    body = {
        "dimensions": ["client", "clientID", "deal", "dealID", "date", "rtbInventorySource", "adCreativeType"],
        "metrics": [
            {
                "metric": "rtbMediaCost"
            },
            {
                "metric": "impressions"
            }
        ],
        "filter": {
            "date": {
                "from": "2022-01-01",
                "to": "2022-03-31"
            }
        }
    }

    response = requests.post(
        "https://api.adform.com/v1/buyer/stats/data",
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(access_token)
        },
        json={
            'dimensions': body['dimensions'],
            'metrics': body['metrics'],
            'filter': body['filter']
        }
    )
    if response.status_code != 202:
        print("failed {}".format(response.status_code))
        print(response.text)
        time.sleep(5)
        access_token = create_access_token(client_id, client_secret)
        update_access_token(access_token)
        print("updated json")
        post_operation(client_id, client_secret)
    else:
        print("success {}".format(response.status_code))
        print(response.headers)
        time.sleep(5)
        print(response.headers)
        time.sleep(5)
        location = response.headers['Location']
        time.sleep(5)
        operation = response.headers['Operation-Location']
        time.sleep(5)
        read_operation_status(operation)
        time.sleep(5)
        read_location(location)


def read_operation_status(operation):
    access_token = read_access_token()
    response = requests.get(
        "https://api.adform.com/{}".format(operation),
        headers={"Authorization": "Bearer {}".format(access_token)}
    )

    if 200 == response.status_code:
        pass
    else:
        time.sleep(5)
        print("read operation {}".format(response.status_code))
        read_operation_status(operation)
    print(response.status_code)


def read_location(location):
    access_token = create_access_token(client_id, client_secret)
    response = requests.get(
        "https://api.adform.com/{}".format(location),
        headers={"Authorization": "Bearer {}".format(access_token)}
    )
    print("read location {}".format(response.status_code))

    rows = response.json()['reportData']['rows']
    headers = response.json()['reportData']['columnHeaders']
    df = pd.DataFrame(rows)
    df.columns = headers
    df['report_time'] = dt.datetime.now().strftime('%Y-%m-%d')
    #df['date'] = df['date'].map(lambda x: str(x)[:10])
    print(df.info())
    df.to_csv("output_data_{}.csv".format(df['report_time'].max()), index=False)


def setup():
    with open(r"C:\Users\bugra.buga\.vscode\Main Projects\client_secret.json", "r") as j:
        data = json.loads(j.read())

    scope = "McDonalds_OMD"
    client_id = data["client_id"]
    client_secret = data["client_secret"]
    post_operation(client_id, client_secret, scope)


if __name__ == '__main__':
    setup()


data = pd.read_csv('output_data_2022-05-23.csv')
data.info()
data.columns
data['date'] = data['date'].astype('datetime64')
data["date"] = data["date"].dt.strftime("%Y-%m-%d")
data['date']
data['rtbMediaCost'].sum()

data.to_csv('output_data.csv', index=False)



##################################################################################################
# BigQuery Load
##################################################################################################

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\bugra.buga\.vscode\Main Projects\DATASETS\omgtracme-924d2efd67ef.json"

client = bigquery.Client()

table_id = 'omgtracme.mediumtotalreport.adform_MediumTotal'

job_config = bigquery.LoadJobConfig(
    source_format = bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=False
)

job_config.schema = [
    bigquery.SchemaField('client', 'STRING'),
    bigquery.SchemaField('clientID', 'INTEGER'),
    bigquery.SchemaField('deal', 'STRING'),
    bigquery.SchemaField('dealID', 'STRING'),
    bigquery.SchemaField('date', 'DATE'),
    bigquery.SchemaField('rtbInventorySource', 'STRING'),
    bigquery.SchemaField('adCreativeType', 'STRING'),
    bigquery.SchemaField('rtbMediaCost', 'FLOAT'),
    bigquery.SchemaField('impressions', 'INTEGER')


]


with open(r"C:\Users\bugra.buga\.vscode\Main Projects\output_data.csv", "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)


job.result()

table = client.get_table(table_id)
print(
    "Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id)
)








