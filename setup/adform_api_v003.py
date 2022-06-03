#!/usr/bin/env python
__author__ = 'Ahnet Bugra Buga'
__version__ = '1.0.0'
__email__ = 'bugra.buga@omd.com'
__status__ = 'Production'


################################################################################################################################################################
# Libraries                                                                                                                                                    #
################################################################################################################################################################
import json
from urllib import response
import requests
import time
import pandas as pd
import os
import datetime as dt


################################################################################################################################################################
# Access Token                                                                                                                                                # 
################################################################################################################################################################

def create_access_token(client_id, client_secret):
    """
    Create an access token using the client_id and client_secret

    Parameters
    ------------
    client_id : str
        The client id of the application
    client_secret : str
        The client secret of the application
    
    Returns
    ------------
    access_token : str
        The access token

    """
    response = requests.post("https://id.adform.com/sts/connect/token", data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://api.adform.com/scope/buyer.stats"
    })

    access_token = response.json()['access_token']
    with open("/path/your_jsondata.json", "r") as j:
        file_data = json.loads(j.read())
    file_data.update({"access_token": access_token})
    with open('/path/your_jsondata.json', mode='w', encoding='utf-8') as json_file:
        json.dump(file_data, json_file)
    return access_token


def read_access_token():
    with open("/path/your_jsondata.json", "r") as jsonFile:
        data = json.loads(jsonFile.read())

    access_token = data["access_token"]
    return access_token


def update_access_token(client_id, client_secret):
    """
    Update the access token using the client_id and client_secret

    Parameters
    ------------
    client_id : str
        The client id of the application
    client_secret : str
        The client secret of the application
    
    Returns
    ------------
    access_token : str
        The access token
    """
    access_token = create_access_token(client_id, client_secret)

    return access_token



def read_operation_status(operation):
    """
    Read the operation status

    Parameters
    ------------
    operation : str
        The operation location

    """
    access_token = read_access_token()
    response = requests.get(
        "htttps://api.adform.com/{}".format(operation),
        headers={"Authorization": "Bearer {}".format(access_token)}
    )
    if response.status_code == 200:
        pass
    else:
        time.sleep(3)
        print(f"Read Operation Status Failed: {response.status_code}")
        print("Try Again...")
        read_operation_status(operation)
    print(response.status_code)


def read_location(location):
    """
    Read the location

    Parameters
    ------------
    location : str
        The location

    """
    access_token = read_access_token()
    response = requests.get(
        "https://api.adform.com/{}".format(location),
        headers={"Authorization": "Bearer {}".format(access_token)}
    )
    print("Read Location {}".format(response.status_code))

    rows = response.json()['reportData']['rows']
    headers = response.json()['reportData']['columnHeaders']
    df = pd.DataFrame(rows, columns=headers)
    df['get_report_time'] = dt.datetime.now().strftime("%Y-%m-%d")
    print(df.info())
    df.to_csv("/path/your_csvdata{}.csv".format(df["get_report_time"].max()), index=False)


def post_requests(client_id, client_secret):
    """
    Post the requests

    Parameters
    ------------
    client_id : str
        The client id of the application

    client_secret : str
        The client secret of the application
    
    
    """


    access_token = read_access_token()
    body = {
        "dimensions": ["dimension1", "dimension2"],
        "metrics": [
            {
                "metric": "metric1"
            },
            {
                "metric": "metric2"
            }
        ],
        "filter": {
            "date": {
                "from": "YYYY-MM-DD",
                "to": "YYYY-MM-DD"
            }
        }
    }

    response = requests.post("https://api.adform.com/v1/buyer/stats", data=json.dumps(body),
                             headers={
                                    "Content-Type": "application/json",
                                    "Authorization": "Bearer {}".format(access_token)
                                },
                                json={
                                    "dimensions": body["dimensions"],
                                    "metrics": body["metrics"],
                                    "filter": body["filter"]

                                })
    if response.status_code != 202:
        print(f"Post Requests Failed: {response.status_code}")
        print(f"Error Messages: {response.text}")
        time.sleep(5)
        update_access_token(client_id, client_secret)
        print("Updated Access Token")
        print("Try Again...")
        post_requests(client_id, client_secret)
    else:
        print(f"Post Requests Successful: {response.status_code}")
        print(f"Response: {response.headers}")
        time.sleep(5)
        location = response.headers['Location']
        print(f"Location: {location}")
        time.sleep(5)
        operation = response.headers['Operation-Location']
        print(f"Operation: {operation}")
        time.sleep(5)
        read_operation_status(operation)
        time.sleep(5)
        read_location(location)


def setup():
    with open("/path/your_jsondata.json", "r") as jsonFile:
        data = json.loads(jsonFile.read())
    
    scope = data["scope"]
    client_id = data["client_id"]
    client_secret = data["client_secret"]
    post_requests(client_id=client_id, client_secret=client_secret)


if __name__ == "__main__":
    setup()