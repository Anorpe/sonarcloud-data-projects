import requests
from google_cloud_functions import upload_blob


def get_api_data(url,headers,start,limit,date_after, date_before):
    
    params = {
    "start" : start,
    "limit" : limit,
    "date_afterOrNow" : date_after,
    "date_beforeOrNow" : date_before

    }

    response = requests.get(
        url,
        headers = headers,
        params = params
    )

    return response.text

