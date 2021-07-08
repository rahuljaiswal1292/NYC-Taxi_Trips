import requests
projectId = "statueofliberty-318808"
datasetId = "statueofliberty-318808:nyc_taxi_trips"
tableId = "statueofliberty-318808:nyc_taxi_trips.nyc_taxi_trips_vw"

def implicit():
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from pprint import pprint

    # TODO(developer): Set key_path to the path to the service account key
    #                  file.
    key_path = "C:\\Users\Rahul\Desktop\Python\statueofliberty-318808-97c66d0a2a32.json"
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    url =  'https://bigquery.googleapis.com/bigquery/v2/projects/'+ projectId +'/datasets/'+ datasetId +'/tables/' +tableId; 
    headers = {
        "Authorization": "Bearer $ACCESS_TOKEN"
    }
    response = requests.get(url,headers=headers)
    pprint(response.json())

if __name__ == "__main__":
    implicit()