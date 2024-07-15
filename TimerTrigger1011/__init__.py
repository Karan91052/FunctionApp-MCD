import datetime
import logging
import os
from time import sleep
import pandas as pd
# from datetime import date,timedelta
from datetime import datetime, timedelta
# from datetime import datetime, timezone
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import HttpResponseError
from azure.storage.blob import BlobServiceClient, BlobClient
import azure.functions as func
 
azure_client_id = os.environ.get("secret.Azure-Client-id")
azure_client_secret = os.environ.get("secret.Azure-Client-secret")
azure_subscription_id = os.environ.get("secret.Azure-Subscription-id")
azure_tenant_id = os.environ.get("secret.Azure-Tenant-id")

def main(mytimer: func.TimerRequest) -> None:
    credential = DefaultAzureCredential()
    client = LogsQueryClient(credential)
    query = "SecurityRecommendation"
    start_time = datetime(2023,11,8)
    duration = timedelta(days=5)
    end_time = start_time + duration
    try:
        response = client.query_workspace(
            workspace_id="LOg Analytic Workspace id",
            query=query,
            timespan=(start_time, duration)
            )
        if response.status == LogsQueryStatus.PARTIAL:
            error = response.partial_error
            data = response.partial_data
            print(error.message)
        elif response.status == LogsQueryStatus.SUCCESS:
            data = response.tables
            for table in data:
                df = pd.DataFrame(data=table.rows, columns=table.columns)
                print(df)
                df.to_csv("Policies.csv")
                blob_service_client = BlobServiceClient.from_connection_string("secret.Paste the Storage-account-connection-string")
                blob_client = blob_service_client.get_blob_client(container="blobconatiner", blob="Policies.csv")
            with open("Policies.csv", "rb") as data:
                 blob_client.upload_blob(data, blob_type="AppendBlob")
    except HttpResponseError as err:
            print("something fatal happened")
            print (err)
            
main(func.TimerRequest)