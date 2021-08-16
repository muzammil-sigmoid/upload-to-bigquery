from google.oauth2 import service_account
import pandas_gbq
from google.cloud import bigquery


class GBQHelper:

    def __init__(self,keypath,folder_path, project_id):
        self.credentials = service_account.Credentials.from_service_account_file(keypath)
        pandas_gbq.context.credentials = self.credentials
        pandas_gbq.context.project = project_id
        self.bq_client = bigquery.Client.from_service_account_json(keypath)

    def check(self):
        df = pandas_gbq.read_gbq("select * from `airflow-1-321705.vehicle_analytics.latest`")
        print(type(df))
        

    # creating dataset
    def create_dataset(self, dataset_id, project_id):
        try:
            dataset = bigquery.Dataset(project_id+'.'+dataset_id)
            dataset.location = "US"
            dataset = self.bq_client.create_dataset(dataset, timeout=45)
            return dataset.dataset_id
        except Exception as err:
            print(err)
            raise Exception("Creation of dataset failed")

    def load_table_to_bq(self,dataset_id,table_id, df):
        try:
            pandas_gbq.to_gbq(df, dataset_id+'.'+table_id,if_exists='fail')
        except Exception as err:
            print(err)
            raise Exception("Table Load Failed")




