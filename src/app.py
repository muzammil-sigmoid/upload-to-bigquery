from src.gcs_helper import GCSHelper
from src.gbq_helper import GBQHelper
from secret.config import config
from src.pandas_helper import PandasHelper


class App:

    def __init__(self):
        self.project_id = config["PROJECT_ID"]
        self.gcs = GCSHelper(config["KEY_PATH"], config["FOLDER_PATH"])
        self.gbq = GBQHelper(config["KEY_PATH"], config["FOLDER_PATH"], config["PROJECT_ID"])
        self.pd = PandasHelper(config["FOLDER_PATH"])

    #driver function
    def solve(self):
        try:
            self.gcs.get_file('customers.csv', 'assignment-customers-orders')
            self.gcs.get_file('orders.csv', 'assignment-customers-orders')
            self.gbq.create_dataset("customer_orders", self.project_id)
            df1 = self.pd.load_csv("customers.csv")
            df2 = self.pd.load_csv("orders.csv")
            merged_df = df1.merge(df2, on="CustomerID", how="left")
            self.gbq.load_table_to_bq("customer_orders", "customer_orders", merged_df)
        except Exception as err:
            print(err.args[0])





