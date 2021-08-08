from google.cloud import storage


class GCSHelper:
    def __init__(self,key_path, folder_path):
        self.KEY_PATH = key_path
        self.FOLDER_PATH = folder_path
        self.storage_client = storage.Client.from_service_account_json(self.KEY_PATH)

    def list_buckets(self):
        try:
            buckets = list(self.storage_client.list_buckets())
            print(buckets)
        except Exception as err:
            raise Exception('Buckets fetching failed')

    def get_file(self, file_name, bucket_name):
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(file_name)
            blob.download_to_filename(self.FOLDER_PATH+file_name)
        except Exception as err:
            print(err)
            raise Exception("Fail to download "+file_name+".")




