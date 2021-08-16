import pandas

class PandasHelper:
    def __init__(self, folder_path):
        self.FOLDER_PATH = folder_path

    def load_csv(self, file_name):
        try:
            df = pandas.read_csv(self.FOLDER_PATH+file_name)
            return df
        except Exception as err:
            print(err)
            raise Exception("CSV could not be loaded")

    
