from pyspark.sql import SparkSession

class Transform_module:
    def __init__(self):
        self.spark = SparkSession.builder.appName("ETL-Liquor_Sales").getOrCreate()

    def read_csv(self, file_path):
        df = self.spark.read.csv(file_path, header=True, inferSchema=True)
        return df

    def unit_transform(self):
        pass

    def hashing(data):
        pass

    def mapping(data):
        pass

    def cleansing(data):
        pass

    def change_data_type(datag):
        pass
