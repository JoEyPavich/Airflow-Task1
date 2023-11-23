from pyspark.sql import SparkSession

class CSVReader:
    def __init__(self, file_path):
        self.file_path = file_path
        self.spark = SparkSession.builder.appName("CSV Reader").getOrCreate()

    def read_csv(self):
        df = self.spark.read.csv(self.file_path, header=True, inferSchema=True)
        return df