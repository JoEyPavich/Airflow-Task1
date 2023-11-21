from pyspark.sql import SparkSession

class SparkCSVReader:
    def __init__(self):
        self.spark = SparkSession.builder.appName("CSVReader").getOrCreate()

    def read_csv(self, file_path, header=True, infer_schema=True):
        df = self.spark.read.csv(file_path, header=header, inferSchema=infer_schema)
        return df
