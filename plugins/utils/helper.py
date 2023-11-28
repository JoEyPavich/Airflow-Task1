from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import col, split

class Helper:
    def __init__(self):
        self.spark = SparkSession.builder.appName("ETL-Liquor_Sales").getOrCreate()

    def read_csv(self, file_path):
        df = self.spark.read.csv(file_path, header=True, inferSchema=True).limit(20)
        return df
    
    def read_parquet(self, file_path):
        df = self.spark.read.parquet(file_path, header=True, inferSchema=True)
        return df

    def split_lat_long(self, df, col_name='Store Location'):
        df = df.withColumn(
            'Latitude',
            regexp_extract(df[col_name], r'POINT \(([-+]?\d+\.\d+) ([-+]?\d+\.\d+)\)', 1)
        )
        df = df.withColumn(
            'Longitude',
            regexp_extract(df[col_name], r'POINT \(([-+]?\d+\.\d+) ([-+]?\d+\.\d+)\)', 2)
        )
        return df

    # def clean_store_name(self, df, col_name='Store Name'):
    #     # Splitting the 'Store Name' column by '/'
    #     # split_col = split(df[col_name], ' / ')
    #     # print(split_col)

    #     # Selecting all parts except the last one (which is assumed to be the city)
    #     # new_store_name = split_col[:-1]  # Exclude the last part (city)

    #     # Joining the remaining parts back together using '/'
    #     df = df.withColumn('Store Name without City', col('Store Name').substr(1, col('Store Name').rfind('/')))

    #     return df
    
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
