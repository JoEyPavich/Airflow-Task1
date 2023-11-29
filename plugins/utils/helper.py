from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import col, split , lit
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import expr
from pyspark.sql.functions import concat,concat_ws, col , coalesce
class Helper:
    def __init__(self):
        self.spark = SparkSession.builder.appName("ETL-Liquor_Sales").getOrCreate()

    def read_csv(self, file_path):
        df = self.spark.read.csv(file_path, header=True, inferSchema=True).limit(20)
        return df
    
    def read_parquet(self, file_path):
        df = self.spark.read.parquet(file_path, header=True, inferSchema=True)
        return df
    def select_column_by_city(self,df):
        # file_path = "/opt/airflow/data_source"
        # selected_columns = self.spark.read.csv(file_path, header=True, inferSchema=True).select('City')
        selected_columns = df.select('City')
        print(selected_columns)
        selected_columns = selected_columns.dropDuplicates()
        # selected_columns = selected_columns.reset_index()
        
        selected_columns = selected_columns.withColumn("index", monotonically_increasing_id())
        selected_columns.write.csv("/opt/airflow/data_source/City", header=True, mode="overwrite")

    def split_store_name_and_city(self,df):

        # ใช้ expr เพื่อใส่เงื่อนไขในการแยกข้อมูล
        print("**********************************************************************")
        # ใช้ split เพื่อแยกข้อมูลด้วย '/'
        split_col = split(df['store_name'], '/')
        df = df.withColumn('store_name_1', split_col.getItem(0))
        df = df.withColumn('store_name_2', split_col.getItem(1))
        df = df.withColumn('store_name', concat(coalesce(df['store_name_1'],lit('')), lit(','),coalesce(df['store_name_2'],lit(''))))   
        print(df.show())
        # ใช้ getItem ในลูปเพื่อดึงข้อมูลตั้งแต่ตำแหน่งที่ 1 จนถึงตำแหน่งที่ n-1
        print("**********************************************************************")
        

        # df_split.show(truncate=False)
        # return df_split
        return df

    def find_index_by_city(self,target_city):
        file_path = "/opt/airflow/data_source/City"
        index =0 
        city = self.spark.read.csv(file_path, header=True, inferSchema=True)
        city = city.filter(col("city") == target_city).select("index")
        index = city.first()[0]
        return index
        

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
    
    def rename_col(self, df):
        column_names = df.columns
        print(column_names)

        for column_name in column_names:
            new_column_name = column_name.replace(' ', '_').lower()
            df = df.withColumnRenamed(column_name, new_column_name)
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
