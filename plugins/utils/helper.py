from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import col, split , lit
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import expr
from pyspark.sql.functions import concat,concat_ws, col , coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

class Helper:
    def __init__(self):
        self.spark = SparkSession.builder.appName("ETL-Liquor_Sales").getOrCreate()

    def read_csv(self, file_path):
        # df = self.spark.read.csv(file_path, header=True, inferSchema=True)
        df = self.spark.read.csv(file_path, header=True, inferSchema=True).limit(1000000)
        return df
    
    def read_parquet(self, file_path):
        df = self.spark.read.parquet(file_path, header=True, inferSchema=True)
        return df

    def generate_id(self, df,columns_to_select):
        df = df.select(*columns_to_select).distinct()
        df = df.withColumn("id", 1 + monotonically_increasing_id())
        columns = df.columns
        columns.insert(0, columns.pop(columns.index("id")))
        df = df.select(*columns)
        
        return df

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
    
    def create_schema_from_config(self, config):
        # Initialize an empty list to store the schema fields
        schema_fields = []

        # Iterate through the tables in the config
        for table in config['tables']:
            table_name = table['name']
            columns = table['columns']

            # Initialize an empty list for columns in the table
            table_columns = []

            # Iterate through the columns in the table
            for column in columns:
                column_name = column['name']
                column_type = column['type']
                column_nullable = column['nullable']

                # Map column type names to corresponding PySpark types
                if column_type.lower() == 'int':
                    spark_type = IntegerType()
                elif column_type.lower() == 'string':
                    spark_type = StringType()
                elif column_type.lower() == 'number':
                    spark_type = DoubleType()
                # Add more type mappings as needed

                # Create a StructField for each column and add it to table_columns
                table_columns.append(StructField(column_name, spark_type, nullable=True))

            # Create a StructType for the table and add it to schema_fields
            table_schema = StructType(table_columns)
            schema_fields.append((table_name, table_schema))

        return schema_fields

    def mapping(self, df, config):    
        new_dfs = {}  # Dictionary to hold new DataFrames for each table

        for table in config['tables']:
            table_name = table['name']
            columns = table['columns']
            selected_columns = [col(column['raw_name']).alias(column['name']) for column in table['columns'] if column['raw_name'] is not None]

            if(table_name == 'Store'):
                selected_columns.append(col('city'))
            print(f"\n\ntable name:{table['name']}")
            print(f"df : {df.count()}")
            selected_df = df.select(*selected_columns)
            print(f"selected_df : {selected_df.count()}")
            
            if(table_name == 'City'):
                gen_selected_columns = [column['name'] for column in columns if column['raw_name'] is not None]
                selected_df = self.generate_id(selected_df,gen_selected_columns)

            if(table_name == 'Store'):
                city_df = new_dfs['City'].withColumnRenamed("id", "city_id").withColumnRenamed("name", "city_name")
                selected_df = selected_df.join(city_df, selected_df['city'] == city_df['city_name'], 'left_outer') \
                    .drop('city','city_name', 'zip_code')
            
            selected_df = selected_df.distinct()
            print(f"distinct : {selected_df.count()}")
            selected_df.printSchema()
            new_dfs[table_name] = selected_df
            
        return new_dfs
