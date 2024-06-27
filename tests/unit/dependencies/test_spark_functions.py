import pytest
from dependencies.spark_functions import columns_to_upper

"""from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spark").getOrCreate()


class TestDependenciesSparkFunctions:
    
    def test_columns_to_upper(self):
        data_in = [{"name":"jose","age":40}]
        df_in = spark.createDataFrame(data_in)
        
        data_out = [{"NAME":"jose","AGE":40}]
        df_out = spark.createDataFrame(data_in)
        assert columns_to_upper(df_in) == df_out"""
