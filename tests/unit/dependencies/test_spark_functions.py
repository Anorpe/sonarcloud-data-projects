import pytest
from pyspark.sql import SparkSession
#from dependencies.spark_functions import columns_to_upper

spark = SparkSession.builder.appName("spark").getOrCreate()

def columns_to_upper(df):
  """
  
  Tranforma a mayusculas todas las columnas de un Dataframe 
  
  Parametros
  ----------
  data: Dataframe
    Dataframe que se va a procesar
    
  Retorna
  -------
  df: Dataframe
  """
  
  for col in df.columns:
      df = df.withColumnRenamed(col, col.upper())
  return df


class TestDependenciesSparkFunctions:
    
    def test_columns_to_upper(self):
        data_in = [{"name":"jose","age":40}]
        df_in = spark.createDataFrame(data_in)
        
        data_out = [{"NAME":"jose","AGE":40}]
        df_out = spark.createDataFrame(data_in)
        assert columns_to_upper(df_in) == df_out
