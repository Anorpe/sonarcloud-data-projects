
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import lit
from utils_functions import list_to_string

def display(df):
    print(df.toPandas().head())


def cast_columns(data,dict_columns_types):
    """

    Castea las columnas de un DataFrame

    Parametros
    ----------
    data: Dataframe
    Dataframe que se va a procesar
    dict_columns_types : Dictionary
    Diccionario que contiene pares de clave, valor donde la clave es el nombre la columna y su valor es el tipo de dato a castear

    Retorna
    -------
    data: DataFrame
    """
    for column in dict_columns_types:
        if column in data.columns:
            data = data.withColumn(column,data[column].cast(dict_columns_types[column]))

    return data

def with_nested_column_renamed(df,dict_columns_names):
    """
    
    Agrega Columnas a partir de columnas anidadas 
    
    Parametros
    ----------
    data: Dataframe
      Dataframe que se va a procesar
    dict_columns_names: Dict
      Diccionario, donde las claves son los nombres antiguos 
      y los valores los nombres nuevos
      
    Retorna
    -------
    df: Dataframe
    """
    for key in dict_columns_names:
        df = df.withColumn(dict_columns_names[key],df[key])
    return df


def max_date_key(df,key_column,date_column = "date"):
    df.createOrReplaceTempView("df")
    df_max = spark.sql(f"""
    SELECT
        {key_column},
        MAX({date_column}) as {date_column}
    FROM
        df
    GROUP BY
        {key_column}
    """)
    df_max.createOrReplaceTempView("df_max")
    df = spark.sql(f"""
    SELECT
        df.*
    FROM
        df
    INNER JOIN
        df_max
        ON
        df.{key_column} = df_max.{key_column} AND df.{date_column} = df_max.{date_column}
    """)
    
    df = df.drop(date_column)
    return df



def fill_empty_string(df,fill_string,subset = []):
  for column in subset:
    df = df.withColumn(column, regexp_replace(df[column], "^$", fill_string))
    #df = df.withColumn(column, regexp_replace(df[column], ' ', fill_string))
    #df = df[column].replace(' ', fill_string)
  return df
  



def withColumnRenamed(df,dict_columns_names):
    """
    
    Renombra varias columnas de un dataframe 
    
    Parametros
    ----------
    data: Dataframe
      Dataframe que se va a procesar
    dict_columns_names: Dict
      Diccionario, donde las claves son los nombres antiguos 
      y los valores los nombres nuevos
      
    Retorna
    -------
    df: Dataframe
    """
    for key in dict_columns_names:
        df = df.withColumnRenamed(key,dict_columns_names[key])
    return df
  
def columns_to_lower(df):
  """
  
  Tranforma a minusculas todas las columnas de un Dataframe 
  
  Parametros
  ----------
  data: Dataframe
    Dataframe que se va a procesar
    
  Retorna
  -------
  df: Dataframe
  """
  
  for col in df.columns:
    df = df.withColumnRenamed(col, col.lower())
  return df
  
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

def path_exists(path):
  """
  
  Devuelve un booleano sobre la existencia de una carpeta
  
  Parametros
  ----------
  path : String
    Ruta de la carpeta
    
  Retorna
  -------
  Boolean
  """
    
  try:
    dbutils.fs.ls(path)
    return True
  except:
    return False

def write_json(data, path,mode="overwrite"):
  """
  
  Escribe un archivo en formato json
  
  Parametros
  ----------
  data: Dataframe
    Dataframe que se va a escribir
  path : String
    Ruta donde se va a escribir
  mode : String
    Modo de escritura
    
  Retorna
  -------
  
  """
  data = data.repartition(1)
  data.write\
      .format('json')\
      .mode(mode)\
      .save(path)
  f_path = [f.path for f in dbutils.fs.ls(path) if f.name.endswith('json')][0]
  dbutils.fs.mv(f_path, path + '.json')
  dbutils.fs.rm(path, True)

def write_parquet(data, path,mode="overwrite"):
  """
  
  Escribe un archivo en formato parquet
  
  Parametros
  ----------
  data: Dataframe
    Dataframe que se va a escribir
  path : String
    Ruta donde se va a escribir
  mode : String
    Modo de escritura
    
  Retorna
  -------
  
  """
  data = data.repartition(1)
  data.write\
      .format('parquet')\
      .mode(mode)\
      .save(path)
  f_path = [f.path for f in dbutils.fs.ls(path) if f.name.endswith('parquet')][0]
  dbutils.fs.mv(f_path, path + '.parquet')
  dbutils.fs.rm(path, True)
  
def write_csv(data, path, delimiter = ',', header = "true",mode="overwrite"):
  """
  
  Escribe un archivo en formato csv
  
  Parametros
  ----------
  data: Dataframe
    Dataframe que se va a escribir
  path : String
    Ruta donde se va a escribir
  delimiter : String
    Delimitador de columnas
  mode : String
    Modo de escritura
    
  Retorna
  -------
  
  """
  data = data.repartition(1)
  data.write\
      .format('csv')\
      .mode(mode)\
      .option("header", header)\
      .option("delimiter", delimiter)\
      .save(path)
  f_path = [f.path for f in dbutils.fs.ls(path) if f.name.endswith('csv')][0]
  dbutils.fs.mv(f_path, path + '.csv')
  dbutils.fs.rm(path, True)



  
def dropna_columns_error(data,columns):
  """
  
  Elimina las fila nulas de las columnas seleccionadas de un dataframe y retorna las filas que fueron eliminadas 
  
  Parametros
  ----------
  data: Dataframe
    Dataframe que se va a procesar
  columns : List
    Lista que contiene las columnas seleccionadas
    
  Retorna
  -------
  data_result: DataFrame
    Dataframe sin filas nulas en las columnas seleccionadas
  data_error: DataFrame
    Dataframe con filas nulas en las columnas seleccionadas y la columna "error" con el error
  """
  data_result = data.dropna(subset = columns)
  
  data_error = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema = data.schema)
  data_error = data_error.withColumn("error",lit(""))
  for column in columns:
    data_column_error = spark.sql("""
    SELECT 
      data.*,
      'Column {0} with null value' AS error
    FROM
      data
    WHERE
      data.{0} IS NULL
    """.format(column))
    data_error = data_error.union(data_column_error)
  
  return data_result,data_error





def cast_columns_error(data,dict_columns_types):
  """
  
  Castea las columnas de un DataFrame
  
  Parametros
  ----------
  data: Dataframe
    Dataframe que se va a procesar
  dict_columns_types : Dictionary
    Diccionario que contiene pares de clave, valor donde la clave es el nombre la columna y su valor es el tipo de dato a castear
    
  Retorna
  -------
  data_result: DataFrame
    Dataframe con las columnas casteadas
  data_error: Dataframe
    Dataframe con filas que no pudieron ser casteadas al tipo definido y una columna de "error" con la causa
  """
  data_result = data
  for column in dict_columns_types:
    data_result = data_result.withColumn(column,data[column].cast(dict_columns_types[column]))
  
  data_result.createOrReplaceTempView("data_result")
  data.createOrReplaceTempView("data")
  
  data_error = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema = data.schema)
  data_error = data_error.withColumn("error",lit(""))
  
  for column in dict_columns_types:
    data_column_error = spark.sql("""
    SELECT 
      data.*,
      'Could not cast column {0} to defined type' AS error
    FROM
      data
    LEFT JOIN
      data_result
      ON
      data.{0} = data_result.{0}
    WHERE
      data.{0} IS NOT NULL AND data_result.{0} IS NULL
    """.format(column))
    data_error = data_error.union(data_column_error)
    
  return data_result,data_error




def group_by(df,key_columns,value_columns):
  """
  
  Aplica una operación de Group By sobre un dataframe
  
  Parametros
  ----------
  df: Dataframe
    Dataframe que se va a procesar
  key_columns: List
    Lista de columnas por las cuales se va a hacer la agrupación
  value_columns : Dictionary
    Diccionario que contiene como claves las columnas que se va a agregar y como valores los tipos de agregación para cada columna
    
  Retorna
  -------
  df: Dataframe   
  """
  select_key = list_to_string(key_columns,sep = ",")
  select_values = ""
  for value,aggregate_function in value_columns.items():
    select_values += aggregate_function + "(" + value + ")" + " as " + value + ","
 
  select = select_key+select_values
  select = select[:-1]
  query = "SELECT " + select + " FROM df GROUP BY " +  select_key[:-1]
  
  df.createOrReplaceTempView("df")
  df = spark.sql(query)
  
  
  return df


