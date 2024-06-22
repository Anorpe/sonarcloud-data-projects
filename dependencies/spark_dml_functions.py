import spark

def create_register_table(database_name,table_name,schema,source,partition = None,is_dim_type_2 = False,if_not_exists = True):
    """
  
    Crear una tabla y registrarla en la base de datos
    
    Parametros
    ----------
    zone: String
        Zona del datalake a la que pertenece la tabla
    table_name : String
        Nombre de la tabla
    schema : String,StructType
        Esquema de la tabla
    source : String
        Sistema origen de la tabla
    partition : String,List<String>
        Columna o columnas para realizar particionamiento de la tabla
        

    Retorna
    -------
  
    """
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema = schema )
    

    if is_dim_type_2:
        df = df.withColumn(  date_from_name_dim,lit(to_date(current_timestamp() )).cast('date')  )
        df = df.withColumn(  date_to_name_dim,lit(to_date(current_timestamp() )).cast('date')  )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    
    if type(partition) == type(None):
        df.write.format("delta").mode("ignore").option("overwriteSchema", "true").save(f'{mountpoint}{source}{table_name}')
    else:
        df.write.format("delta").mode("ignore").option("overwriteSchema", "true").partitionBy(partition).save(f'{mountpoint}{source}{table_name}')
        
    if if_not_exists:
      query = f"CREATE TABLE IF NOT EXISTS {database_name}.{table_name} USING delta LOCATION '{mountpoint}{source}{table_name}'"
      spark.sql(query) 
    else:
      query = f"DROP TABLE IF EXISTS {database_name}.{table_name} "
      spark.sql(query) 
      query = f"CREATE TABLE {database_name}.{table_name} USING delta LOCATION '{mountpoint}{source}{table_name}'"
      spark.sql(query)
        




def update_silver_dim_type_2(df_silver_delta,source,table_name,keys):
    filename_silver = f"silver/{source}/{table_name}"
    #spark.conf.set("variable_local.table_name",table_name)
    #spark.conf.set("variable_local.source",source)
    keys_query = ""
    for key in keys:
        keys_query += f"silver.`{key}` = dif.`{key}` AND "
    keys_query = keys_query[0:-4]
    
    df_silver = spark.read.format("delta").load(mountpoint + "/" + filename_silver)
    df_silver_last = df_silver.where(f"{date_to_name_dim} IS NULL")
    df_silver_last = df_silver_last.drop(date_from_name_dim)
    df_silver_last = df_silver_last.drop(date_to_name_dim)
    df_silver_last.createOrReplaceTempView("df_silver_last")
    #display(df_silver_last)

    differences = df_silver_delta.subtract(df_silver_last)
    differences = differences.withColumn(date_from_name_dim,lit(current_date()) )
    differences = differences.withColumn(date_to_name_dim,lit(None).cast('date'))
    differences.createOrReplaceTempView("differences")
    #display(differences)

    merge_query = f"""
    MERGE INTO 
        delta.`{mountpoint}/silver/{source}/{table_name}` silver
    USING 
        differences dif
        ON {keys_query}
    WHEN MATCHED AND silver.DATE_TO is null THEN UPDATE SET DATE_TO = dif.DATE_FROM
    """
    print(merge_query)
    spark.sql(merge_query)
    differences.write.format("delta").mode("append").save(mountpoint + "/" + filename_silver)