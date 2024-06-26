

def list_to_string(lista:list, sep:str = ""):
  """
  
  Convierte una lista en un string
  
  Parametros
  ----------
  lista: List
    Lista a convertir
  sep : String
    Separador de los elementos de la lista
    
  Retorna
  -------
  result: String   
  """
  result = ""
  for item in lista:
    result += str(item)+sep
  return result


def dict_to_string_schema(dict_schema:dict):
  """

  Convierte un diccionario con un esquema de Spark en un
  esquema en formato string de Spark

  Parametros
  ----------
  dict_schema: dict
    Diccionario con el esquema
  
    
  Retorna
  -------
  result: string  
  """
  str_schema = ""
  for column, type in dict_schema.items():
      str_schema += column + " " + type +","
  str_schema = str_schema[:-1]
  return str_schema