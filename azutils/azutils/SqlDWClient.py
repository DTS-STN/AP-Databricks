from azutils.AKVClient import AKVClient
from pyspark.sql import SparkSession
import re
from itertools import permutations

class SqlDWClient:

  """ 
  This class allows the user/developer to read from and write to 
  Azure Synapse Dedicated SQL Pool.
  """
  
  spark = SparkSession.builder.getOrCreate()

  def __init__(self):
    pass
    
  def connect_to_adls(self):
    """Connects to ADLS."""
    SqlDWClient.spark.conf.set('fs.azure.account.key.' + AKVClient.adlsName() + '.dfs.core.windows.net', AKVClient.adlsAccessKey())
  
  @classmethod
  def write(cls, df, dw_table:str, write_mode:str = "overwrite"):
    """Writes dataframe to Synapse Table."""
    cls.connect_to_adls(cls)
    
    container_name = "sqltmp"
    temp_dir = f"abfss://{container_name}@{AKVClient.adlsName()}.dfs.core.windows.net/sqldw"

    df.write \
      .format("com.databricks.spark.sqldw") \
      .option("url", AKVClient.jdbcUrl()) \
      .option("user", AKVClient.jdbcUsername()) \
      .option("password", AKVClient.jdbcPassword()) \
      .option("tempDir", temp_dir) \
      .option("forward_spark_azure_storage_credentials", "true") \
      .option("dbtable", f"dbo.{dw_table}") \
      .mode(write_mode) \
      .save()
    
    print(f"Table {dw_table} created in synapse database")

  @classmethod
  def read(cls, query:str):
    """Reads table from Synapse in form of a spark dataframe."""
    #connect to adls
    cls.connect_to_adls(cls)

    container_name = "sqltmp"
    temp_dir = f"abfss://{container_name}@{AKVClient.adlsName()}.dfs.core.windows.net/sqldw"

    #get tables in synapse database along with their schema 
    db_tables = SqlDWClient.spark.read \
      .format("com.databricks.spark.sqldw") \
      .option("url", AKVClient.jdbcUrl()) \
      .option("user", AKVClient.jdbcUsername()) \
      .option("password", AKVClient.jdbcPassword()) \
      .option("tempDir", temp_dir) \
      .option("forward_spark_azure_storage_credentials", "true") \
      .option("Query", "select * from information_schema.tables") \
      .load()

    #create a list of tables that exist in syanpse
    db_table_names = [f"{name.TABLE_SCHEMA}.{name.TABLE_NAME}" for name in db_tables.select("TABLE_SCHEMA", "TABLE_NAME").collect()]

    #if all tables in the query exist then perform read otherwise print the table names that do not exist 
    if not all(table in db_table_names for table in cls.get_tables(cls, query)):
      for tbl in cls.get_tables(cls, query):
        if tbl not in db_table_names:
          print(f"Table does not exist : {tbl}")
    else: 
      return SqlDWClient.spark.read \
        .format("com.databricks.spark.sqldw") \
        .option("url", AKVClient.jdbcUrl()) \
        .option("user", AKVClient.jdbcUsername()) \
        .option("password", AKVClient.jdbcPassword()) \
        .option("tempDir", temp_dir) \
        .option("forward_spark_azure_storage_credentials", "true") \
        .option("Query", query) \
        .load()
        

  def get_tables(self, query):
    
    # splits query string on 'FROM'
    # removes the first element in the list as it is a 'select' clause
    tables = re.split(self.create_combinations(self, 'FROM'), query)[1:]

    table_list = []
    
    #gets a list of tables mentioned in the query 
    for table in tables:

      #removes select if it is in the parsed string
      if 'select' in table.lower():
        table = re.split(self.create_combinations(self, 'SELECT'), table)[0]
      #gets tables listed after join
      if 'join' in table.lower():
        tbls_after_join = re.split(self.create_combinations(self, 'JOIN'), table)[1:]
        for tb in tbls_after_join:
          if len(tb.strip().split(" ")) > 1 and ('.' in tb.strip().split(" ")[0]):
            table_list.append(tb.strip().split(" ")[0])  

      #gets table names out of each parsed string
      #splits each parsed string on a comma then check for below conditions
      #as the below conditions are met the table name is added to the list od tables mentioned in the query provided by the user
      for tbl in re.split(",|, | , | ,", table.strip()):
        #extracts table from a string with only one table name ex. dbo.test_tbl
        if len(tbl.strip().split(" ")) == 1 and ('.' in tbl.strip().split(" ")[0]):
          table_list.append(tbl.strip().split(" ")[0])
        #extracts table from a string with only one table name and a letter/word that renames it ex. dbo.test_tbl a
        elif len(tbl.strip().split(" ")) == 2 and ('.' in tbl.strip().split(" ")[0]):
          table_list.append(tbl.strip().split(" ")[0])
        #extracts table from a string with only one table name and renamed with as and then a letter/word that renames it ex. dbo.test_tbl as a
        elif (len(tbl.strip().split(" ")) == 3) and (" as " in tbl) and ('.' in tbl.strip().split(" ")[0]):
          table_list.append(tbl.strip().split(" ")[0])
        #extracts table from a string with only one table name and remaning query it ex. dbo.test_tbl WHERE column='Y'
        #this can be found after all tables have been listed after 'FROM' so break the loop here
        elif (len(tbl.strip().split(" ")) == 3) and (" as " not in tbl) and ('.' in tbl.strip().split(" ")[0]):
          table_list.append(tbl.strip().split(" ")[0])
          break
        #if none of the above conditions are met satisfied and the first element in the string contains '.' add it to list  
        else:
          if '.' in tbl.strip().split(" ")[0]:
            table_list.append(tbl.strip().split(" ")[0])

    #returns a list of tables
    return table_list

  def create_combinations(self, word:str):
    #create uppercase and lowercase combinations of a 'word' to cover various styles of user inputs
    combinations=[''.join(x) for x in permutations(list(word.lower())+list(word.upper()),len(word)) if ''.join(x).lower()==word.lower()]

    #join combinations with '|'
    combos = ""
    for combo in combinations:
        combos+='|'+combo
    
    return combos[1:]
