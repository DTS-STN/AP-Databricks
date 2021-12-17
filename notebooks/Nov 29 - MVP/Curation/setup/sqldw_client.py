# Databricks notebook source
# MAGIC %run ./sqldw_settings

# COMMAND ----------

class SqlDWClient:
  def __init__(self):
    self.connect_to_adls()
    
  def connect_to_adls(self):
    spark.conf.set('fs.azure.account.key.' + account_name + '.dfs.core.windows.net', storage_account_key)
  
  @staticmethod
  def write(df, dw_table):
    df.write \
      .format("com.databricks.spark.sqldw") \
      .option("url", jdbc_url) \
      .option("user", jdbc_username) \
      .option("password", jdbc_password) \
      .option("tempDir", temp_dir) \
      .option("forward_spark_azure_storage_credentials", "true") \
      .option("dbtable", f"dbo.{dw_table}") \
      .mode("overwrite") \
      .save()