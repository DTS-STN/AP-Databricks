# Databricks notebook source
secret_scope_name = "akv-saeb-dbc-scrt-scp" # kv-saeb-dev-01 key vault

# ADLS Connection Properties
account_name = "stsaebdevca01"
container_name = "sqltmp"
storage_account_key = dbutils.secrets.get(scope = secret_scope_name, key = "saebadlsstorageaccesskey")
temp_dir = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/sqldw"

# Azure SQL Connection Properties
jdbc_hostname = dbutils.secrets.get(scope = secret_scope_name, key = "sqlpool01-servername")
jdbc_database = dbutils.secrets.get(scope = secret_scope_name, key = "sqlpool01-dbname")

jdbc_username = dbutils.secrets.get(scope = secret_scope_name, key = "sqlpool01-user") 
jdbc_password = dbutils.secrets.get(scope = secret_scope_name, key = "sqlpool01-pwd") 

jdbc_port = "1433"
extra_options = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"

jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};database={jdbc_database};{extra_options}"