# Azutils Library

Azutils is a python library that allows developers to access data in Azure Storage Account and Azure Synapse Database without exposing to them or sharing with them the secrets/keys required to build connection with these services. It is specifically designed for databricks environment.


The modules of Azutils are:

- **`AKVClient`**
- **`SqlDWClient`**
- **`MountClient`**



#### _class_ `azutils.AKVClient.AKVClient`

>>AKVClient can be used to fetch secrets from Azure Key Vault via the Databbricks secret scope which redacts the secret values so they are not visible to the developer, acting like an additional security layer for the secrets.

>>```python
>>AKVClient.adlsAccessKey()
>>```

>>**`jdbcServerName()`**
>>>>Returns Azure Synapse SQL Dedicated Pool server name.

>>**`jdbcDatabase()`**
>>>>Returns Azure Synapse SQL Dedicated Pool database name.
       
>>**`jdbcUsername()`**
>>>>Return Azure Synapse SQL Dedicated Pool username.

>>**`jdbcPassword()`**
>>>>Returns Azure Synapse SQL Dedicated Pool password.

>>**`jdbcHost()`**
>>>>Returns access to Azure Synapse SQL Dedicated Pool JDBC host.

>>**`jdbcPort()`**
>>>>Returns Azure Synapse SQL Dedicated Pool JDBC port.

>>**`jdbcUrl()`**
>>>>Returns Azure Synapse SQL Dedicated Pool JDBC URL.

>>**`statscanPassword()`**
>>>>Retuns password to access to Government of Canada API Store.

>>**`statscanUsername()`**
>>>>Returns username to access to Government of Canada API Store.

>>**`saebDbwAdfToken()`**
>>>>Returns Databricks Token for building connection to Azure Data Factory.

>>**`adlsAccessKey()`**
>>>>Returns Azure Data Lake Storage account access key.

>>**`adlsConnectionString()`**
>>>>Returns Azure Data Lake Storage Connection String.
       
>>**`adlsName()`**
>>>>Returns Azure Data Lake Storage Name.
       
>>**`adobeAnalyticsClientID()`**
>>>>Returns Adobe Analy`tics Client ID.

>>**`adobeAnalyticsCLientSecret()`**
>>>>Returns Adobe Analytics Client Secret.

>>**`adobeAnalyticsGlobalCompanyID()`**
>>>>Returns Adobe Analytics Global Company ID.

>>**`adobeAnalyticsOrgID()`**
>>>>Returns Adobe Analytics Organization ID.

>>**`adobeAnalyticsPrivateKey()`**
>>>>Returns Adobe Analytics Private Key.

>>**`adobeAnalyticsReportSuiteID()`**
>>>>Returns Adobe Analytics Report Suite ID.

>>**`adobeAnalyticsSubjectAccount()`**
>>>>Returns Adobe Analytics Subject Account.

>>**`dbcSecretScopeName()`**
>>>>Returns Databricks Secret Scope Name.

#### _class_ `azutils.SqlDWClient.SqlDWClient`

>>SqlDWClient can be used to read from and write to Azure Synape Dedicated SQL Pool (aka Azure SQL DW) by building connection to the Dedicated SQL Pool

>>**`write(df, table, mode='overwrite')`**
>>>>Saves the content of the dataframe in a specified table in Azure Synapse Analytics.

>>>>**Parameters:**
>>>>* **df** - dataframe
>>>>* **table : _str_** - string, name of table in Synapse Analytics
>>>>* **mode : _str_** - string, behavior when data or table already exists, default is _overwrite_

>>>>>>Options include: 
>>>>>>* _append_: append contents of this dataframe to existing data
>>>>>>* _overwrite_: overwrite existing data
>>>>>>* _error_ or _errorifexists_: Throw an exception if data already exists
>>>>>>* _ignore_: silently ignore this operation if data already exists

>>```python
>>SqlDWClient.write(df, "test_tbl")
>>```

>>**`read(query)`**
>>>>Loads SQL query results from Azure Synapse Analtyics and returns the result as a dataframe. Before loading data from Synapse tables, the function checks if the tables mentioned in the query exist in Synapse Analytics.

>>>>**Parameters: query : _str_** - string, SQL query to read data from Synapse Analytics tables

>>```python
>>df = SqlDWClient.read("SELECT * FROM dbo.test_tbl")
>>display(df)
>>```

#### _class_ `azutils.MountClient.MountClient`

>>MountClient can be used to mount and unmount Azure Storage containers in Databricks as well as read from and write to the mounted containers.

>>**`MountClient`** instances can be created by:
>>```python
>>MountClient(container="test")
>>```
>>Instances are not required to use the **`read`** and **`write`** method of this class.

>>**`mount()`**
>>>>Mounts container if it is not mounted and exists in Azure Storage account.

>>```python
>>MountClient(container="test").mount()
>>```

>>**`unmount()`**
>>>>Unmounts container if it is mounted.

>>```python
>>MountClient(container="test").unmount()
>>```

>>**`read(source_path, source_format, schema, options)`**
>>>>Loads specified file and returns the result as a dataframe. Before loading the file, the function checks to see if the specified path is correct and if the container mentioned in the path is mounted.

>>>>**Parameters:**
>>>>* **source_path : _str_** - string, input file path in mounted container
>>>>* **source_format : _str_** - string, format of input file e.g. 'json', 'parquet', 'csv'
>>>>* **schema :  an optional ```pyspark.sql.types.StructType``` for the input schema
>>>>* **options : _dict_** - dictionary, input options e.g. ```{"header": True}``` for a csv file

>>```python
>>source_path = "/mnt/<storage_account>/<container>/population-projections/input/17100057.csv"
>>df = MountClient.read(source_path, "csv")
>>display(df)
>>```

>>**`write(df, output_path, output_format, mode='overwrite', options)`**
>>>>Saves the content of the dataframe in a specified format at the specified path. Before saving the file, the function checks to see if the specified path is correct and if the container mentioned in the path is mounted.

>>>>**Parameters:**
>>>>* **df** - dataframe
>>>>* **path : _str_** - string, output file path in mounted container
>>>>* **format : _str_** - string, format of destination file e.g. 'json', 'parquet', 'csv'
>>>>* **mode : _str_** - string, behavior when data or table already exists, default is _overwrite_
>>>>>>Options include: 
>>>>>>* _append_: append contents of this dataframe to existing data
>>>>>>* _overwrite_: overwrite existing data
>>>>>>* _error_ or _errorifexists_: Throw an exception if data already exists
>>>>>>* _ignore_: silently ignore this operation if data already exists
>>>>* **options : _dict_** - dictionary, ouput options e.g. ```{"header": True}``` for a csv file

>>```python
>>output_path = "/mnt/<storage_account>/<container>/statscan/Pop_Projections_test"
>>MountClient.write(df, output_path, "parquet")
>>```
