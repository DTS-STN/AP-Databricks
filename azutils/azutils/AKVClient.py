from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


class AKVClient:

    """ 
    This class allows the user/developer to access Azure Key Vault Secrets 
    without knowing the databricks secret scope name and azure key vault secrets.
    """

    # get or create spark session 
    spark = SparkSession.builder.getOrCreate()
    # access DBUtils module 
    dbutils = DBUtils(spark)

    # class takes no parameters
    def __init__(self):
       pass

    # Below are the class methods that can be called to return secrets from azure key vault secrets  
    @classmethod
    def dbcSecretScopeName(cls):
       """
       Provides access to Databricks Secret Scope Name.
       """
       secretScopeName = "akv-saeb-dbc-scrt-scp"
       return cls.dbutils.secrets.get(scope = secretScopeName, key = "dbc-secret-scope")

    @classmethod
    def jdbcServerName(cls):
        """
        Provides access to Azure Synapse SQL Dedicated Pool Server Name.
        """
        return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "sqlpool01-servername")
   
    @classmethod
    def jdbcDatabase(cls):
       """
       Provides access to Azure Synapse SQL Dedicated Pool Database Name.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "sqlpool01-dbname")

    @classmethod
    def jdbcUsername(cls):
       """
       Provides access to Azure Synapse SQL Dedicated Pool Username.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "sqlpool01-user")
   
    @classmethod
    def jdbcPassword(cls):
       """
       Provides access to Azure Synapse SQL Dedicated Pool Password.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "sqlpool01-pwd")
   
    @classmethod
    def jdbcHost(cls):
       """
       Provides access to Azure Synapse SQL Dedicated Pool JDBC Host.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "sqlpool01-host")
   
    @classmethod
    def jdbcPort(cls):
       """
       Provides access to Azure Synapse SQL Dedicated Pool JDBC Port.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "sqlpool01-port")
    
    @classmethod
    def jdbcUrl(cls):
       """
       Provides access to Azure Synapse SQL Dedicated Pool JDBC URL.
       """
       extra_options = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"
       return f"jdbc:sqlserver://{cls.jdbcServerName()}:{cls.jdbcPort()};database={cls.jdbcDatabase()};{extra_options};"
   
    @classmethod
    def statscanPassword(cls):
       """
       Provides Password to access to Government of Canada API Store.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "statscan-password")

    @classmethod
    def statscanUsername(cls):
       """
       Provides Username to access to Government of Canada API Store.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "statscan-username")    
   
    @classmethod
    def saebDbwAdfToken(cls):
       """
       Provides access to Databricks Token for Azure Data Factory Connection.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "saebdbwadftoken")
   
    @classmethod
    def adlsAccessKey(cls):
       """
       Provides access to Azure Data Lake Storage Access Key.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "saebadlsstorageaccesskey")
   
    @classmethod
    def adlsConnectionString(cls):
       """
       Provides access to Azure Data Lake Storage Connection String.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "saebadlsstorage-connection-string")
   
    @classmethod
    def adlsName(cls):
       """
       Provides access to Azure Data Lake Storage Name.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "saebadlsstoragename")

    @classmethod
    def adobeAnalyticsClientID(cls):
       """
       Provides access to Adobe Analytics Client ID.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "aa-client-id")
   
    @classmethod
    def adobeAnalyticsClientSecret(cls):
       """
       Provides access to Adobe Analytics Client Secret.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "aa-client-secret")
   
    @classmethod
    def adobeAnalyticsGlobalCompanyID(cls):
       """
       Provides access to Adobe Analytics Global Company ID.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "aa-global-company-id")
   
    @classmethod
    def adobeAnalyticsOrgID(cls):
       """
       Provides access to Adobe Analytics Organization ID.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "aa-org-id")

    @classmethod
    def adobeAnalyticsPrivateKey(cls):
       """
       Provides access to Adobe Analytics Private Key.
       """
       # returns private key in bytes format
       private_key =  cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "aa-private-key")
       return ("-----BEGIN PRIVATE KEY-----\n"+private_key+"\n-----END PRIVATE KEY-----").encode()
   
    @classmethod
    def adobeAnalyticsReportSuiteID(cls):
       """
       Provides access to Adobe Analytics Report Suite ID.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "aa-report-suite-id")
   
    @classmethod
    def adobeAnalyticsSubjectAccount(cls):
       """
       Provides access to Adobe Analytics Subject Account.
       """
       return cls.dbutils.secrets.get(scope = cls.dbcSecretScopeName(), key = "aa-subject-account") 
