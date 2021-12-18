Azutils is a python library that allows developers to access data in Azure Storage Account and Azure Synapse Database without exposing to them or sharing with them the secrets/keys required to build connection with these services. It is specifically designed for databricks environment.

The package consits of three classes:

- **`AKVClient`**: allows to fetch secrets from Azure Key Vault via the Databbricks secret scope which redacts the secret values so they are not visible to the developer, acting like an additional security layer for the secrets
- **`SqlDWClient`**: allows to read from and write to Azure Synape Dedicated SQL Pool (aka Azure SQL DW) by building connection to the Dedicated SQL Pool
- **`MountClient`**: allows to mount and unmount Azure Storage containers in Databricks as well as read from and write to the mounted containers


_class_ `AKVClient`

**jdbcServerName**()

Returns Azure Synapse SQL Dedicated Pool server name.

**jdbcDatabase**()

Returns Azure Synapse SQL Dedicated Pool database name.
       
**jdbcUsername**()

Return Azure Synapse SQL Dedicated Pool username.

**jdbcPassword**()

Returns Azure Synapse SQL Dedicated Pool password.

**jdbcHost**()

Returns access to Azure Synapse SQL Dedicated Pool JDBC host.

**jdbcPort**()
       
 Returns Azure Synapse SQL Dedicated Pool JDBC port.

**jdbcUrl**()

Returns Azure Synapse SQL Dedicated Pool JDBC URL.

**statscanPassword**()

Retuns password to access to Government of Canada API Store.

**statscanUsername**()
Returns username to access to Government of Canada API Store.

**saebDbwAdfToken**()
 
 Returns Databricks Token for building connection to Azure Data Factory.

**adlsAccessKey**()

Returns Azure Data Lake Storage account access key.

**adlsConnectionString**()

Returns Azure Data Lake Storage Connection String.
       
**adlsName**()

Returns Azure Data Lake Storage Name.
       
**adobeAnalyticsClientID**()

Returns Adobe Analytics Client ID.

**adobeAnalyticsCLientSecret**()

Returns Adobe Analytics Client Secret.

**adobeAnalyticsGlobalCompanyID**()

Returns Adobe Analytics Global Company ID.

**adobeAnalyticsOrgID**()

Returns Adobe Analytics Organization ID.

**adobeAnalyticsPrivateKey**()

Returns Adobe Analytics Private Key.

**adobeAnalyticsReportSuiteID**()

Returns Adobe Analytics Report Suite ID.

**adobeAnalyticsSubjectAccount**()

Returns Adobe Analytics Subject Account.

**dbcSecretScopeName**()

Returns Databricks Secret Scope Name.
