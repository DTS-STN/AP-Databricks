# Databricks notebook source
from azutils import MountClient, SqlDWClient
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col

# COMMAND ----------

pop_est_schema = StructType(fields=[StructField("REF_DATE", IntegerType(), False),
                                    StructField("GEO", StringType(), False),
                                    StructField("DGUID", StringType(), False),
                                    StructField("Sex", StringType(), False),
                                    StructField("Age group", StringType(), False),
                                    StructField("UOM", StringType(), False),
                                    StructField("UOM_ID", IntegerType(), False),
                                    StructField("SCALAR_FACTOR", StringType(), False),
                                    StructField("SCALAR_ID", IntegerType(), False),
                                    StructField("VECTOR", StringType(), False),
                                    StructField("COORDINATE", StringType(), False),
                                    StructField("VALUE", IntegerType(), False),
                                    StructField("STATUS", StringType(), False),
                                    StructField("SYMBOL", StringType(), False),
                                    StructField("TERMINATED", StringType(), False),
                                    StructField("DECIMALS", IntegerType(), False)])


# COMMAND ----------

source_file_path = "/mnt/stsaebdevca01/statscan/population-estimates/input/17100005.csv"

# COMMAND ----------

df = MountClient.read(source_file_path, "csv", pop_est_schema, options={"header": True})
display(df)

# COMMAND ----------

df = (df.select(col("REF_DATE").alias("year"),
                col("GEO").alias("geo"),
                col("Sex").alias("sex"), 
                col("Age group").alias("age_group"),
                col("VALUE").alias("population"))
      .filter((col('ref_date') >= 2017) 
              & (col('age_group') == '65 years and over')
             )
     )
display(df)

# COMMAND ----------

# Create dataframe for export
df_pivot = (df
            .groupBy("year", "geo", "age_group")
            .pivot("sex").sum("population")
            .withColumnRenamed("Both sexes", "both_sexes")
            .withColumnRenamed("Females", "females")
            .withColumnRenamed("Males", "males")
            .sort('year')
           )
display(df_pivot)

# COMMAND ----------

# Write to data warehouse
SqlDWClient.write(df_pivot, "17100005_PopEstimates_SC")

# Write to adls as csv so we can read and perform joins with other dataframes
output_path = "/mnt/stsaebdevca01/saebcurated/statscan/17100005_PopEstimates_SC.csv" 
MountClient.write(df_pivot, output_path, "csv", options={"header": True})
