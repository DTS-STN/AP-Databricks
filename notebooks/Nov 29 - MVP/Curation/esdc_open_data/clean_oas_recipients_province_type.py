# Databricks notebook source
# MAGIC %run ../setup/mount_client

# COMMAND ----------

MountClient(container_name="esdc-open-data").mount()

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import regexp_replace, regexp_extract, col, concat_ws, split, lit, sum, avg, round, first, when, lag, isnull, to_timestamp, date_format
from pyspark.sql.window import Window


# COMMAND ----------

source_file_path = "/mnt/stsaebdevca01/esdc-open-data/oas/number_of_persions_receiving_benefits_by_province_and_type.csv"
df = spark.read \
          .option("header", True) \
          .csv(source_file_path)


# COMMAND ----------

# Use pandas to delete the first couple rows
df_pd = df.toPandas()
df_pd = df_pd.iloc[2: , :]

# Go back to using spark and delete pandas frame
df = spark.createDataFrame(df_pd)
del df_pd


# COMMAND ----------

# Rename all column headers
df = (df
        .withColumnRenamed("Period", "period")
        .withColumnRenamed("Province", "province")
        .withColumnRenamed("Old Age Security Pension", "old_age_sec_pension")
        .withColumnRenamed("Guaranteed Income Supplement", "guaranteed_income_supp")
        .withColumnRenamed(" Allowance ", "allowance")
        .withColumnRenamed("GIS as % OAS", "gis_as_percent_of_oas"))

# Remove commas from strings that represent number of recipients and convert string percentage to float
num_regex = "(\d+.\d+)"
df = (df
        .withColumn("old_age_sec_pension", regexp_replace("old_age_sec_pension", ",", "").cast(IntegerType()))
        .withColumn("guaranteed_income_supp", regexp_replace("guaranteed_income_supp", ",", "").cast(IntegerType()))
        .withColumn("allowance", regexp_replace("allowance", ",", "").cast(IntegerType()))
        .withColumn("gis_as_percent_of_oas", regexp_extract(col("gis_as_percent_of_oas"), num_regex, 1).cast(DoubleType())))


# COMMAND ----------

# MAGIC %run ../utils/month_string_to_num

# COMMAND ----------

# Split date into year and month and reformat period column

regex = "^(\w+)(.+?)(\d+)"
df = (df
       .withColumn('period', concat_ws('-', 
                                       month_string_to_num(lit(regexp_extract(col('period'), regex, 1))), 
                                       regexp_extract(col('period'), regex, 3)))
       .withColumn('year', split(col('period'), '-')[1].cast(IntegerType()))
       .withColumn('month', split(col('period'), '-')[0]))


# COMMAND ----------

# MAGIC %run ../utils/provinces

# COMMAND ----------

# Clean up province/territory names

df_clean = df
for prov in PROVINCES.keys():
  regex = f"^({prov})(.*)"
  df_clean = df_clean.withColumn("province", regexp_replace("province", regex, PROVINCES[prov]))
  
df_clean = (df_clean.withColumn("province", regexp_replace("province", "INTERNATIONAL AGREEMENTS /ACCORDS INTERNATIONAUX", "International Agreements")))
  

# COMMAND ----------

df_totals = (df_clean
               .groupBy(["year", "month"])
               .agg(sum("old_age_sec_pension").alias("old_age_sec_pension"), 
                    sum("guaranteed_income_supp").alias("guaranteed_income_supp"), 
                    sum("allowance").alias("allowance"),
                    first("period").alias("period"),
                    round(avg("gis_as_percent_of_oas"), 2).alias("gis_as_percent_of_oas"))
               .withColumn("province", lit("TOTAL"))
               .withColumn("sort_id", lit(2)))

df_all = df_clean.withColumn("sort_id", lit(1))

df_final = (df_all
              .unionByName(df_totals)
              .sort("year", "month", "sort_id") \
              .drop('sort_id'))


# COMMAND ----------

# Using Pandas, flip the table using melt so the benefits are together in one column

pandas_df = df_final.toPandas()
pandas_df = pandas_df.melt(id_vars = ["period", "province", "year", "month", "gis_as_percent_of_oas"], var_name="benefit", value_name="recipients")

# Go back to using PySpark
df_melted = spark.createDataFrame(pandas_df)
del pandas_df

df_final = (df_melted
              .withColumn("benefit", when(col("benefit") == "allowance", "ALS")
                                     .when(col("benefit") == "old_age_sec_pension", "OAS")
                                     .when(col("benefit") == "guaranteed_income_supp", "GIS")))

# COMMAND ----------

df_final = (df_final
              .withColumn("gis_as_percent_of_oas", 
                          when(col("benefit") != "GIS", None)
                          .otherwise(col("gis_as_percent_of_oas"))))

# COMMAND ----------

# Get month over month changes

changes_window = Window.partitionBy("province", "benefit").orderBy("year", "month")
df_mom = (df_final
            .withColumn("prev_count", 
                        lag(df_final.recipients)
                        .over(changes_window)))

df_mom = (df_mom
            .withColumn("recipients_mom_change_pct", 
                        when(isnull(df_mom.recipients - df_mom.prev_count), 0)
                        .otherwise(round((df_mom.recipients - df_mom.prev_count) / df_mom.prev_count, 3)))
            .drop("prev_count"))


# COMMAND ----------

# MAGIC %run ../setup/sqldw_client

# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(df_mom, "OAS_Recipients_ProvType")

# Write to adls as csv so we can read and perform joins with other dataframes
df_mom.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/esdc-open-data/OAS_Recipients_ProvType.csv")

# COMMAND ----------

df_summ = (df_final.groupBy("province", "benefit", "year")
          .agg(sum("recipients").alias("recipients"),
               round(avg("recipients"), 1).alias("recipients_avg")))


# COMMAND ----------

df_sorted = df_summ.sort("province", "benefit", "year")

# Get year over year changes

yoy_window = Window.partitionBy("province", "benefit").orderBy("year")
df_summ = (df_summ
            .withColumn("prev_count", 
                        lag(df_summ.recipients_avg)
                        .over(yoy_window)))

df_summ = (df_summ
            .withColumn("recipients_yoy_avg_change_pct", 
                        when(isnull(df_summ.recipients_avg - df_summ.prev_count), 0)
                        .otherwise(round((df_summ.recipients_avg - df_summ.prev_count) / df_summ.prev_count, 3)))
            .drop("prev_count"))


# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(df_summ, "OAS_Recipients_ProvType_summary")

# Write to adls as csv so we can read and perform joins with other dataframes
df_summ.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/esdc-open-data/OAS_Recipients_ProvType_summary.csv")