# Databricks notebook source
# MAGIC %run ../setup/mount_client

# COMMAND ----------

MountClient(container_name="adobeanalytics").mount()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType, StringType
from pyspark.sql.functions import translate, regexp_extract, when


# COMMAND ----------

schema = StructType(fields=[StructField("_c0", IntegerType(), False),
                            StructField("itemId_lvl_1", IntegerType(), False),
                            StructField("value_lvl_1", StringType(), False),
                            StructField("itemId_lvl_2", LongType(), False),
                            StructField("value_lvl_2", StringType(), False),
                            StructField("itemId_lvl_3", IntegerType(), False),
                            StructField("value_lvl_3", IntegerType(), False),
                            StructField("metrics/clickmaplinkinstances", DoubleType(), False)])


# COMMAND ----------

source_file_path = "/mnt/stsaebdevca01/adobeanalytics/oas/oas-takeup/oas_takeup_internal_link_clicks.csv"
df = spark.read \
          .option("header", True) \
          .schema(schema) \
          .csv(source_file_path)


# COMMAND ----------

# Rename columns

df = (df
     .withColumnRenamed("value_lvl_1", "page_title")
     .withColumnRenamed("value_lvl_2", "activity_map_link")
     .withColumnRenamed("value_lvl_3", "year")
     .withColumnRenamed("metrics/clickmaplinkinstances", "link_click_instances")
     .withColumnRenamed("itemId_lvl_1", "page_title_id")
     .withColumnRenamed("itemId_lvl_2", "link")
     .withColumnRenamed("itemId_lvl_3", "year_id")
     .drop("_c0"))


# COMMAND ----------

# Add language column based on page_title. English(en) or French(fr)

eng_regex = "^(Old)"
eng_match = regexp_extract(df.page_title, eng_regex, 1) == "Old"
language = when(eng_match, "en").otherwise("fr")

df = df.withColumn("language", language)


# COMMAND ----------

# Remove diacritics

chars_from = "é"
chars_to   = "e"

df = df.withColumn("page_title", translate(df.page_title, chars_from, chars_to))

# COMMAND ----------

# MAGIC %run ../setup/sqldw_client

# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(df, "AA_OAS_Apply_LinkClicks")

# Write to adls as csv so we can read and perform joins with other dataframes
df.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/adobeanalytics/AA_OAS_Apply_LinkClicks.csv")