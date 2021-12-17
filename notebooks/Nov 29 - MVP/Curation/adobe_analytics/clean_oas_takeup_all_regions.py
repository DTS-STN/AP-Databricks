# Databricks notebook source
# MAGIC %run ../setup/mount_client

# COMMAND ----------

MountClient(container_name="adobeanalytics").mount()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType, StringType
from pyspark.sql.functions import regexp_extract, split, to_timestamp, when, lag, isnull, round, sum, avg, first, round, date_format
from pyspark.sql.window import Window


# COMMAND ----------

schema = StructType(fields=[StructField("_c0", IntegerType(), False),
                            StructField("itemId_lvl_1", LongType(), False),
                            StructField("value_lvl_1", StringType(), False),
                            StructField("itemId_lvl_2", LongType(), False),
                            StructField("value_lvl_2", StringType(), False),
                            StructField("itemId_lvl_3", IntegerType(), False),
                            StructField("value_lvl_3", StringType(), False),
                            StructField("metrics/visitors", DoubleType(), False),
                            StructField("metrics/visits", DoubleType(), False)])


# COMMAND ----------

source_file_path = "/mnt/stsaebdevca01/adobeanalytics/oas/oas-takeup/oas_takeup_all_regions.csv"
df = spark.read \
          .option("header", True) \
          .schema(schema) \
          .csv(source_file_path)


# COMMAND ----------

# Rename columns and drop the index column

df = (df
     .withColumnRenamed("value_lvl_1", "page_url")
     .withColumnRenamed("value_lvl_2", "region")
     .withColumnRenamed("value_lvl_3", "date")
     .withColumnRenamed("metrics/visitors", "unique_visitors")
     .withColumnRenamed("metrics/visits", "visits")
     .withColumnRenamed("itemId_lvl_1", "page_url_id")
     .withColumnRenamed("itemId_lvl_2", "region_id")
     .withColumnRenamed("itemId_lvl_3", "date_id")
     .drop("_c0"))


# COMMAND ----------

# Add country column based on region

regex = "\((.*?)\)"
df = df.withColumn("country", regexp_extract(df.region, regex, 1))


# COMMAND ----------

# Split date into month and year columns. Keep the original date column and convert it to a date object so we can sort by it.

split_col = split(df.date, ' ')
date_col = to_timestamp(df.date, 'MMM yyyy')

df = (df
        .withColumn("month", split_col[0])
        .withColumn("year", split_col[1].cast(IntegerType()))
        .withColumn("date", date_col))


# COMMAND ----------

# Add an additional date column to be more flexible in powerBI

df = df.withColumn("date_2", date_format(df.date, 'MM-yyyy'))

# COMMAND ----------

# Add language column based on page_url. English(en) or French(fr)

lg_regex = "\/(fr|en?)\/"
df = df.withColumn("language", regexp_extract(df.page_url, lg_regex, 1))


# COMMAND ----------

# Add new page_title column that determines page title based on language

page_column = when(df.language == 'en', "Old Age Security: Your application").when(df.language == 'fr', "Pension de la Sécurité de vieillesse : Votre demande").otherwise('')
df = df.withColumn("page_title", page_column)


# COMMAND ----------

# Group languages together to get totals for both

df = (df
        .groupBy("region", "date")
        .agg(sum("unique_visitors").alias("unique_visitors"),
             sum("visits").alias("visits"),
             first("region_id").alias("region_id"),
             first("date_id").alias("date_id"),
             first("country").alias("country"),
             first("month").alias("month"),
             first("year").alias("year"),
             first("date_2").alias("date_2")))


# COMMAND ----------

# Add column to capture month over month percentage changes for unique visitors and visits

mom_change_window = Window.partitionBy("region").orderBy("date")

# Unique visitors
df = df.withColumn("prev_month_uniq_visitors", lag(df.unique_visitors).over(mom_change_window))
df = (df
       .withColumn("mom_change_uniq_visitors", 
                   when(isnull(df.unique_visitors - df.prev_month_uniq_visitors), 0)
                   .otherwise(round((df.unique_visitors - df.prev_month_uniq_visitors) / df.prev_month_uniq_visitors, 3))))

# Visits
df = df.withColumn("prev_month_visits", lag(df.visits).over(mom_change_window))
df = (df
       .withColumn("mom_change_visits", 
                   when(isnull(df.visits - df.prev_month_visits), 0)
                   .otherwise(round((df.visits - df.prev_month_visits) / df.prev_month_visits, 3))))

# Drop columns that were used for above calculation but are not required in final version
df = df.drop("prev_month_uniq_visitors", "prev_month_visits")


# COMMAND ----------

df_dropped = df.sort("region", "date").drop("region_id", "date_id")

# COMMAND ----------

# MAGIC %run ../setup/sqldw_client

# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(df_dropped, "AA_OAS_Apply_Regions_Monthly")

# Write to adls as csv so we can read and perform joins with other dataframes
df_dropped.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/adobeanalytics/AA_OAS_Apply_Regions_Monthly.csv")

# COMMAND ----------

# Create summary table
# Total unique visitors in a year per country
# Average unique visitors per region in a given year

df_country = (df_dropped
                .groupBy("country", "year")
                .agg(sum("unique_visitors").alias("total_unique_visitors"),
                     round(avg("unique_visitors"), 1).alias("avg_unique_visitors")))


# COMMAND ----------

display(df_country)

# COMMAND ----------

# Add column to capture year over year percentage changes for unique visitors by country and year

yoy_change_window = Window.partitionBy("country").orderBy("year")

# Unique visitors change in totals
df_country = df_country.withColumn("prev_year_total_uniq_visitors", lag(df_country.total_unique_visitors).over(yoy_change_window))
df_country = (df_country
                .withColumn("yoy_change_total_uniq_visitors", 
                            when(isnull(df_country.total_unique_visitors - df_country.prev_year_total_uniq_visitors), 0)
                            .otherwise(round((df_country.total_unique_visitors - df_country.prev_year_total_uniq_visitors) / df_country.prev_year_total_uniq_visitors, 3))))

# Unique visitors change in average
df_country = df_country.withColumn("prev_year_avg_uniq_visitors", lag(df_country.avg_unique_visitors).over(yoy_change_window))
df_country = (df_country
                .withColumn("yoy_change_avg_uniq_visitors", 
                            when(isnull(df_country.avg_unique_visitors - df_country.prev_year_avg_uniq_visitors), 0)
                            .otherwise(round((df_country.avg_unique_visitors - df_country.prev_year_avg_uniq_visitors) / df_country.prev_year_avg_uniq_visitors, 3))))


# Drop columns that were used for above calculation but are not required in final version
df_country = df_country.drop("prev_year_total_uniq_visitors", "prev_year_avg_uniq_visitors")

# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(df_country, "AA_OAS_Apply_Country")

# Write to adls as csv so we can read and perform joins with other dataframes
df_country.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/adobeanalytics/AA_OAS_Apply_Country.csv")

# COMMAND ----------

# Add new columns for unique visitors average and unique visits for each region

df = (df_dropped.groupBy(["region", "year"])
                .agg(round(avg("unique_visitors"), 1).alias("unique_visitors_avg"),
                     round(avg("visits"), 1).alias("visits_avg"),
                     first("country").alias("country")))
    
# Add year over year change
prev_year_avg_window = Window.partitionBy("region").orderBy("year")

# Year to year change in unique visitors average
df = df.withColumn("prev_year_unique_visitors_yearly_avg", lag(df.unique_visitors_avg).over(prev_year_avg_window))
df = (df
       .withColumn("yoy_unique_visitors_avg", 
                   when(isnull(df.unique_visitors_avg - df.prev_year_unique_visitors_yearly_avg), 0)
                   .otherwise(round((df.unique_visitors_avg - df.prev_year_unique_visitors_yearly_avg) / df.prev_year_unique_visitors_yearly_avg, 3))))

# Year to year change in visits average
df = df.withColumn("prev_year_visits_avg", lag(df.visits_avg).over(prev_year_avg_window))
df = (df
       .withColumn("yoy_visits_avg",
                   when(isnull(df.visits_avg - df.prev_year_visits_avg), 0)
                   .otherwise(round((df.visits_avg - df.prev_year_visits_avg) / df.prev_year_visits_avg, 3))))

# Drop columns that were used for above calculation but are not required in final version
df_reg = df.drop("prev_year_unique_visitors_yearly_avg", "prev_year_visits_avg")


# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(df_reg, "AA_OAS_Apply_Regions")

# Write to adls as csv so we can read and perform joins with other dataframes
df_reg.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/adobeanalytics/AA_OAS_Apply_Regions.csv")