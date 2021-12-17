# Databricks notebook source
# MAGIC %run ../setup/mount_client

# COMMAND ----------

MountClient(container_name="statscan").mount()
MountClient(container_name="saebcurated").mount()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType
from pyspark.sql.functions import col, year, month, to_timestamp, round, sum, avg, lag, when, isnull, date_format
from pyspark.sql.window import Window


# COMMAND ----------

lfs_schema = StructType(fields=[StructField("REF_DATE", DateType(), False),
                                StructField("GEO", StringType(), False),
                                StructField("DGUID", StringType(), False),
                                StructField("Labour force characteristics", StringType(), False),
                                StructField("Sex", StringType(), False),
                                StructField("Age group", StringType(), False),
                                StructField("UOM", StringType(), False),
                                StructField("UOM_ID", IntegerType(), False),
                                StructField("SCALAR_FACTOR", StringType(), False),
                                StructField("SCALAR_ID", IntegerType(), False),
                                StructField("VECTOR", StringType(), False),
                                StructField("COORDINATE", StringType(), False),
                                StructField("VALUE", DoubleType(), False),
                                StructField("STATUS", StringType(), False),
                                StructField("SYMBOL", StringType(), False),
                                StructField("TERMINATED", StringType(), False),
                                StructField("DECIMALS", IntegerType(), False)])


# COMMAND ----------

source_file_path = "/mnt/stsaebdevca01/statscan/labour-force/input/14100017.csv"

df = spark.read \
          .option("header", True) \
          .schema(lfs_schema) \
          .csv(source_file_path)


# COMMAND ----------

selected = (df.select(col("REF_DATE").alias("ref_date"), 
                      col("Labour force characteristics").alias("labour_force_chars"),
                      col("GEO").alias("geo"), 
                      col("Sex").alias("sex"), 
                      col("Age group").alias("age_group"),
                      col("VALUE").alias("population"),
                      col("STATUS").alias("status"))
             .withColumn("year", year(to_timestamp(col("ref_date"))))
             .withColumn("month", month(to_timestamp(col("ref_date"))))
             .withColumn("date_2", date_format(to_timestamp(col("ref_date")), "MM-yyyy"))
             .withColumn("population", col("population") * 1000))

lfs_sc = (selected.filter(((selected.year > 2016) | 
                          ((selected.month >= 5) & (selected.year == 2016))) &
                          (selected.labour_force_chars == "Population") &
                          ((selected.status != 'x') | (selected.status.isNull())))
                  .drop("ref_date")
                  .drop("labour_force_chars"))


# COMMAND ----------

# MAGIC %run ../setup/sqldw_client

# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(lfs_sc, "1410001701_Pop_LFS_SC")

# Write to adls as csv so we can read and perform joins with other dataframes
lfs_sc.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/statscan/1410001701_Pop_LFS_SC.csv")

# COMMAND ----------

# MAGIC %md ### Create Summary Table

# COMMAND ----------

lfs_filtered = (lfs_sc
                  .filter((selected.age_group == "65 years and over") & 
                          (selected.sex == "Both sexes")))
  

# COMMAND ----------

lfs_grouped = (lfs_filtered
                 .groupBy("year", "geo")
                 .agg(sum("population").alias("total_pop"),
                      round(avg("population"),1).alias("avg_monthly_pop"))
                 .orderBy("year", "geo"))


# COMMAND ----------

pop_window = Window.partitionBy("geo").orderBy("year")
lfs_sum = (lfs_grouped
             .withColumn("prev_pop",
                         lag(lfs_grouped.avg_monthly_pop)
                         .over(pop_window)))
lfs_sum = (lfs_sum
             .withColumn("year_over_year_change",
                         when(isnull(lfs_sum.avg_monthly_pop - lfs_sum.prev_pop), 0)
                         .otherwise(round((lfs_sum.avg_monthly_pop - lfs_sum.prev_pop) / lfs_sum.prev_pop, 3)))
             .drop("prev_pop"))


# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(lfs_sum, "1410001701_Pop_LFS_Sum")

# Write to adls as csv so we can read and perform joins with other dataframes
lfs_sum.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/statscan/1410001701_Pop_LFS_Sum.csv")