# Databricks notebook source
# MAGIC %run ../setup/mount_client

# COMMAND ----------

MountClient(container_name="saebcurated").mount()

# COMMAND ----------

from pyspark.sql.functions import lag, when, round, isnull
from pyspark.sql.window import Window


# COMMAND ----------

path_pop_lfs_sum = "/mnt/stsaebdevca01/saebcurated/statscan/1410001701_Pop_LFS_Sum.csv"
pop_lfs_sum = spark.read.option("header", True).option("inferSchema", True).csv(path_pop_lfs_sum)


# COMMAND ----------

display(pop_lfs_sum)

# COMMAND ----------

path_pop_proj_sc =  "/mnt/stsaebdevca01/saebcurated/statscan/171000501_PopProjections_SC.csv"
pop_proj_sc = spark.read.option("header", True).option("inferSchema", True).csv(path_pop_proj_sc)


# COMMAND ----------

display(pop_proj_sc)

# COMMAND ----------

regex = "( M3:)" # We'll only take the medium growth projection
pop_proj_sc = (pop_proj_sc
                   .filter((pop_proj_sc.age_group == '65 years and over') & 
                           (pop_proj_sc.proj_scenario.rlike(regex)))
                   .drop("age_group", "proj_scenario", "females", "males")
                   .withColumnRenamed("both_sexes", "total_pop"))


# COMMAND ----------

pop_counts_proj_sc = pop_lfs_sum.unionByName(pop_proj_sc, allowMissingColumns = True)

# COMMAND ----------

# The appended data (the projections) does not have the year over year changes captured and is added here

pop_window = Window.partitionBy("geo").orderBy("year")
df = (pop_counts_proj_sc
          .withColumn("prev_pop", 
                      lag(pop_counts_proj_sc.total_pop)
                      .over(pop_window)))
df = (df
         .withColumn("year_over_year_change", 
                     when(isnull(df.total_pop - df.prev_pop), 0)
                     .otherwise(round((df.total_pop - df.prev_pop) / df.prev_pop, 3)))
         .drop("prev_pop"))

# COMMAND ----------

# MAGIC %run ../setup/sqldw_client

# COMMAND ----------

display(df)

# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(df, "Pop_CountsProjections_SC")

# Write to adls as csv so we can read and perform joins with other dataframes
df.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/statscan/Pop_CountsProjections_SC.csv")