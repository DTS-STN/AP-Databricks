# Databricks notebook source
# MAGIC %run ../setup/mount_client

# COMMAND ----------

MountClient(container_name="statscan").mount()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType
from pyspark.sql.functions import col, month, to_timestamp, sum, lit, concat_ws, format_string, lag, when, isnull, round
from pyspark.sql.window import Window

# COMMAND ----------

pop_estimate_schema = StructType(fields=[StructField("REF_DATE", IntegerType(), True),
                                         StructField("GEO", StringType(), True),
                                         StructField("DGUID", StringType(), True),
                                         StructField("Sex", StringType(), True),
                                         StructField("Age group", StringType(), True),
                                         StructField("UOM", StringType(), True),
                                         StructField("UOM_ID", IntegerType(), True),
                                         StructField("SCALAR_FACTOR", StringType(), True),
                                         StructField("SCALAR_ID", IntegerType(), True),
                                         StructField("VECTOR", StringType(), True),
                                         StructField("COORDINATE", StringType(), True),
                                         StructField("VALUE", DoubleType(), True),
                                         StructField("STATUS", StringType(), True),
                                         StructField("SYMBOL", StringType(), True),
                                         StructField("TERMINATED", StringType(), True),
                                         StructField("DECIMALS", IntegerType(), True)])

# COMMAND ----------

source_file_path = "/mnt/stsaebdevca01/statscan/population-estimates/input/17100005.csv"

# COMMAND ----------

df = spark.read \
          .option("header", True) \
          .schema(pop_estimate_schema) \
          .csv(source_file_path)

# COMMAND ----------

selected = (df.select(col("REF_DATE").alias("ref_date"),
                     col("GEO").alias("geo"),
                     col("Sex").alias("sex"), 
                     col("Age group").alias("age_group"),
                     col("VALUE").alias("proj_population"))
              .withColumn("year", col("ref_date"))
              .where(col('age_group').rlike("^([7|8|9]\d)|(100)|(65|66|67|68|69)")))

filtered = selected.filter(selected.age_group.rlike("^(\d{2} to)|(100)")).drop("ref_date").drop("month")

# COMMAND ----------

# Make a separate dataframe with the 65+ totals and merge it with the original dataframe
# We can sort by the temporary "sort_id" column to make sure the row gets added to the bottom of the existing age groups

df_over_65 = (filtered
               .groupBy(["year","geo", "sex"])
               .agg(sum("proj_population").alias("proj_population"))
               .withColumn("age_group", lit("65 years and over"))
               .withColumn("sort_id", lit(2)))

df_all = filtered.withColumn("sort_id", lit(1))
df_all = df_all \
           .unionByName(df_over_65) \
           .sort("year", "geo", "sex", "sort_id") \
           .drop('sort_id')


# COMMAND ----------

df_pivot = (df_all
            .groupBy("year", "geo", "age_group")
            .pivot("sex").sum("proj_population")
            .withColumnRenamed("Both sexes", "both_sexes")
            .withColumnRenamed("Females", "females")
            .withColumnRenamed("Males", "males")
            .withColumn("statistic", lit("Population Estimate")))

# COMMAND ----------

# Get the population projections dataset 
path_pop_projections = "/mnt/stsaebdevca01/saebcurated/statscan/171000501_PopProjections_SC.csv"
pop_projections = spark.read.option("header", True).csv(path_pop_projections)

# Combine projection data with estimated population data
df_combined = df_pivot.unionByName(pop_projections)

# COMMAND ----------

# Complete transformations after the append

df = (df_combined
          .withColumn("month", lit(7))
          .withColumn("date", concat_ws('-', col("year"), format_string("%02d", col("month"))))
          .withColumnRenamed("both_sexes", "ann_pop_bothsexes")
          .withColumnRenamed("females", "ann_pop_female")
          .withColumnRenamed("males", "ann_pop_male"))


# COMMAND ----------

# Add year over year change

prev_proj_window = Window.partitionBy(["geo", "age_group", "statistic"]).orderBy("year")

# Both sexes
df = df.withColumn("prev_year_bothsexes_pop", lag(df.ann_pop_bothsexes).over(prev_proj_window))
df = (df
       .withColumn("yoy_pop_chg_bothsexes", 
                   when(isnull(df.ann_pop_bothsexes - df.prev_year_bothsexes_pop), 0)
                   .otherwise(round((df.ann_pop_bothsexes - df.prev_year_bothsexes_pop) / df.prev_year_bothsexes_pop, 3))))

# Male
df = df.withColumn("prev_year_male_pop", lag(df.ann_pop_male).over(prev_proj_window))
df = (df
       .withColumn("yoy_pop_chg_m", 
                   when(isnull(df.ann_pop_male - df.prev_year_male_pop), 0)
                   .otherwise(round((df.ann_pop_male - df.prev_year_male_pop) / df.prev_year_male_pop, 3))))

# Female
df = df.withColumn("prev_year_female_pop", lag(df.ann_pop_female).over(prev_proj_window))
df = (df
       .withColumn("yoy_pop_chg_f", 
                   when(isnull(df.ann_pop_female - df.prev_year_female_pop), 0)
                   .otherwise(round((df.ann_pop_female - df.prev_year_female_pop) / df.prev_year_female_pop, 3))))

# Drop columns that were used for above calculation but are not required in final version
df_final = df.drop("prev_year_bothsexes_pop", "prev_year_male_pop", "prev_year_female_pop")

# COMMAND ----------

# MAGIC %run ../setup/sqldw_client

# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(df_final, "Pop_EstProjections_SC")

# Write to adls as csv so we can read and perform joins with other dataframes
df_final.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/statscan/Pop_EstProjections_SC.csv")