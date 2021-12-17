# Databricks notebook source
# MAGIC %run ../setup/mount_client

# COMMAND ----------

MountClient(container_name="statscan").mount()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType
from pyspark.sql.functions import col, desc, year, month, to_timestamp, trim, sum, lit, last, lag, when, isnull, round, max, min
from pyspark.sql.window import Window


# COMMAND ----------

pop_proj_schema = StructType(fields=[StructField("REF_DATE", IntegerType(), False),
                                     StructField("GEO", StringType(), False),
                                     StructField("DGUID", StringType(), False),
                                     StructField("Projection Scenario", StringType(), False),
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

source_file_path = "/mnt/stsaebdevca01/statscan/population-projections/input/17100057.csv"

# COMMAND ----------

df = spark.read \
          .option("header", True) \
          .schema(pop_proj_schema) \
          .csv(source_file_path)


# COMMAND ----------

selected = (df.select(col("REF_DATE").alias("ref_date"),
                     col("GEO").alias("geo"),
                     col("Projection Scenario").alias("proj_scenario"),
                     col("Sex").alias("sex"), 
                     col("Age group").alias("age_group"),
                     col("VALUE").alias("proj_population"))
              .withColumn("year", col("ref_date"))
              .withColumn("month", month(to_timestamp(col("ref_date"))))
              .withColumn("proj_population", col("proj_population") * 1000)
              .where(col('age_group').rlike("^([7|8|9]\d)|(100)|(65|66|67|68|69)")))

filtered = (selected.filter((selected.year > 2021) &
                            (selected.age_group.rlike("^(\d{2} to)|(100)")))
                    .drop("ref_date").drop("month"))


# COMMAND ----------

# Make a separate dataframe with the 65+ totals and merge it with the original dataframe
# We can sort by the temporary "sort_id" column to make sure the row gets added to the bottom of the existing age groups

df_over_65 = (filtered
               .groupBy(["year","geo","proj_scenario", "sex"])
               .agg(sum("proj_population").alias("proj_population"))
               .withColumn("age_group", lit("65 years and over"))
               .withColumn("sort_id", lit(2)))

df_all = filtered.withColumn("sort_id", lit(1))
df_all = df_all \
           .unionByName(df_over_65) \
           .sort("year", "geo", "proj_scenario", "sex", "sort_id") \
           .drop('sort_id')


# COMMAND ----------

# Create first dataframe for export

df_pivot = (df_all
            .groupBy("year", "geo", "proj_scenario", "age_group")
            .pivot("sex").sum("proj_population")
            .withColumnRenamed("Both sexes", "both_sexes")
            .withColumnRenamed("Females", "females")
            .withColumnRenamed("Males", "males"))


# COMMAND ----------

# MAGIC %run ../setup/sqldw_client

# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(df_pivot, "171000501_PopProjections_SC")

# Write to adls as csv so we can read and perform joins with other dataframes
df_pivot.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/statscan/171000501_PopProjections_SC.csv")

# COMMAND ----------

# Create second dataframe for export

df = (df_all
        .sort("geo", "proj_scenario", "sex", "year")
        .filter(col("age_group").rlike("^65 years"))
        .filter(col("geo") == "Canada")
        .filter(col("sex") == "Both sexes")
        .filter(col("year").isin(2022, 2068)))


# COMMAND ----------

# Some of the below transformations are not used in the final version as changes to requirements were made over time.
# The below cells are kept as is for now in case any further changes are made or we revert back to needing some of these columns in the near term.

# Add year over year change
# prev_proj_window = Window.partitionBy(["geo", "proj_scenario", "sex"]).orderBy("year")
# df = df.withColumn("prev_year_pop_proj", lag(df.proj_population).over(prev_proj_window))
# df = (df
#        .withColumn("year_over_year_change", 
#                    when(isnull(df.proj_population - df.prev_year_pop_proj), 0)
#                    .otherwise(round((df.proj_population - df.prev_year_pop_proj) / df.prev_year_pop_proj, 3))))

# # Add change from the last year population projection is available
# max_proj_window = Window.partitionBy(["geo", "proj_scenario", "sex"])
# df = df.withColumn("pop_last_year_projected", last("proj_population").over(max_proj_window))
# df = (df
#        .withColumn("current_to_last_available_proj_change",
#                    when(isnull(df.pop_last_year_projected - df.proj_population), 0)
#                    .otherwise(round((df.pop_last_year_projected - df.proj_population) / df.proj_population, 3))))

# # Drop columns that were used for above calculation but are not required in final version
# df_summ = df.drop("prev_year_pop_proj", "pop_last_year_projected")

# COMMAND ----------

prev_proj_window = Window.partitionBy(["geo", "proj_scenario", "sex"]).orderBy("year")
df = df.withColumn("year_pop_proj_2022", lag(df.proj_population).over(prev_proj_window))

df = (df
       .withColumn("change_2022_2068_diff", 
                   when(isnull(df.proj_population - df.year_pop_proj_2022), None)
                   .otherwise(df.proj_population - df.year_pop_proj_2022)))
df = (df
       .withColumn("change_2022_2068_pct", 
                   when(isnull(df.proj_population - df.year_pop_proj_2022), None)
                   .otherwise(round((df.proj_population - df.year_pop_proj_2022) / df.year_pop_proj_2022, 3))))

df = df.drop("year_pop_proj_2022")


# COMMAND ----------

# Change 2022_2068_Max
min_max_window = Window.partitionBy("geo")
df = (df
        .withColumn("change_2022_2068_max", when(col("year") == 2068, max(df.change_2022_2068_pct).over(min_max_window)))
        .withColumn("change_2022_2068_min", when(col("year") == 2068, min(df.change_2022_2068_pct).over(min_max_window))))


# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(df, "Pop_CountsProjections_Sum")

# Write to adls as csv so we can read and perform joins with other dataframes
df.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/statscan/Pop_CountsProjections_Sum.csv")