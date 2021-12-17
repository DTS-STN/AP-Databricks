# Databricks notebook source
# MAGIC %run ../setup/mount_client

# COMMAND ----------

MountClient(container_name="esdc-open-data").mount()

# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import regexp_extract, regexp_replace, col, concat_ws, lit, split, sum, avg, round, when, lag, isnull
from pyspark.sql.window import Window


# COMMAND ----------

source_file_path = "/mnt/stsaebdevca01/esdc-open-data/oas/number_of_new_benefits_by_province_and_type.csv"
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
df = ((df
         .withColumnRenamed("Period", "period")
         .withColumnRenamed("Province", "province") 
         .withColumnRenamed("Old Age Security Pension", "old_age_sec_pension")
         .withColumnRenamed("Guaranteed Income Supplement", "guaranteed_income_supp")
         .withColumnRenamed(" Allowance ", "allowance")))

# Remove commas from strings that represent number of recipients
df = (df
        .withColumn("old_age_sec_pension", regexp_replace("old_age_sec_pension", ",", ""))
        .withColumn("guaranteed_income_supp", regexp_replace("guaranteed_income_supp", ",", ""))
        .withColumn("allowance", regexp_replace("allowance", ",", "")))
  
# Replace all "X" values in benefits columns with null
df = (df
        .withColumn("old_age_sec_pension", when(col("old_age_sec_pension") == 'X', None).otherwise(col("old_age_sec_pension")))
        .withColumn("guaranteed_income_supp", when(col("guaranteed_income_supp") == 'X', None).otherwise(col("guaranteed_income_supp")))
        .withColumn("allowance", when(col("allowance") == 'X', None).otherwise(col("allowance"))))


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

df_filt = df
for prov in PROVINCES.keys():
  regex = f"^({prov})(.*)"
  df_filt = df_filt.withColumn("province", regexp_replace("province", regex, PROVINCES[prov]))

df_filt = (df_filt.withColumn("province", regexp_replace("province", "INTERNATIONAL AGREEMENTS /ACCORDS INTERNATIONAUX", "International Agreements")))


# COMMAND ----------

# Fix September 2012-2016 values

filter_condition = ((df_filt.period.rlike("09-20(12|13|14|15|16)")) & (df_filt.province.isin("TOTAL", "International Agreements")))
df_filt_minus_prob = df_filt.filter(~filter_condition)

# Isolate problem period in separate dataframe
df_prob = df_filt.filter(filter_condition)
df_fixed = df_prob.withColumn("province", when(df_prob.province == "TOTAL", "International Agreements").otherwise("TOTAL"))

# Append the data frame without the problem rows with the data frame containing just the fixed rows
# Sort so it's easier to inspect
df_final = df_filt_minus_prob.union(df_fixed).sort("year", "month")


# COMMAND ----------

# Using Pandas, flip the table using melt so the benefits are together in one column

pandas_df = df_final.toPandas()
pandas_df = pandas_df.melt(id_vars = ["period", "province", "year", "month"], var_name="benefit", value_name="recipients")

# Go back to using PySpark
df_melted = spark.createDataFrame(pandas_df)
del pandas_df

df_mom = (df_melted
            .withColumn("benefit", when(col("benefit") == "allowance", "ALS")
                                   .when(col("benefit") == "old_age_sec_pension", "OAS")
                                   .when(col("benefit") == "guaranteed_income_supp", "GIS")))


# COMMAND ----------

# Get month over month changes

changes_window = Window.partitionBy("province", "benefit").orderBy("year", "month")
df_mom = (df_mom
            .withColumn("prev_count", 
                        lag(df_mom.recipients)
                        .over(changes_window)))

df_mom = (df_mom
            .withColumn("mom_change_pct", 
                        when(isnull(df_mom.recipients - df_mom.prev_count), 0)
                        .otherwise(round((df_mom.recipients - df_mom.prev_count) / df_mom.prev_count, 3)))
            .withColumn("mom_change_diff", 
                        when(isnull(df_mom.recipients - df_mom.prev_count), 0)
                        .otherwise(round((df_mom.recipients - df_mom.prev_count))))
            .drop("prev_count"))
         

# COMMAND ----------

# MAGIC %run ../setup/sqldw_client

# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(df_mom, "OAS_NewBenefits_ProvType")

# Write to adls as csv so we can read and perform joins with other dataframes
df_mom.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/esdc-open-data/OAS_NewBenefits_ProvType.csv")

# COMMAND ----------

# MAGIC %md ### Create Summary Table

# COMMAND ----------

df_filtered = df_final.filter(col("province") != "TOTAL").filter(col("province") != "International Agreements")

df = (df_filtered
        .groupBy("province", "year")
        .agg(sum("old_age_sec_pension").alias("total_old_age_sec_pension"), 
             round(avg("old_age_sec_pension"), 1).alias("avg_old_age_sec_pension"),
            sum("guaranteed_income_supp").alias("total_guaranteed_income_supp"),
             round(avg("guaranteed_income_supp"), 1).alias("avg_guaranteed_income_supp"),
             sum("allowance").alias("total_allowance"),
             round(avg("allowance"), 1).alias("avg_allowance")))


# COMMAND ----------

# Make a separate dataframe with the Canadian totals and merge it with the original dataframe
# Using the temporary "sort_id" column we can make sure the row gets added to the bottom of the provinces for a given year

df_canada = (df
             .groupBy("year")
             .agg(sum("total_old_age_sec_pension").alias("total_old_age_sec_pension"),
                 round(avg("avg_old_age_sec_pension"), 1).alias("avg_old_age_sec_pension"),
                 sum("total_guaranteed_income_supp").alias("total_guaranteed_income_supp"),
                 round(avg("avg_guaranteed_income_supp"), 1).alias("avg_guaranteed_income_supp"),
                 sum("total_allowance").alias("total_allowance"),
                 round(avg("avg_allowance"), 1).alias("avg_allowance"))
             .withColumn("province", lit("Canada"))
             .withColumn("sort_id", lit(2)))

df_summ = df.withColumn("sort_id", lit(1))
df_summ = df_summ \
           .unionByName(df_canada).sort("year", "sort_id") \
           .drop('sort_id')


# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(df_summ, "OAS_NewBenefits_Summary")

# Write to adls as csv so we can read and perform joins with other dataframes
df_summ.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/esdc-open-data/OAS_NewBenefits_Summary.csv")