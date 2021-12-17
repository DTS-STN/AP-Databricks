# Databricks notebook source
# MAGIC %run ../setup/mount_client

# COMMAND ----------

MountClient(container_name="esdc-open-data").mount()

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import monotonically_increasing_id, \
                                  regexp_extract, desc, \
                                  regexp_replace, \
                                  to_timestamp, year, \
                                  col, sum, lit, round, \
                                  first, concat_ws, when, \
                                  date_format, to_date, split
from pyspark.sql.window import Window

# COMMAND ----------

source_file_path = "/mnt/stsaebdevca01/esdc-open-data/oas/number_of_recipients_by_type_and_gender.csv"

df = spark.read \
          .csv(source_file_path)


# COMMAND ----------

# Add ids temporarily so we can filter out the first 3 rows
df_with_ids = df.withColumn("id", monotonically_increasing_id())
df_filtered = df_with_ids.filter(df_with_ids.id > 2).drop("id")


# COMMAND ----------

display(df_filtered)

# COMMAND ----------

# Use pandas to insert missing values. 
# Once we have named columns we can promote a full first row to header

pandas_df = df_filtered.toPandas()

pandas_df.at[0, "_c0"] = "benefit"
pandas_df.at[0, "_c1"] = "benefit_fr"
pandas_df.at[0, "_c2"] = "sex"
pandas_df.at[0, "_c3"] = "sex_fr"

# Promote first row to header
pandas_df.columns = pandas_df.iloc[0] 
pandas_df = pandas_df[1:]
pandas_df.head()

# Drop unwanted columns and reshape/unpivot data frame
pandas_df = pandas_df.drop(columns=["benefit_fr", "sex_fr"])
pandas_df = pandas_df.melt(id_vars = ["benefit", "sex"], var_name = "period", value_name = "recipients")

# Go back to using spark and delete pandas frame
df_melted = spark.createDataFrame(pandas_df)
del pandas_df


# COMMAND ----------

# Convert the recipients column to integer data type
df = df_melted.withColumn("recipients", regexp_replace("recipients", ",", ""))
df = df.withColumn("recipients", df.recipients.cast(IntegerType()))


# COMMAND ----------

# MAGIC %run ../utils/month_string_to_num

# COMMAND ----------

# Split date into year and month and reformat period column

regex = '(\w+)(\s+)(\/)(\s{1})(\w+)(\s+)(\d+)'
period_column = concat_ws('-', 
                          month_string_to_num(lit(regexp_extract(col('period'), regex, 1))), 
                          regexp_extract(col('period'), regex, 7))

df = (df
       .withColumn('period', period_column)
       .withColumn('year', split(col('period'), '-')[1].cast(IntegerType()))
       .withColumn('month', split(col('period'), '-')[0]))


# COMMAND ----------

# Add new category "Both sexes" and get the total from Male and Female

df_both_sexes = (df
                   .groupBy(["year", "benefit"])
                   .agg(sum("recipients").alias("recipients"), 
                        first("month").alias("month"),
                        first("period").alias("period"))
                   .withColumn("sex", lit("Both sexes"))
                   .withColumn("sort_id", lit(2)))

df_with_sort = df.withColumn("sort_id", lit(1))

df_all = (df_with_sort
            .unionByName(df_both_sexes)
            .sort("year", "benefit", "sort_id")
            .drop("sort_id"))


# COMMAND ----------

df_all = (df_all
            .withColumn("benefit", when(col("benefit") == "Allowance ", "ALS")
                                   .when(col("benefit") == "Guaranteed Income Supplement", "GIS")
                                   .when(col("benefit") == "Old Age Security Pension", "OAS"))
            .withColumn("sex", when(col("sex") == "Male", "M")
                               .when(col("sex") == "Female", "F")
                               .when(col("sex") == "Both sexes", "Total"))
            .withColumnRenamed("recipients", "sex_count"))


# COMMAND ----------

window_total = Window.partitionBy("year", "month", "benefit").orderBy(desc("sex"))
df_w_total = df_all.withColumn("sex_count_total", first(col("sex_count")).over(window_total))

df = (df_w_total
        .withColumn("sex_prop", 
                    when(col("sex") == "M", round(col("sex_count") / col("sex_count_total"), 3))
                    .when(col("sex") == "F", round(col("sex_count") / col("sex_count_total"), 3)))
        .drop("sex_count_total"))


# COMMAND ----------

# MAGIC %run ../setup/sqldw_client

# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(df, "OAS_Recipients_Sex")

# Write to adls as csv so we can read and perform joins with other dataframes
df.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/esdc-open-data/OAS_Recipients_Sex.csv")