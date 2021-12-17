# Databricks notebook source
# MAGIC %run ./setup/mount_client

# COMMAND ----------

MountClient(container_name="saebcurated").mount()

# COMMAND ----------

from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import sum, first, round, avg, regexp_extract, lit, col, regexp_extract, desc, max, lag, when, isnull
from pyspark.sql.window import Window


# COMMAND ----------

# Two main data sets that make up the OAS Takeup master datasets

path_pop_lfs_sc = "/mnt/stsaebdevca01/saebcurated/statscan/1410001701_Pop_LFS_SC.csv"
pop_lfs_sc = spark.read.option("header", True).option("inferSchema", True).csv(path_pop_lfs_sc)

path_aa_apply_regions_monthly = "/mnt/stsaebdevca01/saebcurated/adobeanalytics/AA_OAS_Apply_Regions_Monthly.csv"
aa_apply_regions_monthly = spark.read.option("header", True).option("inferSchema", True).csv(path_aa_apply_regions_monthly)

path_oas_recip_prov_type = "/mnt/stsaebdevca01/saebcurated/esdc-open-data/OAS_Recipients_ProvType.csv"
oas_recip_prov_type = spark.read.option("header", True).csv(path_oas_recip_prov_type)

# Rest of the dataframes. In case we need to join others.

# path_pop_counts_proj = "/mnt/stsaebdevca01/saebcurated/statscan/Pop_CountsProjections_SC.csv"
# pop_counts_proj = spark.read.option("header", True).csv(path_pop_counts_proj)

# path_oas_recip_types = "/mnt/stsaebdevca01/saebcurated/esdc-open-data/OAS_Recipients_Sex.csv"
# oas_recip_types = spark.read.option("header", True).csv(path_oas_recip_types)

# path_oas_new_benefits_prov_type = "/mnt/stsaebdevca01/saebcurated/esdc-open-data/OAS_NewBenefits_ProvType.csv"
# oas_new_benefits_prov_type = spark.read.option("header", True).csv(path_oas_new_benefits_prov_type)

# path_oas_new_benefits_summary = "/mnt/stsaebdevca01/saebcurated/esdc-open-data/OAS_NewBenefits_Summary.csv"
# oas_new_benefits_summary = spark.read.option("header", True).csv(path_oas_new_benefits_summary)

# path_oas_recip_prov_type_summ = "/mnt/stsaebdevca01/saebcurated/esdc-open-data/OAS_Recipients_ProvType_summary.csv"
# oas_recip_prov_type_summ = spark.read.option("header", True).csv(path_oas_recip_prov_type_summ)

# path_aa_apply_regions = "/mnt/stsaebdevca01/saebcurated/adobeanalytics/AA_OAS_Apply_Regions.csv"
# aa_apply_regions = spark.read.option("header", True).csv(path_aa_apply_regions)

# path_aa_oas_apply_country = "/mnt/stsaebdevca01/saebcurated/adobeanalytics/AA_OAS_Apply_Country.csv"
# aa_oas_apply_country = spark.read.option("header", True).csv(path_aa_oas_apply_country)

# path_aa_oas_apply_link_clicks = "/mnt/stsaebdevca01/saebcurated/adobeanalytics/AA_OAS_Apply_LinkClicks.csv"
# aa_oas_apply_link_clicks = spark.read.option("header", True).csv(path_aa_oas_apply_country)


# COMMAND ----------

display(oas_recip_prov_type)

# COMMAND ----------

display(aa_apply_regions_monthly)

# COMMAND ----------

display(pop_lfs_sc)

# COMMAND ----------

pop_lfs_sc = pop_lfs_sc.filter((pop_lfs_sc.sex == 'Both sexes') & (pop_lfs_sc.age_group == '65 years and over'))

display(pop_lfs_sc)

# COMMAND ----------

# MAGIC %run ./utils/month_string_to_num

# COMMAND ----------

# In order to join by month, the month columns need to be the same data type
pop_lfs_sc_takeup = pop_lfs_sc.withColumn("month", col("month").cast(IntegerType()))

new_month_column = month_string_to_num(lit(col("month"))).cast(IntegerType())
aa_apply_regions_monthly = aa_apply_regions_monthly.withColumn("month", new_month_column)

oas_recip_prov_type = (oas_recip_prov_type
                           .withColumn("month", col("month").cast(IntegerType()))
                           .withColumn("year", col("year").cast(IntegerType()))
                           .withColumnRenamed("period", "date_2"))

# Rename column so we can join by "province"
pop_lfs_sc_takeup = pop_lfs_sc_takeup.withColumnRenamed("geo", "province")


# COMMAND ----------

# Change "region" to "province" so we can join by that column

reg_regex = "(.+)( \(Canada\))"
aa_apply_regions_monthly = (aa_apply_regions_monthly
                                .filter(aa_apply_regions_monthly.country == "Canada")
                                .withColumn("region", regexp_extract(aa_apply_regions_monthly.region, reg_regex, 1))
                                .withColumnRenamed("region", "province")
                                .drop("country", "page_title", "page_url", "date"))


# COMMAND ----------

# Add "Canada" to aa_apply_regions_monthly

aa_apply_regions_monthly_raw = aa_apply_regions_monthly.drop("mom_change_uniq_visitors", "mom_change_visits")

aa_apply_canada = (aa_apply_regions_monthly_raw
                       .groupBy("month", "year")
                       .agg(sum("unique_visitors").alias("unique_visitors"), 
                            sum("visits").alias("visits"),
                            first("date_2").alias("date_2"))
                       .withColumn("province", lit("Canada"))
                       .withColumn("sort_id", lit(2)))

aa_apply_all = aa_apply_regions_monthly_raw.withColumn("sort_id", lit(1))
aa_apply_all = aa_apply_all.unionByName(aa_apply_canada).sort("year", "month", "sort_id").drop("sort_id")


# COMMAND ----------

# Bring back the month over month changes

mom_change_window = Window.partitionBy("province").orderBy("year", "month")

# Unique visitors
aa_apply_all = aa_apply_all.withColumn("prev_month_uniq_visitors", lag(aa_apply_all.unique_visitors).over(mom_change_window))
aa_apply_all = (aa_apply_all
                   .withColumn("mom_change_uniq_visitors", 
                               when(isnull(aa_apply_all.unique_visitors - aa_apply_all.prev_month_uniq_visitors), 0)
                               .otherwise(round((aa_apply_all.unique_visitors - aa_apply_all.prev_month_uniq_visitors) / aa_apply_all.prev_month_uniq_visitors, 3))))

# Visits
aa_apply_all = aa_apply_all.withColumn("prev_month_visits", lag(aa_apply_all.visits).over(mom_change_window))
aa_apply_all = (aa_apply_all
                   .withColumn("mom_change_visits", 
                               when(isnull(aa_apply_all.visits - aa_apply_all.prev_month_visits), 0)
                               .otherwise(round((aa_apply_all.visits - aa_apply_all.prev_month_visits) / aa_apply_all.prev_month_visits, 3))))

# Drop columns that were used for above calculation but are not required in final version
aa_apply_all = aa_apply_all.drop("prev_month_uniq_visitors", "prev_month_visits")


# COMMAND ----------

oas_takeup = pop_lfs_sc_takeup.join(aa_apply_all, ["year", "month", "province", "date_2"], "outer")
oas_takeup = oas_takeup.withColumn("takeup_rate_regional", round((oas_takeup.unique_visitors / oas_takeup.population), 5))

oas_takeup_regional = oas_takeup.filter(col("province") != "Canada")
oas_takeup_country = oas_takeup.filter(col("province") == "Canada").withColumnRenamed("takeup_rate_regional", "takeup_rate_country")


# COMMAND ----------

# MAGIC %run ./setup/sqldw_client

# COMMAND ----------

display(oas_takeup_regional)

# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(oas_takeup_regional, "OAS_Takeup_Regional")

# Write to adls as csv so we can read and perform joins with other dataframes
oas_takeup_regional.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/OAS_Takeup_Regional.csv")

# COMMAND ----------

oas_recip_canada = oas_recip_prov_type.filter(col("province") == "TOTAL").withColumn("province", lit("Canada"))
oas_takeup_country = oas_takeup_country.join(oas_recip_canada, ["province", "year", "month", "date_2"], "outer")


# COMMAND ----------

display(oas_takeup_country)

# COMMAND ----------

# Write to data warehouse
SqlDWClient().write(oas_takeup_country, "OAS_Takeup_Country")

# Write to adls as csv so we can read and perform joins with other dataframes
oas_takeup_country.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/OAS_Takeup_Country.csv")

# COMMAND ----------

# Cells below are likely not needed based on the new requirements but leaving in place in case some value can be gleaned from the big joined table

# COMMAND ----------

# pop_lfs_sc_grouped = (pop_lfs_sc
#                           .withColumn("population", pop_lfs_sc.population.cast(DoubleType()))
#                           .filter(pop_lfs_sc.age_group == "65 years and over")
#                           .groupBy("geo", "year")
#                              .agg(sum("population").alias("population"),
#                                   first("sex").alias("sex"),
#                                   first("age_group").alias("age_group"),
#                                   first("status").alias("status"))
#                          .withColumnRenamed("geo", "province"))


# COMMAND ----------

# display(pop_lfs_sc_grouped)

# COMMAND ----------

# pop_counts_proj = (pop_counts_proj
#                        .withColumnRenamed("total_pop", "projected_population")
#                        .withColumnRenamed("geo", "province"))


# COMMAND ----------

# Can't join
# This dataset is unrelated to provinces
# Could group by "benefit" and "year" and add to master data set to match with "Canada" row

# display(oas_recip_types)

# COMMAND ----------

# oas_new_benefits_prov_type = (oas_new_benefits_prov_type
#                                   .groupBy("province", "year")
#                                      .agg(sum("old_age_sec_pension").alias("old_age_sec_pension"),
#                                           sum("guaranteed_income_supp").alias("guaranteed_income_supp"),
#                                           sum("allowance").alias("allowance"))
#                                   .withColumnRenamed("old_age_sec_pension", "old_age_sec_pension__prov_type")
#                                   .withColumnRenamed("guaranteed_income_supp", "guaranteed_income_supp__prov_type")
#                                   .withColumnRenamed("allowance", "allowance__prov_type"))


# COMMAND ----------

# display(oas_new_benefits_prov_type)

# COMMAND ----------

# oas_new_benefits_summary = oas_new_benefits_summary.withColumnRenamed("avg_old_age_sec_pension", "avg_old_age_sec_pension__benefits_summary")

# display(oas_new_benefits_summary)

# COMMAND ----------

# oas_recip_prov_type = (oas_recip_prov_type
#                                   .groupBy("province", "year")
#                                      .agg(sum("old_age_sec_pension").alias("old_age_sec_pension"),
#                                           sum("guaranteed_income_supp").alias("guaranteed_income_supp"),
#                                           sum("allowance").alias("allowance"),
#                                           round(avg("gis_as_percent_of_oas"), 2).alias("gis_as_percent_of_oas"))
#                                   .withColumnRenamed("avg_guaranteed_income_supp", "avg_guaranteed_income_supp__prov_type"))


# COMMAND ----------

# display(oas_recip_prov_type)

# COMMAND ----------

# oas_recip_prov_type_summ = (oas_recip_prov_type_summ
#                                 .withColumnRenamed("avg_guaranteed_income_supp", "avg_guaranteed_income_supp__prov_type_summ")
#                                 .withColumnRenamed("avg_allowance", "avg_allowance__prov_type_summ"))

# display(oas_recip_prov_type_summ)

# COMMAND ----------

# aa_apply_regions = aa_apply_regions.filter(aa_apply_regions.country == "Canada")

# reg_regex = "(.+)( \(Canada\))"
# aa_apply_regions = (aa_apply_regions
#                         .withColumn("region", regexp_extract(aa_apply_regions.region, reg_regex, 1))
#                         .withColumnRenamed("region", "province"))


# COMMAND ----------

# display(aa_apply_regions)

# COMMAND ----------

# Can't join
# This dataset does not relate to provinces but particular URL visits for a given country

# display(aa_oas_apply_country)

# COMMAND ----------

# Can't join
# This dataset does not relate to provinces

# display(aa_oas_apply_link_clicks)

# COMMAND ----------

# df_one = pop_lfs_sc_grouped.join(pop_counts_proj, ["year", "province"], "outer")
# df_two = df_one.join(oas_new_benefits_prov_type, ["year", "province"], "outer")
# df_three = df_two.join(oas_new_benefits_summary, ["year", "province"], "outer")
# df_four = df_three.join(oas_recip_prov_type, ["year", "province"], "outer")
# df_five = df_four.join(oas_recip_prov_type_summ, ["year", "province"], "outer")

# final_join = df_five.join(aa_apply_regions, ["year", "province"], "outer")


# COMMAND ----------

# oas_takeup = oas_takeup.filter(oas_takeup.country == "Canada")

# display(final_join)

# COMMAND ----------

# # Write to data warehouse
# SqlDWClient().write(final_join, "OAS_Takeup")

# # Write to adls as csv so we can read and perform joins with other dataframes
# final_join.write.mode("overwrite").option("header", True).csv("/mnt/stsaebdevca01/saebcurated/OAS_Takeup.csv")