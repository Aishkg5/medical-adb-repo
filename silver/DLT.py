# Databricks notebook source
from pyspark.sql.functions import col
def get_rules(tag):
  """
    loads data quality rules from a table
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  rules = {}
  df = spark.read.table("hospital.default.rules")
  for row in df.filter(col("tag").isin(tag)).collect():
    rules[row['name']] = row['constraint']
    quarantine_rules = "NOT({0})".format(" OR ".join(rules.values()))
  return (rules,quarantine_rules)

# COMMAND ----------

medical_adls_bronze = 'abfss://bronze@medicalprojectadls.dfs.core.windows.net'


# COMMAND ----------

import dlt

from pyspark.sql.functions import concat_ws,date_format,expr,current_timestamp

quarantine_rules = get_rules('validity_dept_id')
@dlt.table(
  name = 'silver_department',
  temporary=True,
  partition_cols=["is_quarantined"] #if it works on files or only table???
)
@dlt.expect_all(quarantine_rules[0]) #2 tags can i give???
def departments():
    df_depta = spark.readStream\
	.format('cloudFiles')\
	.option('cloudFiles.format','parquet')\
	.load(f"{medical_adls_bronze}/hosa/departments")
 
    df_deptb = spark.readStream\
	.format('cloudFiles')\
	.option('cloudFiles.format','parquet')\
	.load(f"{medical_adls_bronze}/hosb/departments")
    df_dept = df_depta.unionByName(df_deptb).withColumnRenamed('DeptID','src_dept_id')\
	.withColumnRenamed('Name','dept_name')\
	.withColumnRenamed('datasource','data_source')\
	.withColumn('dept_id',concat_ws('-','src_dept_id','data_source'))\
	.withColumn("is_quarantined", expr(quarantine_rules[1]))
    return df_dept
