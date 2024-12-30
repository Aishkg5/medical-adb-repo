# Databricks notebook source
import dlt

@dlt.table(
name = 'gold_patient'
)
def patients():
	patients_df = spark.readStream.table('LIVE.silver_patient').where('is_quarantined' = False & _end_at is null )
	return patients_df

@dlt.table(
name = 'gold_department'
)
def departments():
	departments_df = spark.readStream.table('LIVE.silver_department').where('is_quarantined' = False)
	return departments_df
	

@dlt.table(
name = 'gold_encounters'
)
def encounters():
	encounters_df = spark.readStream.table('LIVE.silver_encounters').where('is_quarantined' = False & _end_at is null )
	return encounters_df
	
@dlt.table(
name = 'gold_providers'
)
def providers():
	providers_df = spark.readStream.table('LIVE.silver_providers').where('is_quarantined' = False)
	return providers_df

# COMMAND ----------

@dlt.table(
name = 'gold_transactions'
)
def transactions():
	transactions_df = spark.readStream.table('LIVE.silver_transactions')
	.withColumn('fk_patient_id',concat_ws('-','src_patient_id','data_source'))
	.withColumn('fk_provider_id',case when ('data_source' = 'hos-a' then concat('H1-','src_provider_id') else  concat('H2-','src_provider_id'))
	
	.withColumn('fk_dept_id',concat_ws('-','src_dept_id','data_source'))
	.drop('src_patient_id')
	.drop('src_provider_id')
	.srop('src_dept_id')
	.where('is_quarantined' = False & _end_at is null )
	return transactions_df
