# Databricks notebook source
from pyspark.sql.functions import col
def get_rules(tag):
  """
    loads data quality rules from a table
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  rules = {}
  df = spark.read.table("hospitalsdata.default.rules")
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

# COMMAND ----------

quarantine = get_rules(['validity_provider','validity_dept_id'])
@dlt.table(
    name ='silver_providers',
    partition_cols=["is_quarantined"]
)
@dlt.expect_all(quarantine[0])
def providers():
    prova_df = spark.readStream.format('cloudFiles').option('cloudFiles.format','parquet')\
    .load(f"{medical_adls_bronze}/hosa/providers")
    provb_df = spark.readStream.format('cloudFiles').option('cloudFiles.format','parquet')\
    .load(f"{medical_adls_bronze}/hosb/providers")	
    providers_df = prova_df.unionByName(provb_df).withColumnRenamed('ProviderID','src_provider_id')\
    .withColumnRenamed('FirstName','first_name')\
    .withColumnRenamed('LastName','last_name')\
    .withColumnRenamed('Specialization','specialization')\
    .withColumnRenamed('DeptID','src_dept_id')\
    .withColumnRenamed('NPI','npi')\
    .withColumnRenamed('datasource','data_source')\
    .withColumn('dept_id',concat_ws('-','src_dept_id','data_source'))\
    .withColumn('provider_id',concat_ws('-','src_provider_id','data_source'))\
    .withColumn("is_quarantined", expr(quarantine[1]))
    return providers_df

# COMMAND ----------


import dlt
t_quarantine_rules = get_rules(['validity_transaction','validity_visitdate','validity_patient'])
@dlt.table(
	name = 'transactions_src',
	partition_cols = ["is_quarantined"]
)
@dlt.expect_all(t_quarantine_rules[0])
def transactions():
	transa_df = spark.readStream\
	.format('cloudFiles')\
	.option('cloudFiles.format','parquet')\
	.load(f'{medical_adls_bronze}/hosa/transactions')
	
	transb_df = spark.readStream\
	.format('cloudFiles')\
	.option('cloudFiles.format','parquet')\
	.load(f'{medical_adls_bronze}/hosb/transactions')
	
	trans_df = transa_df.unionByName(transb_df)\
    .withColumnRenamed('TransactionID','src_transaction_id')\
	.withColumnRenamed('EncounterID','src_encounter_id')\
	.withColumnRenamed('PatientID','src_patient_id')\
	.withColumnRenamed('ProviderID','src_provider_id')\
	.withColumnRenamed('DeptID','src_dept_id')\
	.withColumnRenamed('VisitDate','visit_date')\
	.withColumnRenamed('ServiceDate','service_date')\
	.withColumnRenamed('PaidDate','paid_date')\
	.withColumnRenamed('VisitType','visit_type')\
	.withColumnRenamed('Amount','amount')\
	.withColumnRenamed('AmountType','amount_type')\
	.withColumnRenamed('PaidAmount','paid_amount')\
	.withColumnRenamed('ClaimID','src_claim_id')\
	.withColumnRenamed('PayorID','payor_id')\
	.withColumnRenamed('ProcedureCode','procedure_code')\
	.withColumnRenamed('ICDCode','icd_code')\
	.withColumnRenamed('LineOfBusiness','line_of_business')\
	.withColumnRenamed('MedicaidID','medicaid_id')\
	.withColumnRenamed('MedicareID','medicare_id')\
	.withColumnRenamed('InsertDate','src_insert_date')\
	.withColumnRenamed('ModifiedDate','src_modified_date')\
	.withColumnRenamed('datasource','data_source')\
	.withColumn('transaction_id',concat_ws('-','src_transaction_id','data_source'))\
	.withColumn("is_quarantined", expr(t_quarantine_rules[1]))\
	.withColumn('file_processed_date',date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss'))
	return trans_df


dlt.create_streaming_table(
	name = 'silver_transactions'
)

dlt.apply_changes(
	target = 'silver_transactions',
	source = 'transactions_src',
	keys = ['transaction_id'],
	stored_as_scd_type = 2,
	sequence_by = 'file_processed_date',
	track_history_column_list = ['file_processed_date']
)

# COMMAND ----------

p_quarantine_rules = get_rules(['validity_dob','validity_firstname','validity_patient'])

@dlt.table(
	name = 'patient_src',
	partition_cols = ['is_quarantined']
)
@dlt.expect_all(p_quarantine_rules[0])

def patient():

    patienta_df = spark.readStream\
	.format('cloudFiles')\
	.option('cloudFiles.format','parquet')\
    .load(f'{medical_adls_bronze}/hosa/patients')\
    .withColumnRenamed('PatientID','src_patient_id')\
    .withColumnRenamed('FirstName','first_name')\
    .withColumnRenamed('LastName','last_name')\
    .withColumnRenamed('MiddleName','middle_name')\
    .withColumnRenamed('SSN','ssn')\
    .withColumnRenamed('PhoneNumber','phone_number')\
    .withColumnRenamed('Gender','gender')\
    .withColumnRenamed('DOB','dob')\
    .withColumnRenamed('Address','address')\
    .withColumnRenamed('ModifiedDate','modified_date')\
    .withColumnRenamed('datasource','data_source')

    patientb_df = spark.readStream\
	.format('cloudFiles')\
	.option('cloudFiles.format','parquet')\
    .load(f'{medical_adls_bronze}/hosb/patients')\
    .withColumnRenamed('ID','src_patient_id')\
    .withColumnRenamed('F_Name','first_name')\
    .withColumnRenamed('L_Name','last_name')\
    .withColumnRenamed('M_Name','middle_name')\
    .withColumnRenamed('SSN','ssn')\
    .withColumnRenamed('PhoneNumber','phone_number')\
    .withColumnRenamed('Gender','gender')\
    .withColumnRenamed('DOB','dob')\
    .withColumnRenamed('Address','address')\
    .withColumnRenamed('Updated_Date','modified_date')\
    .withColumnRenamed('datasource','data_source')

    patients_df = patienta_df.unionByName(patientb_df)\
    .withColumn('patient_id',concat_ws('-','src_patient_id','data_source'))\
    .withColumn("is_quarantined", expr(p_quarantine_rules[1]))
    return patients_df  

dlt.create_streaming_table(
    name = 'silver_patients'
)
 

dlt.apply_changes(
    target = 'silver_patients',
    source = 'patient_src',
    keys = ['patient_id'],
    stored_as_scd_type = 1,
    sequence_by = '1'
)

# COMMAND ----------


e_quarantine_rules = get_rules(['validity_encounter','validity_patient'])
@dlt.table(
	name = 'encounters_src',
	partition_cols = ['is_quarantined']
)
@dlt.expect_all(e_quarantine_rules[0])
def encounters():
	encountersa_df = spark.readStream\
	.format('cloudFiles')\
	.option('cloudFiles.format','parquet')\
	.load(f'{medical_adls_bronze}/hosa/encounters')
	encountersb_df = spark.readStream\
	.format('cloudFiles')\
	.option('cloudFiles.format','parquet')\
	.load(f'{medical_adls_bronze}/hosb/encounters')
	
	encounters_df = encountersa_df.unionByName(encountersb_df)\
	.withColumnRenamed('EncounterID','src_encounter_id')\
	.withColumnRenamed('PatientID','src_patient_id')\
	.withColumnRenamed('EncounterDate','encounter_date')\
	.withColumnRenamed('EncounterType','encounter_type')\
	.withColumnRenamed('ProviderID','src_provider_id')\
	.withColumnRenamed('DepartmentID','src_dept_id')\
	.withColumnRenamed('ProcedureCode','procedure_code')\
	.withColumnRenamed('InsertedDate','src_inserted_date')\
	.withColumnRenamed('ModifiedDate','src_modified_date')\
	.withColumnRenamed('datasource','data_source')\
	.withColumn('encounter_id',concat_ws('-','src_encounter_id','data_source'))\
	.withColumn('is_quarantined',expr(e_quarantine_rules[1]))\
	.withColumn('file_processed_date',date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss'))
	return encounters_df
		
dlt.create_streaming_table(
	name = 'silver_encounters'
)

dlt.apply_changes(
	target = 'silver_encounters',
	source = 'encounters_src',
	keys = ['encounter_id'],
	stored_as_scd_type = 2,
	sequence_by = 'file_processed_date',
	track_history_column_list = ['file_processed_date']
)

# COMMAND ----------

import dlt
from pyspark.sql.functions import col,concat,concat_ws,when,lit

@dlt.table(
	name = 'gold_patient'
)
def patients():
	patients_df = spark.readStream\
 	.table('LIVE.silver_patients')\
  	.where((col('is_quarantined') == False))
	return patients_df

# COMMAND ----------

@dlt.table(
name = 'gold_department'
)
def departments():
	departments_df = spark.readStream.table('LIVE.silver_department').where(col('is_quarantined')== False)
	return departments_df

# COMMAND ----------

@dlt.table(
name = 'gold_encounters'
)
def encounters():
	encounters_df = spark.readStream.table('LIVE.silver_encounters').where(col('is_quarantined')== False )
	return encounters_df
	
@dlt.table(
name = 'gold_providers'
)
def providers():
	providers_df = spark.readStream.table('LIVE.silver_providers').where(col('is_quarantined')== False)
	return providers_df

# COMMAND ----------

@dlt.table(
name = 'gold_transactions'
)
def transactions():
	transactions_df = spark.readStream.table('LIVE.silver_transactions')\
	.withColumn('fk_patient_id',concat_ws('-','src_patient_id','data_source'))\
	.withColumn('fk_provider_id',when(col('data_source') == 'hos-a' , concat(lit('H1-'),col('src_provider_id'))).otherwise( concat(lit('H2-'),col('src_provider_id'))))\
     .withColumn('fk_dept_id',concat_ws('-',col('src_dept_id'),col('data_source')))\
	.drop('src_patient_id')\
	.drop('src_provider_id')\
	.drop('src_dept_id')\
	.where(col('is_quarantined') == False )
	return transactions_df
