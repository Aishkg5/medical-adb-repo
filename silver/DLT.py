# Databricks notebook source
# MAGIC %run ../generic_configs.functions

# COMMAND ----------

df_depta = spark.read.
	.format('parquet')
	.load("medical-adls-bronze/hosa/department")

# COMMAND ----------



import dlt

@dlt.table(
name = 'silver_department',
  temporary=True,
  partition_cols=["is_quarantined"] -- if it works on files or only table???
)
@dlt.expect_all(get_rules('validity')) -- 2 tags can i give???
def departments():
	df_depta = spark.readStream
	.format('cloudFiles')
	.option('cloudFiles.format','parquet')
	.load("/mnt/bronze/hosa/department")
    df_deptb = spark.readStream
	.format('cloudFiles')
	.option('cloudFiles.format','parquet')
	.load("/mnt/bronze/hosb/department")
	df_dept = df_depta.unionByName(df_deptb).withColumnRenamed('DeptID','src_dept_id')
	.withColumnRenamed('Name','dept_name')
	.withColumnRenamed('datasource','data_source')
	.withColumn('dept_id',concat_ws('-','src_dept_id','data_source'))
	.withColumn("is_quarantined", expr(quarantine_rules))
	return df_dept

# COMMAND ----------

@dlt.table(
name ='silver_providers'
)
def providers():
	prova_df = spark.readStream.format('cloudFiles').option('cloudFiles.format','parquet')
	.load("/mnt/bronze/hosa/providers")
	provb_df = spark.readStream.format('cloudFiles').option('cloudFiles.format','parquet')
	.load("/mnt/bronze/hosb/providers")
	
	providers_df = prova_df.unionByName(provb_df).withColumnRenamed('ProviderID','src_provider_id')
	.withColumnRenamed('FirstName','first_name')
	.withColumnRenamed('LastName','last_name')
	.withColumnRenamed('Specialization','specialization')
	.withColumnRenamed('DeptID','dept_src_id')
	.withColumnRenamed('NPI','npt')
	.withColumnRenamed('datasource','data_source')
	.withColumn('dept_id',concat_ws('-','src_dept_id','data_source'))
	.withColumn('provider_id',concat_ws('-','src_provider_id','data_source'))
return providers_df

# COMMAND ----------


import dlt

@dlt.tables(
name = 'transactions_src',
temporary = True,
partition_cols = ["is_quarantined"]
)
@dlt.expect_all(get_rules('validity'))
def transactions():
	transa_df = spark.readStream
	.format('cloudFiles')
	.option('cloudFile.format','parquet')
	.load('/mnt/bronze/hosa/transactions')
	
	transb_df = spark.readStream
	.format('cloudFiles')
	.option('cloudFile.format','parquet')
	.load('/mnt/bronze/hosb/transactions')
	
	trans_df = transa_df.unionByName(transb_df)
	.withColumnRenamed('TransactionID ',' src_transaction_id')
	.withColumnRenamed('EncounterID','src_encounter_id')
	.withColumnRenamed('PatientID','src_patient_id')
	.withColumnRenamed('ProviderID','src_provider_id')
	.withColumnRenamed('DeptID','src_dept_id')
	.withColumnRenamed('VisitDate','visit_date')
	.withColumnRenamed('ServiceDate','service_date')
	.withColumnRenamed('PaidDate','paid_date')
	.withColumnRenamed('VisitType','visit_type')
	.withColumnRenamed('Amount','amount')
	.withColumnRenamed('AmountType','amount_type')
	.withColumnRenamed('PaidAmount','paid_amount')
	.withColumnRenamed('ClaimID','src_claim_id')
	.withColumnRenamed('PayorID','payor_id')
	.withColumnRenamed('ProcedureCode','procedure_code')
	.withColumnRenamed('ICDCode','icd_code')
	.withColumnRenamed('LineOfBusiness','line_of_business')
	.withColumnRenamed('MedicaidID','medicaid_id')
	.withColumnRenamed('MedicareID','medicare_id')
	.withColumnRenamed('InsertDate','src_insert_date')
	.withColumnRenamed('ModifiedDate','src_modified_date')
	.withColumnRenamed('datasource','data_source')
	.withColumn('transaction_id',concat_ws('-','src_transaction_id','data_source'))
	.withColumn("is_quarantined", expr(quarantine_rules))
	.withColumn('file_processed_date',date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss'))
	return trans_df


dlt.create_streaming_table(
name = 'silver_transactions'
)

dlt.apply_changes(
	target = 'silver_transactions',
	source = 'transactions_src',
	keys = ['transaction_id'],
	stored_as_scd_types = '2',
	sequence_by = 'file_processed_date',
	track_history_column_list = ['file_processed_date']
)


# COMMAND ----------

@dlt.table(
name = 'patient_src',
temporary = 'True',
partitioned_cols = ['is_quarantined']
)
@dlt.expect_all(get_rules())
def patient():
	patienta_df = spark.readStream.format('cloudFiles').options('cloudFiles.format','parquet').load('/mnt/bronze/hosa/patients')
	.withColumnRenamed('PatientID,'src_patient_id')
	.withColumnRenamed('FirstName,'first_name')
	.withColumnRenamed('LastName,'last_name')
	.withColumnRenamed('MiddleName,'middle_name')
	.withColumnRenamed('SSN,'ssn')
	.withColumnRenamed('PhoneNumber,'phone_number')
	.withColumnRenamed('Gender,'gender')
	.withColumnRenamed('DOB,'dob')
	.withColumnRenamed('Address,'address')
	.withColumnRenamed('ModifiedDate,'modified_date')
	.withColumnRenamed('datasource','data_source')
	
	patientb_df = spark.readStream.format('cloudFiles').options('cloudFiles.format','parquet').load('/mnt/bronze/hosb/patients')
	.withColumnRenamed('ID','src_patient_id')
	.withColumnRenamed('F_Name','first_name')
	.withColumnRenamed('L_Name','last_name')
	.withColumnRenamed('M_Name','middle_name')
	.withColumnRenamed('SSN','ssn')
	.withColumnRenamed('PhoneNumber','phone_number')
	.withColumnRenamed('Gender','gender')
	.withColumnRenamed('DOB','dob')
	.withColumnRenamed('Address','address')
	.withColumnRenamed('Updated_Date','modified_date')
	.withColumnRenamed('datasource','data_source')
	
	patients_df = patienta_df.unionByName(patientb_df)
	.withColumn('patient_id',concat_ws('-','src_patient_id','data_source'))
	.withColumn("is_quarantined", expr(quarantine_rules))
	return patients_df

dlt.create_streaming_table(
name = 'silver_patients'
)

dlt.apply_changes(
	target = 'silver_patients',
	source = 'patients_src',
	keys = ['patient_id'],
	stored_as_scd_types = '1',
	sequence_by = ''
)

# COMMAND ----------


@dlt.table(
name = 'encounter_src'
temporary = True,
partitioned_cols = ['is_quarantined']
)
@dlt.expect_all(get_rules('validity'))
def encounters():
	encountersa_df = spark.readStream.format('cloudFiles').options('cloudFiles.format','parquet').
	load('/mnt/bronze/hosa/encounters')
	encountersb_df = spark.readStream.format('cloudFiles').options('cloudFiles.format','parquet').
	load('/mnt/bronze/hosb/encounters')
	
	encounters_df = encountersa_df.unionByName(encountersb_df)
	.withColumnRenamed('EncounterID','src_encounter_id')
	.withColumnRenamed('PatientID','src_patient_id')
	.withColumnRenamed('EncounterDate','encounter_date')
	.withColumnRenamed('EncounterType','encounter_type')
	.withColumnRenamed('ProviderID','src_provider_id')
	.withColumnRenamed('DepartmentID','src_dept_id')
	.withColumnRenamed('ProcedureCode','procedure_code')
	.withColumnRenamed('InsertedDate','src_inserted_date')
	.withColumnRenamed('ModifiedDate','src_modified_date')
	.withColumnRenamed('datasource','data_source')
	.wthColumn('encounter_id',concat_ws('-','src_encounter_id','data_source')
	.wthColumn('is_quarantined',expr(quarantine_rules))
	.withColumn('file_processed_date',date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss'))
	return encounters_df
		
dlt.create_streaming_table(
name = 'silver_encounters'
)

dlt.apply_changes(
	target = 'silver_encounters',
	source = 'encounters_src',
	keys = ['encounter_id'],
	stored_as_scd_types = '2',
	sequence_by = 'file_processed_date',
	track_history_column_list = ['file_processed_date']
)
