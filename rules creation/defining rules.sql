-- Databricks notebook source
select current_catalog();
select current_schema();

-- COMMAND ----------

CREATE OR REPLACE TABLE hospital.default.rules
AS SELECT
  col1 AS name,
  col2 AS constraint,
  col3 AS tag
FROM (
  VALUES
  ("src_deptID_not_null","src_dept_id IS NOT NULL","validity_dept_id"),
  ("src_providerID_not_null","src_provider_id IS NOT NULL","validity_provider"),
  ("dept_name_not_null","dept_name IS NOT NULL","validity_dept_name"),
  ("src_encounter_id_not_null"," src_encounter_id IS NOT NULL","validity_encounter"),
  ("src_patient_id_not_null"," src_patient_id IS NOT NULL","validity_patient"),
  ("src_transaction_id_not_null"," src_transaction_id IS NOT NULL","validity_transaction"),
  ("visit_date_not_null"," visit_date IS NOT NULL","validity_visitdate"),
  ("dob_not_null"," dob IS NOT NULL","validity_dob"),
  ("firstname_not_null"," first_name IS NOT NULL or lower(firstname)!='null'","validity_firstname"),
  ("visit_date_not_null"," visit_date IS NOT NULL","validity_visitdate")
  );
