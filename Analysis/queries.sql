-- Databricks notebook source
--Total Charge Amount per provider by department
select
concat_ws(' ','first_name','last_name') as provider_name,
dept_name,
sum(gt.amount) as total_charged_amt
from gold_transactions gt 
left join gold_providers gp on gp.provider_id = gt.fk_provider_id
left join gold_department gd on gd.dept_id = gt.fk_dept_id
group by 
	all

-- COMMAND ----------

--Total Charge Amount per provider by department for each month for year 2024
select
concat_ws(' ','first_name','last_name') as provider_name,
dept_name,
date_format(servicedate, 'yyyyMM') YYYYMM,
sum(gt.amount) as total_charged_amt
from gold_transactions gt 
left join gold_providers gp on gp.provider_id = gt.fk_provider_id
left join gold_department gd on gd.dept_id = gt.fk_dept_id
where year(ft.ServiceDate) = 2024
group by 
	all
order by 1,3
