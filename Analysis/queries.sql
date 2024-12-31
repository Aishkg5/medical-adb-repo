-- Databricks notebook source
--Total Charge Amount per provider by department
select
concat(gp.first_name,' ',gp.last_name) as provider_name,
dept_name,
sum(gt.amount) as total_charged_amt
from hospitalsdata.medical.gold_transactions gt 
left join hospitalsdata.medical.gold_providers gp on gp.src_provider_id = gt.fk_provider_id
left join hospitalsdata.medical.gold_department gd on gd.dept_id = gt.fk_dept_id
group by 
	all

-- COMMAND ----------

--Total Charge Amount per provider by department for each month for year 2024
select
concat(gp.first_name,' ',gp.last_name) as provider_name,
dept_name,
date_format(service_date, 'yyyyMM') YYYYMM,
sum(gt.amount) as total_charged_amt
from hospitalsdata.medical.gold_transactions gt 
left join hospitalsdata.medical.gold_providers gp on gp.src_provider_id = gt.fk_provider_id
left join hospitalsdata.medical.gold_department gd on gd.dept_id = gt.fk_dept_id
where year(gt.Service_Date) = 2024
group by 
	all
order by 1,3
