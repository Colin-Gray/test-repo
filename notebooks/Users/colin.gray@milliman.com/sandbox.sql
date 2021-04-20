-- Databricks notebook source
select
  *
from default.nemt_claims as nemt 
inner join default.enc_medical as clms on
  clms.num_icn = nemt.claim_icn

-- COMMAND ----------

select format_number(sum(Paid),"0,000") as Paid from mcs_historic.claims_202012

-- COMMAND ----------

select service_Category, format_number(sum(Paid),"0,000") as Paid from mcs_prod.claims group by 1

-- COMMAND ----------

select format_number(sum(Paid),"0,000") as Paid from mcs_dev.mcs_base_claims_20201005_1544

-- COMMAND ----------

select
  Incurred_Month
  ,substr(ClaimID,1,length(ClaimID)-1) as ClaimID
  ,format_number(sum(Paid),"0,000") as Paid
from mcs_prod.claims
where incurred_month = "2017-01-01"
group by 1,2
order by 1,2

-- COMMAND ----------

select
  Incurred_Month
  ,ClaimID
  ,format_number(sum(Paid),"0,000") as Paid
from mcs_dev.mcs_base_claims_20201005_1544
where incurred_month = "2017-01-01"
group by 1,2
order by 1,2

-- COMMAND ----------

select * from mcs_prod.claims where ClaimID in ("7017094001001P","7017094001001V")

-- COMMAND ----------

select * from enc_medical where num_icn = "7017094001001"

-- COMMAND ----------

select * from mcs_dev.mcs_base_claims_20201005_1544 where ClaimID = "7017094001001"

-- COMMAND ----------

select 
  Incurred_Month
  ,sum(paid) as paid 
from mcs_prod.claims 
where substr(ClaimID,1,length(ClaimID)-1) in (

select
  New.ClaimID
from (
  select
    substr(ClaimID,1,length(ClaimID)-1) as ClaimID
    ,sum(Paid) as Paid
  from mcs_prod.claims 
  group by 1
  ) as new
inner join (
  select
    ClaimID
    ,sum(Paid) as Paid
  from mcs_dev.mcs_base_claims_20201005_1544 
  group by 1
  ) as old
  on new.ClaimID =old.ClaimID
where new.Paid <> old.Paid

)
group by 1

-- COMMAND ----------

