-- Databricks notebook source
select *
from hive_metastore.prod_publish_dw.vw_dims_account_flat_hierarchy
where org_id in ('HCO0000018392', 'HCO0000021086', 'HCO0000045628', 'HCO0000021082', 'HCO0000122502', 'HCO0000938528')
order by org_id, idn

-- COMMAND ----------

select *
from hive_metastore.prod_publish_dw.vw_dims_customer
where customer_id='HCO0000938528'

-- COMMAND ----------

select *
from hive_metastore.prod_publish_dw.vw_dims_customer
where customer_id='HCO0000038063' 

-- COMMAND ----------

select *
from hive_metastore.prod_publish_dw.vw_dims_account_flat_hierarchy
where idn in ('HCO0000018392', 'HCO0000021086', 'HCO0000045628', 'HCO0000021082', 'HCO0000122502', 'HCO0000938528')
order by org_id, idn

-- COMMAND ----------

select distinct primary_affiliation_indicator
from hive_metastore.prod_publish_dw.vw_dims_customer_affiliation
-- where affiliation_sub_type='OW'
LIMIT 100

-- COMMAND ----------

select distinct primary_affiliation_indicator
from hive_metastore.prod_publish_dw.vw_dims_customer_affiliation
where affiliation_type_code='HCP-to-HCO'


-- COMMAND ----------

select distinct affiliation_type_code
from hive_metastore.prod_publish_dw.vw_dims_customer_affiliation


-- COMMAND ----------

SELECT DISTINCT relationship_type_code
from hive_metastore.prod_publish_dw.vw_dims_customer_affiliation
where affiliation_type_code='HCP-to-HCO'
ORDER BY 1

-- COMMAND ----------

-- Check distinct relationship_type_code for HCP-TO-HCO

with HCP_NPI_list as (
  select distinct customer_id, FREQ_HCP_NPI as HCP_NPI
  from hive_metastore.prod_publish_dw.vw_dims_customer_identifiers
  inner join hive_metastore.labs_ambit_byod.ambit_geo_freq_npi_list
  on source_system_id = FREQ_HCP_NPI
  where customer_type = 'HCP'
)
  select distinct relationship_type_code
  from hive_metastore.prod_publish_dw.vw_dims_customer_affiliation a
  inner join HCP_NPI_list b
  on a.start_hcp_id = b.customer_id
  where a.affiliation_type_code = 'HCP-to-HCO' and
        a.primary_affiliation_indicator = 'Yes' and
        a.inactive_flag = 'false'
  order by 1

-- COMMAND ----------


-- Check distinct relationship_type_code for HCO-TO-HCO

with HCP_NPI_list as (
  select distinct customer_id, FREQ_HCP_NPI as HCP_NPI
  from hive_metastore.prod_publish_dw.vw_dims_customer_identifiers
  inner join hive_metastore.labs_ambit_byod.ambit_geo_freq_npi_list
  on source_system_id = FREQ_HCP_NPI
  where customer_type = 'HCP'
),
HCO_list as (
  select distinct a.end_hco_id
  from hive_metastore.prod_publish_dw.vw_dims_customer_affiliation a
  inner join HCP_NPI_list b
  on a.start_hcp_id = b.customer_id
  where a.affiliation_type_code = 'HCP-to-HCO' and
        a.primary_affiliation_indicator = 'Yes' and
        a.inactive_flag = 'false'
)
select distinct relationship_type_code
from hive_metastore.prod_publish_dw.vw_dims_customer_affiliation a
inner join HCO_list b 
on b.end_hco_id = a.start_hco_id
where a.affiliation_type_code = 'HCO-to-HCO'

-- COMMAND ----------

select count(distinct FREQ_HCP_NPI) from hive_metastore.labs_ambit_byod.ambit_geo_freq_npi_list

-- COMMAND ----------

-- Pull all HCO-TO-HCO data

with HCP_NPI_list as (
  select distinct customer_id, FREQ_HCP_NPI as HCP_NPI
  from hive_metastore.prod_publish_dw.vw_dims_customer_identifiers
  inner join hive_metastore.labs_ambit_byod.ambit_geo_freq_npi_list
  on source_system_id = FREQ_HCP_NPI
  where customer_type = 'HCP'
),
HCO_list as (
  select distinct a.end_hco_id
  from hive_metastore.prod_publish_dw.vw_dims_customer_affiliation a
  inner join HCP_NPI_list b
  on a.start_hcp_id = b.customer_id
  where a.affiliation_type_code = 'HCP-to-HCO' and
        a.primary_affiliation_indicator = 'Yes' and
        a.inactive_flag = 'false'
)
select a.*, c.name as org_name, d.name as owner_org_name
from hive_metastore.prod_publish_dw.vw_dims_customer_affiliation a
inner join HCO_list b on b.end_hco_id = a.start_hco_id
left join hive_metastore.prod_publish_dw.vw_dims_customer c on a.start_hco_id=c.customer_id
left join hive_metastore.prod_publish_dw.vw_dims_customer d on a.end_hco_id=d.customer_id
where a.affiliation_type_code = 'HCO-to-HCO'
      and relationship_type_code='OW'
      -- and upper(c.name) like '%CLEVELAND CLINIC%' 
order by org_name

-- COMMAND ----------


-- pull all HCP-HCO affiliation data

with HCP_NPI_list as (
  select distinct customer_id, FREQ_HCP_NPI as HCP_NPI
  from hive_metastore.prod_publish_dw.vw_dims_customer_identifiers
  inner join hive_metastore.labs_ambit_byod.ambit_geo_freq_npi_list
  on source_system_id = FREQ_HCP_NPI
  where customer_type = 'HCP'
)
select a.*, c.name as org_name
  from hive_metastore.prod_publish_dw.vw_dims_customer_affiliation a
  inner join HCP_NPI_list b on a.start_hcp_id = b.customer_id
  left join hive_metastore.prod_publish_dw.vw_dims_customer c on a.END_hco_id=c.customer_id
  where a.affiliation_type_code = 'HCP-to-HCO' and
        a.primary_affiliation_indicator = 'Yes' and
        a.inactive_flag = 'false'




-- COMMAND ----------

-- Testing other affiliation types

with HCP_NPI_list as (
  select distinct customer_id, FREQ_HCP_NPI as HCP_NPI
  from hive_metastore.prod_publish_dw.vw_dims_customer_identifiers
  inner join hive_metastore.labs_ambit_byod.ambit_geo_freq_npi_list
  on source_system_id = FREQ_HCP_NPI
  where customer_type = 'HCP'
),
HCO_list as (
  select distinct a.end_hco_id
  from hive_metastore.prod_publish_dw.vw_dims_customer_affiliation a
  inner join HCP_NPI_list b
  on a.start_hcp_id = b.customer_id
  where a.affiliation_type_code = 'HCP-to-HCO' and
        a.primary_affiliation_indicator = 'Yes' and
        a.inactive_flag = 'false'
)
select a.*, c.name as org_name, d.name as owner_org_name
from hive_metastore.prod_publish_dw.vw_dims_customer_affiliation a
inner join HCO_list b on b.end_hco_id = a.start_hco_id
left join hive_metastore.prod_publish_dw.vw_dims_customer c on a.start_hco_id=c.customer_id
left join hive_metastore.prod_publish_dw.vw_dims_customer d on a.end_hco_id=d.customer_id
where a.affiliation_type_code = 'HCO-to-HCO'
      and relationship_type_code in ('LS', 'MG', 'OW')
      and upper(c.name) like '%CLEVELAND CLINIC%' 
order by org_name

-- COMMAND ----------

SELECT *
FROM hive_metastore.prod_publish_dw.vw_dims_customer_affiliation
WHERE START_HCO_ID='HCO0000101414'

-- COMMAND ----------

SELECT count(distinct customer_id), count(DISTINCT npi)
FROM hive_metastore.prod_publish_dw.vw_dims_customer 
where customer_id in (select HS_HCO_ID from hive_metastore.labs_ambit_byod.ar_hco_id) and npi is not null
; 

-- COMMAND ----------

SELECT distinct customer_id, source_system_id
FROM hive_metastore.prod_publish_dw.vw_dims_customer_identifiers
where customer_id in (select HS_HCO_ID from hive_metastore.labs_ambit_byod.ar_hco_id) and source_system_type = 'NPI'
; 
