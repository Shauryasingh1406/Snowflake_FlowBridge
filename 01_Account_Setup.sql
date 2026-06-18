--creating new database ---
create database if not exists flowbridge_dev_db;
create database if not exists flowbridge_prod_db;
---Creating schema---
create schema if not exists flowbridge_prod_db.bronze_sch;
create schema if not exists flowbridge_prod_db.silver_sch;
create schema if not exists flowbridge_prod_db.gold_sch;
create schema if not exists flowbridge_prod_db.serving_sch;

----creating warehouses --
create warehouse if not exists flowbridge_pipleline_wh
warehouse_size='small'
auto_suspend=60
auto_resume= true 
comment='Pipeline workloads- ingestion + tranformation';


create warehouse if not exists flowbridge_analytics_wh
warehouse_size='small'
auto_suspend=60
auto_resume= true 
comment='Pipeline workloads- streamlit + data sharing';


---Pipeline Resource Monitor ----
create or replace  resource monitor flowbridge_pipeline_wh
  with credit_quota =20
  frequency = monthly
  start_timestamp= immediately
  triggers
        on 75 percent do notify 
        on 90 percent do notify 
        on 100 percent do suspend ;


        create or replace  resource monitor flowbridge_pipeline_rm
  with credit_quota =20
  frequency = monthly
  start_timestamp= immediately
  triggers
        on 75 percent do notify 
        on 90 percent do notify 
        on 100 percent do suspend ;

---Analytics Resource Monitor ----
create or replace  resource monitor flowbridge_analytics_rm
  with credit_quota =20
  frequency = monthly
  start_timestamp= immediately
  triggers
        on 75 percent do notify 
        on 90 percent do notify 
        on 100 percent do suspend ;

--Assign resource monitor to warehouses --
alter warehouse  flowbridge_pipleline_wh set  resource_monitor = flowbridge_pipeline_rm;

alter warehouse  flowbridge_analytics_wh set  resource_monitor = flowbridge_analytics_rm;

--Grant privilages to sysadmin --
use role accountadmin

grant execute task on account to role sysadmin ;
grant usage on warehouse flowbridge_pipleline_wh to role sysadmin ;
grant usage on warehouse  flowbridge_analytics_wh to role sysadmin;
grant all privileges on database FLOWBRIDGE_DEV_DB to role sysadmin;
grant all privileges on database FLOWBRIDGE_PROD_DB to role sysadmin;

grant all privileges on schema FLOWBRIDGE_DEV_DB.BRONZE_SCH to role sysadmin ;
grant all privileges on schema FLOWBRIDGE_DEV_DB.GOLD_SCH to role sysadmin ;
grant all privileges on schema FLOWBRIDGE_DEV_DB.SILVER_SCH to role sysadmin ;
grant all privileges on schema FLOWBRIDGE_DEV_DB.SERVING_SCH to role sysadmin ;

grant all privileges  on all schemas 
in database FLOWBRIDGE_PROD_DB  to role sysadmin;



show databases like 'flow%';
show warehouses like 'flow%';
show resource monitors like 'flow%';

alter warehouse flowbridge_pipleline_wh  set warehouse_size ='x-small';

alter warehouse flowbridge_analytics_wh  set warehouse_size ='x-small';




