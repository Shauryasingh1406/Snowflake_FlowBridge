 -- purpose : Storgae Integration , file format , external layer , RAW_ORDERS table , snowpipe 
--ENV--
use database flowbridge_dev_db ;

use role accountadmin;


create storage integration if not exists  flowbridge_ADLS_integration 
type = external_stage 
storage_provider= 'azure'
enabled= true
azure_tenant_id = '9789ecb1-6847-4f93-86fd-bedaacb1aae9'
storage_allowed_locations = ('azure://flowbridgeadls.blob.core.windows.net/supplychain-raw-dev/',
'azure://flowbridgeadls.blob.core.windows.net/supplychain-raw-prod/');


describe integration flowbridge_ADLS_integration ;

grant usage on integration flowbridge_ADLS_integration to role sysadmin ;

---Setting notification Integration --
create or replace notification integration flowbridge_Azure_integration 
enabled = true 
type= queue 
notification_provider = azure_storage_queue 
azure_storage_queue_primary_uri = 'https://flowbridgeadls.queue.core.windows.net/flowspqueue'
azure_tenant_id=  '9789ecb1-6847-4f93-86fd-bedaacb1aae9';


desc integration flowbridge_Azure_integration ;
grant usage on integration flowbridge_Azure_integration to role sysadmin ;
use role sysadmin;
use database flowbridge_dev_db;
use  schema flowbridge_dev_db.bronze_sch;
use warehouse flowbridge_pipleline_wh;
----FILE FORMAT---
create file format if not exists bronze_sch.json_file_format
type ='json'
strip_outer_array= true 
comment='JjSON file format';

desc file format bronze_sch.json_file_format;
----External Stage --
create stage if not exists bronze_sch.adls_raw_stage 
url='azure://flowbridgeadls.blob.core.windows.net/supplychain-raw-dev/'
storage_integration =flowbridge_ADLS_integration
file_format= bronze_sch.json_file_format;

list @bronze_sch.adls_raw_stage;
---Raw orders table ---
create or replace transient table bronze_sch.raw_orders(
raw_data variant,
ingested_at timestamp_ntz default current_timestamp(),
file_name string  , 
file_row_number  number,
load_id  string default uuid_string()
);


-----------
----------SNOWPIPE
---Auto ingest triggered  by Azure  Event grid  on file arrival 
-----------
create pipe if not exists bronze_sch.supply_chain_pipe
 auto_ingest = true 
 integration = flowbridge_Azure_integration
 as 
    copy into bronze_sch.raw_orders(
        raw_data,
        file_name,
        file_row_number
    )
    from 
    (
        select $1,
                metadata$filename,
                metadata$file_row_number
        from @bronze_sch.adls_raw_stage
    ) file_format =(format_name='bronze_sch.json_file_format');



    show pipes;
    select system$pipe_status('bronze_sch.supply_chain_pipe');
    alter pipe bronze_sch.supply_chain_pipe  refresh ;

    select * from bronze_sch.raw_orders;
---check injestion history 
    select * from  table (information_schema.copy_history(
        table_name => 'raw_orders',
        start_time => dateadd(hours,-1,current_timestamp())
    ));

show warehouses;
show databases;
use schema BRONZE_SCH;