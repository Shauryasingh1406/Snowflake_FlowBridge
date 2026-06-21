-- Silver layer: stream-based bronze-to-silver ETL with dead letter routing and SCD Type 1 merge
-- Co-authored with CoCo
use  schema flowbridge_dev_db.silver_sch;
---ENV--
set env = 'DEV';
set DB = 'FLOWBRIDGE_'|| $env||'_DB';
use database  identifier($DB);
use role sysadmin;
use schema silver_sch;
use warehouse flowbridge_pipleline_wh;
------
------Dead Letter table -----
--Transient --> stores bad and rejected records 
create or replace transient table silver_sch.dead_letter(
raw_data variant ,
error_reason string ,
file_name string ,
file_row_number number ,
rejected_at timestamp_ntz default current_timestamp()
);
----STG_Orders table 
--------Permanent table 
create or replace table stg_orders (
----Order columns 
order_id string ,
order_Date timestamp_ntz ,
order_status string ,

--customer columns 
customer_id  string,
customer_name  string  , 
customer_region  string , 
customer_segment string ,

--supplier fields 
supplier_id string ,
supplier_name string , 
supplier_country string , 
lead_time_Days number ,
performance_score float ,

--shipment fields 
shipment_id string , 
carrier string ,
ship_Date timestamp_ntz,
estimated_delivery date ,
delay_days number , 


--product fields 
product_id string ,
product_name string , 
category string , 
quantity number , 
unit_price float ,

--financial fields 
total_amount float ,
payment_status string ,
--warehouse fields 

warehouse_id string , 
warehouse_location string , 
inventory_level number ,

--metadata 
file_name string ,
file_row_number number , 
ingested_at  timestamp_ntz,
transformed_at timestamp_ntz default current_timestamp()
) comment = ' silver layer stage 1 -- flattened and clean supply chain ';


---Stream on records  --> to capture new records from bronze_layer 
create or replace stream silver_sch.raw_orders_stream 
on table flowbridge_dev_db.bronze_sch.raw_orders
append_only= true 
show_initial_rows = true ;
show streams ;
-------
-----------Create Stored procedure --> sp_bronze_to_silver
--purpose --> 1.Route bad records to dead_letter 2. merge clean records to stg_orders
--called by --> bronze_to_silver_task (every 1 minute )
---test by --> call silver_sch.sp_bronze_to_silver
-----------

create or replace procedure silver_sch.sp_bronze_to_silver()
returns string 
language sql
as 

begin 
    -- part 0 --> buffer stream into temp table 
    -- stream can only be  consumed once per DML 
    -- we materialise it here so  both dead_letter  and merge can read from same snapshot 
    --------------------
    create temporary table silver_sch.stream_buffer as select * from silver_sch.raw_orders_stream
    where metadata$action = 'INSERT';

    ---part -1 -->  route bad records  to dead_letter
    --only true unrecoverable records are rejected 
    --validation is case-insensitive  using upper(trim())
    --order_Date: rejects only when both conditions fail 
    --(AND logic)-ISO format passes TRY_TO_TIMESTAMP_NTZ
    --DD-MM-YYYY  matches like pattern 
    --order_status :upper(trim()) before IN check 
    --so shipped/SHIPPED/Shipped all can pass 
    ------------------------------------------
    Insert into silver_sch.dead_letter(
    Raw_Data,
    error_Reason,
    file_name, 
    file_row_number
    )

    select 
        s.RAW_DATA,
        case 
        --- order level valdiation ----
        when s.RAW_DATA:order_id::string is null 
            then 'missing order id '
        when try_to_timestamp_ntz(s.raw_data:order_date::string) is null 
        and s.raw_data:order_date::string  not like '__-__-____'
        then 
        'Invalid or missing date'
        when upper(trim(s.raw_data:order_status::string)) not in 
        ('PENDING','PROCESSING','SHIPPED','IN TRANSIT','DELIVERED','CANCELLED'
        )
        then 'Invalid order status'
        --CUSTOMER LEVEL VALDIATION ---
        when s.raw_data:customer.customer_id::string is null
        then  'missing customer id '
        ---supplier level ---
        when s.raw_data:supplier.supplier_id::string is null 
            then 'supplier id is mising '
        when s.raw_data:supplier.supplier_performance_score::float  >100 
            then 'Invalid performance score '||s.raw_data:supplier.performance_score::string 
         when s.raw_data:supplier.supplier_performance_score::float  <0 
            then 'Invalid performance score '||s.raw_data:supplier.performance_score::string 
        when s.raw_data:supplier.lead_time_days::number <0
             then 'Invalid lead_time_Days < 0'||s.raw_data:supplier.lead_time_days::string 

        ----Product level--
        when s.raw_data:items[0].product_id::string is null
            then 'Missing product id '
        when s.raw_data:items[0].quantity::number <= 0
            then 'invalid quantity'|| coalesce(s.raw_data:items[0].quantity::string,'null')
        when s.raw_data:items[0].unit_price::float is null 
            or s.raw_data:items[0].unit_price::float <0 
            then 'Invalid unit_price' ||coalesce(s.raw_data:items[0].unit_price::string,'null')
        ---Financial level ------------
        when s.raw_data:financials.total_amount::float is null 
            or s.raw_data:financials.total_amount::float < 0 
            then 'invalid total_amount' || coalesce(s.raw_data:financials.total_amount::string,'Null')
        ---Warehouse level ----------
        when s.raw_data:warehouse.inventory_level::number < 0 
            then 'invalid inventory_level < 0 ' || s.raw_data:warehouse.inventory_level::string 
            else 'unknown'

        end as  ERROR_REASON,
        s.file_name, 
        s.file_row_number 
        from silver_sch.stream_buffer as  S 
        where 
        (
            --- order level ---column level validation/filteration 
            s.raw_data:order_id :: string  is null 
            or (
                    try_to_timestamp_ntz(s.raw_data:order_date::string) is null
                    and s.raw_data:order_date::string not like '__-__-____'
            )
            or upper(trim(s.raw_data:order_status::string))  not in (
                'PENDING','PROCESSING','SHIPPED','IN TRANSIT',
                'DELIVERED','CANCELLED'
            )
            ---CUSTOMER LEVEL 
            OR s.raw_data:customer.customer_id::string is null 
            --supplier level 
            or s.raw_data:supplier.supplier_id::string is null 
            or s.raw_data:supplier.performance_score::float >100 
            or s.raw_data:supplier.performance_score::float <0 
            or s.raw_data:supplier.lead_time_days::number <0 
            --product level 
            or s.raw_data:items[0].product_id::string is null 
            or s.raw_data:items[0].quantity::number is  null
            or s.raw_data:items[0].quantity::number  <= 0 
            or s.raw_data:items[0].unit_price::float is null 
            or s.raw_data:items[0].unit_price::float <0 
            --financial level 
            or s.raw_data:financials.total_amount::float is null 
            or s.raw_data:financials.total_amount::float < 0 
            -- warehouse  level 
            or s.raw_data:warehouse.inventory_level::number < 0
        );
        ---- part 2 - merge clean records into stg_order
        -- when matched update  if not present then insert (SCD Type 1 )
        merge into silver_sch.stg_orders as  tgt using (
    select 
    --order fields 
    upper(trim(s.raw_data:order_id::string)) as order_id ,
        try_to_timestamp_ntz(
            case    
                 when s.raw_data:order_date::string like '__-__-____'
                    then to_varchar(
                        to_date(s.raw_data:order_date::string,'DD-MM-YYYY'),'YYYY-MM-DD') || 'T00:00:00Z'
                    else s.raw_data:order_date::string
                    end
        ) as order_date ,
        upper(trim(s.raw_data:order_status::string)) as order_status ,

        ---customer table ------
        upper(trim(s.raw_data:customer.customer_id::string)) as customer_id ,
        coalesce(upper(trim(s.raw_data:customer.customer_name::string)),'UNKNOWN') as customer_name,
        coalesce(upper(trim(s.raw_data:customer.region::string)),'UNKNOWN') as customer_region,
        coalesce(upper(trim(s.raw_data:customer.segment::string)),'UNKNOWN') as customer_segment,
        --supplier fields 
        upper(trim(s.raw_data:supplier.supplier_id::string)) as supplier_id,
        coalesce(upper(trim(s.raw_data:supplier.supplier_name::string)),'UNKNOWN') as supplier_name,
        coalesce(upper(trim(s.raw_data:supplier.country::string)),'UNKNOWN') as supplier_country,
        coalesce(s.raw_data:supplier.lead_time_days::number, 0) as lead_time_days,
        coalesce(s.raw_data:supplier.performance_score::float, 0) as performance_score,
        ----shipment fields
        coalesce(upper(trim(s.raw_data:shipment.shipment_id::string)),'UNKNOWN') as shipment_id,
        coalesce(upper(trim(s.raw_data:shipment.carrier::string)),'UNKNOWN') as carrier,
        try_to_timestamp_ntz(s.raw_data:shipment.ship_date::string) as ship_date,
        try_to_date(s.raw_data:shipment.estimated_delivery::string) as estimated_delivery,
        greatest(coalesce(s.raw_data:shipment.delay_days::number,0),0) as delay_days,
        --product fields 
        upper(trim(s.raw_data:items[0].product_id::string)) as product_id , 
        coalesce(upper(trim(s.raw_data:items[0].product_name::string)),'UNKNOWN') as product_name,
        coalesce(upper(trim(s.raw_data:items[0].category::string)),'UNKNOWN') as category,
        s.raw_data:items[0].quantity::number as quantity , 
        s.raw_data:items[0].unit_price::float as unit_price,
        --financial fields 
         s.raw_data:financials.total_amount::float as total_amount,
         coalesce(upper(trim(s.raw_data:financials.payment_status::string)),'UNKNOWN') as payment_status,
         -- warehouse fields 
         coalesce(upper(trim(s.raw_data:warehouse.warehouse_id::string)),'UNKNOWN') as warehouse_id,
         coalesce(upper(trim(s.raw_data:warehouse.warehouse_location::string)),'UNKNOWN') as warehouse_location,
         coalesce(s.raw_data:warehouse.inventory_level::number,0) as inventory_level,
         --METADATA 
         s.file_name,
         s.file_row_number,
         s.ingested_at
         from silver_sch.stream_buffer as s 
         where  
        
            --- order level ---column level validation/filteration 
            s.raw_data:order_id::string  is not  null 
            and  (
                    try_to_timestamp_ntz(s.raw_data:order_date::string) is not  null
                    or s.raw_data:order_date::string like '__-__-____'
            )
            and upper(trim(s.raw_data:order_status::string))   in (
                'PENDING','PROCESSING','SHIPPED','IN TRANSIT',
                'DELIVERED','CANCELLED'
            )
            ---CUSTOMER LEVEL 
            and s.raw_data:customer.customer_id::string is not null 
            --supplier level 
            and s.raw_data:supplier.supplier_id::string is not null 
            and s.raw_data:supplier.performance_score::float <= 100 
            and s.raw_data:supplier.performance_score::float >= 0 
            and (s.raw_data:supplier.lead_time_days::number >= 0 
                or s.raw_data:supplier.lead_time_days::number is null)
            --product level 
            and  s.raw_data:items[0].product_id::string is not  null 
            and  s.raw_data:items[0].quantity::number is  not  null
            and  s.raw_data:items[0].quantity::number  > 0 
            and  s.raw_data:items[0].unit_price::float is not  null 
            and  s.raw_data:items[0].unit_price::float >= 0 
            --financial level 
            and  s.raw_data:financials.total_amount::float is  not null 
            and  s.raw_data:financials.total_amount::float >= 0 
            -- warehouse  level 
            and (s.raw_data:warehouse.inventory_level::number >= 0
                 or s.raw_data:warehouse.inventory_level is  null)
                 
        ) as src 
        on tgt.order_id = src.order_id
        when matched then update set 
        tgt.order_status = src.order_status,
        tgt.carrier = src.carrier,
        tgt.ship_date = src.ship_date,
        tgt.estimated_delivery = src.estimated_delivery,
        tgt.delay_days = src.delay_days,
        tgt.inventory_level = src.inventory_level,
        tgt.payment_status = src.payment_status,
        tgt.customer_name = src.customer_name,
        tgt.customer_region = src.customer_region,
        tgt.supplier_name = src.supplier_name,
        tgt.performance_score = src.performance_score,
        tgt.warehouse_id = src.warehouse_id,
        tgt.warehouse_location = src.warehouse_location,
        tgt.transformed_at = current_timestamp()
        when not matched  then insert(
        order_id, order_date, order_status, customer_id, customer_name,
        customer_region, customer_segment, supplier_id, supplier_name,
        supplier_country, lead_time_days, performance_score, shipment_id, carrier, ship_date,
        estimated_delivery, delay_days, product_id, product_name, category, quantity, unit_price,
        total_amount, payment_status, warehouse_id, warehouse_location, inventory_level, file_name,
        file_row_number, ingested_at
        ) values(
        src.order_id, src.order_date, src.order_status, src.customer_id, src.customer_name,
        src.customer_region, src.customer_segment, src.supplier_id, src.supplier_name,
        src.supplier_country, src.lead_time_days, src.performance_score, src.shipment_id, src.carrier, src.ship_date,
        src.estimated_delivery, src.delay_days, src.product_id, src.product_name, src.category,
        src.quantity, src.unit_price,
        src.total_amount, src.payment_status, src.warehouse_id, src.warehouse_location,
        src.inventory_level, src.file_name,
        src.file_row_number, src.ingested_at
        );
         drop table if exists silver_sch.stream_buffer;
         return 'sp_bronze_to_silver completed successfully';
end;
