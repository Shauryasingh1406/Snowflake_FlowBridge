-- Gold layer dynamic tables for aggregation KPIs (base, order fulfillment, supplier performance)
-- Co-authored with CoCo
---purpose--> Gold layer 
--dynamic tables 
--AGG_BASE (base layer -joins silver  dims + fact)
--AGG_ORDER_FULLFILLMENT(Downstream)
--AGG_SUPPLIER_PERFORMANCE(downstream)
--AGG_INVENTORY_TURNOVER(downstream)
--AGG_SHIPMENT_DELAYS(downstream)
----AGG_BASE--> LAG='1 Minute' (queries silver)
---All others --> LAG= downstream (refresh when base refreshes )
--Benefit --> silver queries once ,all KPI's  consistent
use role sysadmin;
use warehouse 
FLOWBRIDGE_PIPLELINE_WH;
use database FLOWBRIDGE_DEV_DB;
use schema gold_sch;


---- creating dynamic table for agg_base
create or replace dynamic table gold_sch.agg_base
lag='1 minute'
warehouse= 
FLOWBRIDGE_PIPLELINE_WH
as 
    select 
    f.order_id,f.order_date,f.order_status,f.payment_status,f.quantity,f.unit_price,f.total_amount,
    f.delay_days,f.inventory_level,f.ingested_at,c.customer_id,c.customer_name,c.customer_region,
    c.customer_segment,p.product_id,p.product_name,p.category,sp.supplier_id,
    sp.supplier_name,sp.supplier_country, sp.lead_time_days,sp.performance_score,w.warehouse_id,
    w.warehouse_location ,sh.shipment_id,sh.carrier, sh.ship_date,sh.estimated_delivery
    from silver_sch.fact_orders as f 
    join silver_sch.dim_customers c on c.customer_sk=f.customer_sk
    join silver_sch.dim_product p on p.product_sk=f.product_sk
    join silver_sch.dim_supplier sp on sp.supplier_sk=f.supplier_sk
    join  silver_sch.dim_warehouse w on w.warehouse_sk = f.warehouse_sk
    left join silver_sch.dim_shipment sh on sh.shipment_sk=f.shipment_sk;

    ---
    use schema gold_sch;
select * from gold_sch.agg_base;
select * from  silver_sch.dim_supplier;
----- Dynamic table number -2 
-- AGG_order_fullfillment -->  to order fullfillment KPIs   by customer region 
--source --> AGG_BASE (not silver directly )
----LAG--> downstream refreshes when AGG_REFRESHES 
--KPIS 
--total_orders--> total order per region 
--delivered_orders --> orders with  status delivered 
-- cancelled orders --> orders with cancelled orders 
-- fullfillment_rate -- > delivered/total*100
--total_reveenue --> sum of totak amount 
--avg_order_value --> average order amount 
create or replace dynamic table  gold_sch.agg_order_fullfillment 
lag= downstream 
warehouse = FLOWBRIDGE_PIPLELINE_WH
as 
select 
customer_region, 
customer_segment , 
count(order_id) as total_orders, 
count (case when order_status= 'DELIVERED' then 1  end ) as delivered_orders,
count (case when order_status in ('PENDING','PROCESSING')THEN 1  END) AS pending_orders , 
count (case when order_status ='CANCELLED' then 1 end ) as cancelled_orders , 
count(case when order_status = 'IN TRANSIT' then 1 end ) as  in_transit_orders , 
round(count(case when order_status ='DELIVERED' then 1 end )/nullif(count(order_id),0)*100,2) as fullfillment_rate_pct, 
round(sum(total_amount),2) as total_revenue, 
round(avg(total_amount),2) as average_order_value , 
-- payment breakdown 
count(case when payment_status ='PAID' then 1 end) as paid_orders, 
count(case when payment_status = 'PENDING' then 1 end ) as payment_pending_orders , 
count(case when payment_status = 'OVERDUE' then 1 end) as overdue_orders
from gold_sch.agg_base
group by customer_region , customer_segment ;


select * from agg_base;
select  * from agg_order_fullfillment;
---- Next step--> AGG_SUPPLIER_PERFORMANCE(Downstream)
----Purpose --> Supplier performance KPIs
--Source--> AGG_BASE (Not from silver)
--LAG--> DOWNSTREAM --> refreshes when AGG_BASE gets refreshes 
---KPIs
---------total_orders--> orders per supplier 
---------avg_lead_time_days --> average lead time 
---------total_revenue --> revenue generated 
---------on_time_orders --> orders with delay days =0 
--------- delayed_orders ---> orders with delay days >0
---------on_time_rate ----> on time/total*100
--------- avg_delay_days ---> average delay  when delayed 
create or replace dynamic table gold_sch.agg_supplier_performance
lag = downstream 
warehouse= FLOWBRIDGE_PIPLELINE_WH 
as 
    select 
            supplier_id , 
            supplier_name , 
            supplier_country, 
            -- order volume 
            count(order_id) as total_oders , 
            round(avg(performance_score), 2) as average_performance_score , 
            round(avg(lead_time_Days),2) as average_lead_time_Days, 
            -- revenue 
            round(sum(total_amount),2)  as total_revenue , 
            round(avg(total_amount),2) as avg_total_revenue , 
            -- delay metrics 
            count(case when delay_days =0 then 1 end) as on_time_orders , 
            count(case when delay_days>0 then 1 end ) as delayed_orders , 

            -- on time 
            round(count(case when delay_days =0 then 1 end)/nullif(count(order_id),0)*100,2) as on_time_rate_pct,
            round(avg(case when delay_days > 0 then delay_days end),2) as avg_delay_days 
            from gold_sch.agg_base 
            group by supplier_id , supplier_name, supplier_country ;

            select * from gold_sch.agg_supplier_performance;


            -------------------------------------------------------------------
            -- next step --> other aggr table 
            -- KPIs 
            --total_orders--> orders ordered per warehouse 
            --total_quantity --> total units per order 
            --avg_inventory_level -- > average inventiry snapshot 
            --min_inventory_level -->  lowest inventory snapshot 
            --max_inventory_level --> highest inventory snapshot 
            --total_revenue --> revenue per warehoue 
            -- inventory_turnover -- > total_quantity/avg_inventory 


        create or replace dynamic table gold_sch.agg_iventory_turnover
        lag = downstream 
        warehouse= FLOWBRIDGE_PIPLELINE_WH
         as 
            select 
                    warehouse_id , 
                    warehouse_location , 
                    category , 
                    -- order volume 
                    count(order_id) as total_orders , 
                    count (quantity) as total_quantity , 
                    -- inventory metrices 
                    round(avg(inventory_level),2) as avg_inventory_level , 
                    min(inventory_level) as min_inventory_level , 
                    max(inventory_level) as max_inventory_level  , 
                    -- revenue 
                    round(sum(total_amount),2) as total_revenue , 
                    round(sum(quantity)/nullif(avg(inventory_level),0),2) as  inventory_turnover_ratio, 
                    from gold_sch.agg_base
                    group by 
                    warehouse_id, warehouse_location, category;

                    select * from gold_sch.agg_iventory_turnover;



--- next step --> shipment agg table --(Downstream)
--KPIs 
-----AGG_SHIPMENT_DELAYS (DOWNSTREAM)
---purpose --> shipment delay KPIs  by carrier 
---Source --> AGG_BASE (NOT SILVER DIRECTLY)
---LAG--> DOWNSTREAM 
---FILTER--> ONLY SHIPPED ORDERS (SHIPMENT_ID IS NOT NULL AND SHIPMENT_ID != 'unknown')
---KPIs--> total_shipment --> shipments per carrier
---on_time_shipment --> delay_days =0
--- delay_shipment ---> delay_days>0
---on_time_rate --> on time/total* 100 
---avg_delay_days --> average delays across all  shipments 
---max_delay_days ---> worst delay 
---total_revenue ---> revenue per carrier 
create or replace dynamic table gold_sch.AGG_SHIPMENT_DELAYS 
Lag= downstream 
warehouse= FLOWBRIDGE_PIPLELINE_WH 
as 
select 
    carrier , 
    customer_region, 
    count(order_id) as total_shipments,
    count(case when delay_days=0 then 1 end) as on_time_shipments,
    count(case when delay_days> 0 then 1 end ) as delayed_shipments , 
    round(count(case when delay_days =0 then 1 end)/nullif(count(order_id),0)*100,2) as on_time_rate_pct, 
    round(avg(delay_days),2) as average_delay_days ,
    max(delay_days) as max_delay_days   , 
    --revenue 
    round(sum(total_amount),2) as total_revenue,
    from gold_sch.agg_base

    ---only include shipment orders
    where shipment_id is not null 
    group by carrier, customer_region; 


    select * from AGG_SHIPMENT_DELAYS ; 

    show dynamic tables in schema gold_sch ;

    --- check history 
    select * from table (information_schema.dynamic_table_refresh_history()) where schema_name = 'GOLD_SCH' ;
    
    
    
    
    
            
