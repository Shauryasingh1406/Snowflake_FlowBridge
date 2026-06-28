use role sysadmin;
use database flowbridge_dev_db;
use warehouse flowbridge_pipleline_wh;
use schema silver_sch;
-----------------------------------------------------
select * from dim_customers;
select * from dim_warehouse;
select * from dim_shipment;
select * from dim_product;
select * from dim_supplier;
select * from fact_orders limit 50;

--------------------------------------------------------------------------------------

select dc.customer_name,count(fo.order_sk),sum(fo.total_amount)from dim_customers  dc
join fact_orders fo on  fo.customer_sk= dc.customer_sk group by customer_name;

select dp.category,count(fo.order_sk) as orders,count(case when fo.order_status='Cancelled'  then 1 end)from dim_product dp
join fact_orders fo on  fo.product_sk = dp.product_sk
group by dp.category;

select dw.warehouse_location,count(case when fo.payment_status='OVERDUE' then 1 end) from dim_warehouse dw
join fact_orders fo on dw.warehouse_sk= fo.warehouse_sk
group by dw.warehouse_location
having count(*)>5 ;

select * from dim_warehouse;

show warehouses;

select *  from fact_orders where unit_price >(select avg(fo.unit_price) from fact_orders fo);
select * from dim_customers;
select * from fact_orders fo where fo.customer_sk in  (select  customer_sk from dim_customers where customer_segment= 'ENTERPRISE' )


select * from bronze_sch.raw_orders;






