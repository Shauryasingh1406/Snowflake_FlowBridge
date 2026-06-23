-- Silver layer stage 2: sequences, dimension tables, fact table, stream, and star schema builder
-- Co-authored with CoCo
use role sysadmin;
use database flowbridge_dev_db;
use schema silver_sch; 
use warehouse flowbridge_pipleline_wh;

------Step 1 --> Sequences 
--purpose --> Auto incerement surrogate keys  for all tables 
--Must run Before tables that references them 
--Relative reference --  no hardode database name 
-- works correctly  in both dev and prod after clone 

create sequence if not exists silver_sch.dim_customer_sk_seq
start=1 
increment =1 ;

create sequence if not exists silver_sch.dim_product_sk_seq
start=1 
increment =1 ;

create sequence if not exists silver_sch.dim_supplier_sk_seq
start=1 
increment =1 ;


create sequence if not exists silver_sch.dim_warehouse_sk_seq
start=1 
increment =1 ;

create sequence if not exists silver_sch.dim_shipment_sk_seq
start=1 
increment =1 ;

create sequence if not exists silver_sch.fact_orders_sk_seq
start=1 
increment =1 ;


---Step 2 --> creation of  DIM_CUSTOMERS 

create or replace table silver_sch.dim_customers (
customer_sk number default  silver_sch.dim_customer_sk_seq.nextval,
customer_id string  not null , 
customer_name string not null,
customer_region string not null ,
customer_Segment string  not null , 
is_active boolean default true , 
created_at timestamp_ntz  default current_timestamp(),
updated_at timestamp_ntz  default current_timestamp()
) comment='customer dimernsion for star schema ';

--step 3 DIM_Product 
create or replace table silver_sch.dim_product (
product_sk number default  silver_sch.dim_product_sk_seq.nextval,
product_id string  not null , 
product_name string not null,
category string not null ,
unit_price float  not null ,
is_active boolean default true , 
created_at timestamp_ntz  default current_timestamp(),
updated_at timestamp_ntz  default current_timestamp()
) comment='product dimension for star schema ';


create or replace table silver_sch.dim_supplier (
supplier_sk number default  silver_sch.dim_supplier_sk_seq.nextval,
supplier_id string  not null , 
supplier_name string not null,
supplier_country string not null ,
lead_time_days number not null ,
performance_score float not null,
is_active boolean default true , 
created_at timestamp_ntz  default current_timestamp(),
updated_at timestamp_ntz  default current_timestamp()
) comment='supplier dimension for star schema ';


create or replace table silver_sch.dim_shipment (
shipment_sk number default  silver_sch.dim_shipment_sk_seq.nextval,
shipment_id string  not null , 
carrier string not null ,  
ship_Date timestamp_ntz ,
estimated_delivery timestamp_ntz, 
is_active boolean default true , 
created_at timestamp_ntz  default current_timestamp(),
updated_at timestamp_ntz  default current_timestamp()
) comment='shipment dimension for star schema ';

create or replace table silver_sch.dim_warehouse (
warehouse_sk number default  silver_sch.dim_warehouse_sk_seq.nextval,
warehouse_id string  not null , 
warehouse_location string not null ,    
is_active boolean default true , 
created_at timestamp_ntz  default current_timestamp(),
updated_at timestamp_ntz  default current_timestamp()
) comment='warehouse dimension for star schema ';


create or replace table silver_sch.dim_shipment (
shipment_sk number default  silver_sch.dim_shipment_sk_seq.nextval,
shipment_id string  not null , 
carrier string not null ,  
ship_Date timestamp_ntz ,
estimated_delivery timestamp_ntz, 
is_active boolean default true , 
created_at timestamp_ntz  default current_timestamp(),
updated_at timestamp_ntz  default current_timestamp()
) comment='shipment dimension for star schema ';


--Next step --Fact table 
----purpose: central fact table --> one row per order 

create or replace table silver_sch.fact_orders(
order_sk number default silver_sch.fact_orders_sk_seq.nextval,
order_id string not null , 
customer_sk  number not null,
product_sk  number not null,
supplier_sk  number not null,
warehouse_sk number not null, 
shipment_sk number not null , 
order_date timestamp_ntz not null , 
order_status string not null , 
payment_status string not null ,

quantity number not null , 
unit_price float not null , 
total_amount number not null , 
delay_days  number not null , 
inventory_level number not null , 
created_at timestamp_ntz default current_timestamp(), 
updated_at timestamp_ntz default current_timestamp(),
ingested_at timestamp_ntz
); 

------------------------------------------
--next step --> stream on stg_orders
--> purpose - to capture  insert+update  from stage 1 merge 
--type--> standard stream (no append only )
--stage 1 merge can insert and update  stg_orders  we we need  full cdc -> Insert + update 
--consumed by --> sp_silver_to_star stored procedure 

create or replace stream silver_sch.stg_orders_stream 
    on table silver_sch.stg_orders
    show_initial_rows=true ;
show streams ;

drop stream if exists silver_sch.RAW_ORDERS_STREM;

------------------------------Next step stored procedure --> sp_silver_to_Star
--purpose--> build star schema  from stg_orders 
-- runs in strict order - dims first , fact last 
--1. merge--> dim_customer
--2. merge--> dim_product
--3.merge--> dim_supplier
--4.merge -->dim_warehouse and so on 
-- why order matters because fact table feeds on data from these tables
-- ============================================================
-- Silver Layer Stage 2 — Star Schema Builder
-- Purpose   : Build and maintain star schema from STG_ORDERS
-- Author    : Flowbridge Project
-- Co-authored with CoCo
-- ============================================================

-- ENV SETUP
USE ROLE SYSADMIN;
USE DATABASE FLOWBRIDGE_DEV_DB;
USE SCHEMA SILVER_SCH;
USE WAREHOUSE FLOWBRIDGE_PIPLELINE_WH;

-- ============================================================
-- STEP 1 — SEQUENCES
-- Purpose  : Auto-increment surrogate keys for all dim + fact tables
-- Must run : Before tables that reference them
-- Note     : Relative reference — no hardcoded DB name
--            Works correctly in both dev and prod after clone
-- ============================================================

CREATE SEQUENCE IF NOT EXISTS SILVER_SCH.DIM_CUSTOMER_SK_SEQ  START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS SILVER_SCH.DIM_PRODUCT_SK_SEQ   START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS SILVER_SCH.DIM_SUPPLIER_SK_SEQ  START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS SILVER_SCH.DIM_WAREHOUSE_SK_SEQ START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS SILVER_SCH.DIM_SHIPMENT_SK_SEQ  START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS SILVER_SCH.FACT_ORDERS_SK_SEQ   START = 1 INCREMENT = 1;

-- ============================================================
-- STEP 2 — DIMENSION TABLES
-- ============================================================

-- DIM_CUSTOMER
CREATE OR REPLACE TABLE SILVER_SCH.DIM_CUSTOMERS (
    customer_sk      NUMBER        DEFAULT SILVER_SCH.DIM_CUSTOMER_SK_SEQ.NEXTVAL,
    customer_id      STRING        NOT NULL,
    customer_name    STRING        NOT NULL,
    customer_region  STRING        NOT NULL,
    customer_segment STRING        NOT NULL,
    is_active        BOOLEAN       DEFAULT TRUE,
    created_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Customer dimension for star schema — surrogate key via sequence';

-- DIM_PRODUCT
CREATE OR REPLACE TABLE SILVER_SCH.DIM_PRODUCT (
    product_sk   NUMBER        DEFAULT SILVER_SCH.DIM_PRODUCT_SK_SEQ.NEXTVAL,
    product_id   STRING        NOT NULL,
    product_name STRING        NOT NULL,
    category     STRING        NOT NULL,
    unit_price   FLOAT         NOT NULL,
    is_active    BOOLEAN       DEFAULT TRUE,
    created_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Product dimension for star schema — surrogate key via sequence';

-- DIM_SUPPLIER
-- NOTE: removed unit_price (not a supplier attribute — that belongs to product)

CREATE OR REPLACE TABLE SILVER_SCH.DIM_SUPPLIER (
    supplier_sk      NUMBER        DEFAULT SILVER_SCH.DIM_SUPPLIER_SK_SEQ.NEXTVAL,
    supplier_id      STRING        NOT NULL,
    supplier_name    STRING        NOT NULL,
    supplier_country STRING        NOT NULL,
    lead_time_days   NUMBER        NOT NULL,
    performance_score FLOAT        NOT NULL,
    is_active        BOOLEAN       DEFAULT TRUE,
    created_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Supplier dimension for star schema — surrogate key via sequence';

-- DIM_WAREHOUSE
CREATE OR REPLACE TABLE SILVER_SCH.DIM_WAREHOUSE (
    warehouse_sk       NUMBER        DEFAULT SILVER_SCH.DIM_WAREHOUSE_SK_SEQ.NEXTVAL,
    warehouse_id       STRING        NOT NULL,
    warehouse_location STRING        NOT NULL,
    is_active          BOOLEAN       DEFAULT TRUE,
    created_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Warehouse dimension for star schema — surrogate key via sequence';

-- DIM_SHIPMENT
CREATE OR REPLACE TABLE SILVER_SCH.DIM_SHIPMENT (
    shipment_sk        NUMBER        DEFAULT SILVER_SCH.DIM_SHIPMENT_SK_SEQ.NEXTVAL,
    shipment_id        STRING        NOT NULL,
    carrier            STRING        NOT NULL,
    ship_date          TIMESTAMP_NTZ,
    estimated_delivery DATE,
    is_active          BOOLEAN       DEFAULT TRUE,
    created_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Shipment dimension for star schema — surrogate key via sequence';

-- ============================================================
-- STEP 3 — FACT TABLE
-- Purpose  : Central fact table — one row per order
--            5 foreign keys (surrogate keys from dims)
--            6 measures: quantity, unit_price, total_amount,
--                        delay_days, inventory_level, payment_status
-- NOTE     : Corrected typo from face_orders → fact_orders
-- ============================================================

CREATE OR REPLACE TABLE SILVER_SCH.FACT_ORDERS (
    order_sk         NUMBER        DEFAULT SILVER_SCH.FACT_ORDERS_SK_SEQ.NEXTVAL,
    order_id         STRING        NOT NULL,
    -- Foreign keys — surrogate keys from dimension tables
    customer_sk      NUMBER        NOT NULL,
    product_sk       NUMBER        NOT NULL,
    supplier_sk      NUMBER        NOT NULL,
    warehouse_sk     NUMBER        NOT NULL,
    shipment_sk      NUMBER        NOT NULL,
    -- Order fields
    order_date       TIMESTAMP_NTZ NOT NULL,
    order_status     STRING        NOT NULL,
    payment_status   STRING        NOT NULL,
    -- Measures
    quantity         NUMBER        NOT NULL,
    unit_price       FLOAT         NOT NULL,
    total_amount     FLOAT         NOT NULL,
    delay_days       NUMBER        NOT NULL,
    inventory_level  NUMBER        NOT NULL,
    -- Metadata
    ingested_at      TIMESTAMP_NTZ,
    created_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Fact table — one row per order. 5 dim foreign keys + 6 measures';

-- ============================================================
-- STEP 4 — STREAM ON STG_ORDERS
-- Purpose  : Capture INSERT + UPDATE from stage 1 merge
-- Type     : Standard stream (NOT append_only)
-- Why      : Stage 1 merge can INSERT and UPDATE stg_orders
--            We need full CDC (insert + update)
--            append_only=true would miss updates
-- Consumed by : sp_silver_to_star stored procedure
-- ============================================================

CREATE OR REPLACE STREAM SILVER_SCH.STG_ORDERS_STREAM
    ON TABLE SILVER_SCH.STG_ORDERS
    SHOW_INITIAL_ROWS = TRUE;

SHOW STREAMS;

-- ============================================================
-- STEP 5 — STORED PROCEDURE: SP_SILVER_TO_STAR
-- ============================================================
-- Purpose   : Build and maintain complete star schema
--             from STG_ORDERS via stg_orders_stream
--
-- Execution order (strict — dims before fact):
--   Part 0 → Buffer stream into temp table
--   Part 1 → Merge DIM_CUSTOMERS
--   Part 2 → Merge DIM_PRODUCT
--   Part 3 → Merge DIM_SUPPLIER
--   Part 4 → Merge DIM_WAREHOUSE
--   Part 5 → Merge DIM_SHIPMENT (skips unshipped orders)
--   Part 6 → Merge FACT_ORDERS (joins all dims for SKs)
--   Part 7 → Drop temp table
--
-- Why order matters:
--   FACT_ORDERS needs surrogate keys from ALL dims
--   Dims must exist before fact can look up their SKs
--   If a dim merge fails, fact merge will have NULL FKs
--
-- Called by : silver_to_star_task (every 1 minute)
-- Test with : CALL SILVER_SCH.SP_SILVER_TO_STAR();
-- ============================================================

CREATE OR REPLACE PROCEDURE SILVER_SCH.SP_SILVER_TO_STAR()
RETURNS STRING
LANGUAGE SQL
AS

BEGIN

    -- --------------------------------------------------------
    -- PART 0 — BUFFER STREAM INTO TEMP TABLE
    -- Why     : Stream can only be consumed ONCE per DML
    --           If we read stream twice (once per merge)
    --           second read returns empty — data already consumed
    --           Materialise into temp table so all 6 merges
    --           read from the same consistent snapshot
    -- --------------------------------------------------------
    CREATE OR REPLACE TEMPORARY TABLE SILVER_SCH.TEMP_STREAM_DATA AS
        SELECT * FROM SILVER_SCH.STG_ORDERS_STREAM
        WHERE METADATA$ACTION = 'INSERT';

    -- --------------------------------------------------------
    -- PART 1 — MERGE DIM_CUSTOMERS
    -- Business key : customer_id
    -- DDUP logic   : QUALIFY row_number() keeps latest order
    --                per customer — avoids ambiguous merge
    -- When matched : Update mutable fields (name, region,
    --                segment) — protect surrogate key
    -- When not matched : Insert new customer with auto SK
    -- --------------------------------------------------------
    MERGE INTO SILVER_SCH.DIM_CUSTOMERS AS TGT
    USING (
        SELECT
            UPPER(TRIM(customer_id))      AS customer_id,
            UPPER(TRIM(customer_name))    AS customer_name,
            UPPER(TRIM(customer_region))  AS customer_region,
            UPPER(TRIM(customer_segment)) AS customer_segment
        FROM SILVER_SCH.TEMP_STREAM_DATA
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(customer_id))
            ORDER BY order_date DESC
        ) = 1
    ) AS SRC
    ON TGT.customer_id = SRC.customer_id
    WHEN MATCHED THEN UPDATE SET
        TGT.customer_name    = SRC.customer_name,
        TGT.customer_region  = SRC.customer_region,
        TGT.customer_segment = SRC.customer_segment,
        TGT.is_active        = TRUE,
        TGT.updated_at       = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        customer_id,
        customer_name,
        customer_region,
        customer_segment
    ) VALUES (
        SRC.customer_id,
        SRC.customer_name,
        SRC.customer_region,
        SRC.customer_segment
    );
    -- NOTE: customer_sk is auto-generated by sequence default
    --       Do NOT include in INSERT columns list — Snowflake
    --       assigns it automatically from the sequence

    -- --------------------------------------------------------
    -- PART 2 — MERGE DIM_PRODUCT
    -- Business key : product_id
    -- Why product has unit_price in dim:
    --   Product price is a slowly changing attribute
    --   If price changes, we update the dim (SCD Type 1)
    --   Fact table stores the unit_price AT ORDER TIME
    --   both are needed for different analysis
    -- --------------------------------------------------------
    MERGE INTO SILVER_SCH.DIM_PRODUCT AS TGT
    USING (
        SELECT
            UPPER(TRIM(product_id))   AS product_id,
            UPPER(TRIM(product_name)) AS product_name,
            UPPER(TRIM(category))     AS category,
            unit_price
        FROM SILVER_SCH.TEMP_STREAM_DATA
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(product_id))
            ORDER BY order_date DESC
        ) = 1
    ) AS SRC
    ON TGT.product_id = SRC.product_id
    WHEN MATCHED THEN UPDATE SET
        TGT.product_name = SRC.product_name,
        TGT.category     = SRC.category,
        TGT.unit_price   = SRC.unit_price,
        TGT.is_active    = TRUE,
        TGT.updated_at   = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        product_id,
        product_name,
        category,
        unit_price
    ) VALUES (
        SRC.product_id,
        SRC.product_name,
        SRC.category,
        SRC.unit_price
    );

    -- --------------------------------------------------------
    -- PART 3 — MERGE DIM_SUPPLIER
    -- Business key : supplier_id
    -- Performance score and lead_time can change over time
    -- SCD Type 1 — always keep latest values (overwrite)
    -- --------------------------------------------------------
    MERGE INTO SILVER_SCH.DIM_SUPPLIER AS TGT
    USING (
        SELECT
            UPPER(TRIM(supplier_id))      AS supplier_id,
            UPPER(TRIM(supplier_name))    AS supplier_name,
            UPPER(TRIM(supplier_country)) AS supplier_country,
            lead_time_days,
            performance_score
        FROM SILVER_SCH.TEMP_STREAM_DATA
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(supplier_id))
            ORDER BY order_date DESC
        ) = 1
    ) AS SRC
    ON TGT.supplier_id = SRC.supplier_id
    WHEN MATCHED THEN UPDATE SET
        TGT.supplier_name     = SRC.supplier_name,
        TGT.supplier_country  = SRC.supplier_country,
        TGT.lead_time_days    = SRC.lead_time_days,
        TGT.performance_score = SRC.performance_score,
        TGT.is_active         = TRUE,
        TGT.updated_at        = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        supplier_id,
        supplier_name,
        supplier_country,
        lead_time_days,
        performance_score
    ) VALUES (
        SRC.supplier_id,
        SRC.supplier_name,
        SRC.supplier_country,
        SRC.lead_time_days,
        SRC.performance_score
    );

    -- --------------------------------------------------------
    -- PART 4 — MERGE DIM_WAREHOUSE
    -- Business key : warehouse_id
    -- Warehouse location can change (e.g. relocated)
    -- SCD Type 1 — keep latest location
    -- --------------------------------------------------------
    MERGE INTO SILVER_SCH.DIM_WAREHOUSE AS TGT
    USING (
        SELECT
            UPPER(TRIM(warehouse_id))       AS warehouse_id,
            UPPER(TRIM(warehouse_location)) AS warehouse_location
        FROM SILVER_SCH.TEMP_STREAM_DATA
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(warehouse_id))
            ORDER BY order_date DESC
        ) = 1
    ) AS SRC
    ON TGT.warehouse_id = SRC.warehouse_id
    WHEN MATCHED THEN UPDATE SET
        TGT.warehouse_location = SRC.warehouse_location,
        TGT.is_active          = TRUE,
        TGT.updated_at         = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        warehouse_id,
        warehouse_location
    ) VALUES (
        SRC.warehouse_id,
        SRC.warehouse_location
    );

    -- --------------------------------------------------------
    -- PART 5 — MERGE DIM_SHIPMENT
    -- Business key : shipment_id
    -- Important    : Skip unshipped orders (shipment_id = UNKNOWN)
    -- Why          : Pending/Processing orders have no shipment
    --                yet. shipment_id = UNKNOWN from stage 1
    --                COALESCE. Inserting UNKNOWN as a dim row
    --                would make all unshipped orders share one
    --                dim_shipment row — wrong.
    --                In fact_orders we use -1 as shipment_sk
    --                for unshipped orders (see Part 6)
    -- --------------------------------------------------------
    MERGE INTO SILVER_SCH.DIM_SHIPMENT AS TGT
    USING (
        SELECT
            UPPER(TRIM(shipment_id)) AS shipment_id,
            UPPER(TRIM(carrier))     AS carrier,
            ship_date,
            estimated_delivery
        FROM SILVER_SCH.TEMP_STREAM_DATA
        WHERE UPPER(TRIM(shipment_id)) != 'UNKNOWN'
          AND shipment_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(shipment_id))
            ORDER BY order_date DESC
        ) = 1
    ) AS SRC
    ON TGT.shipment_id = SRC.shipment_id
    WHEN MATCHED THEN UPDATE SET
        TGT.carrier            = SRC.carrier,
        TGT.ship_date          = SRC.ship_date,
        TGT.estimated_delivery = SRC.estimated_delivery,
        TGT.is_active          = TRUE,
        TGT.updated_at         = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        shipment_id,
        carrier,
        ship_date,
        estimated_delivery
    ) VALUES (
        SRC.shipment_id,
        SRC.carrier,
        SRC.ship_date,
        SRC.estimated_delivery
    );

    -- --------------------------------------------------------
    -- PART 6 — MERGE FACT_ORDERS
    -- Business key : order_id
    -- This is the most complex merge because:
    --   It needs to JOIN all 5 dims to get their
    --   surrogate keys (SK) — the FKs for the fact table
    --   It uses INNER JOIN for customer, product, supplier,
    --   warehouse — every order MUST have these
    --   It uses LEFT JOIN for shipment — unshipped orders
    --   have no shipment row yet, shipment_sk defaults to -1
    -- SCD Type 1 — update mutable measures on match
    -- --------------------------------------------------------
    MERGE INTO SILVER_SCH.FACT_ORDERS AS TGT
    USING (
        SELECT
            -- Business key
            T.order_id,

            -- Surrogate keys from dimension lookups
            C.customer_sk,
            P.product_sk,
            S.supplier_sk,
            W.warehouse_sk,
            COALESCE(SH.shipment_sk, -1) AS shipment_sk,
            -- -1 means not yet shipped
            -- avoids NULL FK in fact table

            -- Order fields
            T.order_date,
            T.order_status,
            T.payment_status,

            -- Measures
            T.quantity,
            T.unit_price,
            T.total_amount,
            T.delay_days,
            T.inventory_level,

            -- Metadata
            T.ingested_at

        FROM SILVER_SCH.TEMP_STREAM_DATA    AS T

        -- INNER JOINs — these dims must exist for valid order
        INNER JOIN SILVER_SCH.DIM_CUSTOMERS AS C
            ON UPPER(TRIM(T.customer_id)) = C.customer_id
            AND C.is_active = TRUE

        INNER JOIN SILVER_SCH.DIM_PRODUCT   AS P
            ON UPPER(TRIM(T.product_id)) = P.product_id
            AND P.is_active = TRUE

        INNER JOIN SILVER_SCH.DIM_SUPPLIER  AS S
            ON UPPER(TRIM(T.supplier_id)) = S.supplier_id
            AND S.is_active = TRUE

        INNER JOIN SILVER_SCH.DIM_WAREHOUSE AS W
            ON UPPER(TRIM(T.warehouse_id)) = W.warehouse_id
            AND W.is_active = TRUE

        -- LEFT JOIN — not every order has shipment yet
        LEFT JOIN SILVER_SCH.DIM_SHIPMENT   AS SH
            ON UPPER(TRIM(T.shipment_id)) = SH.shipment_id
            AND UPPER(TRIM(T.shipment_id)) != 'UNKNOWN'

        -- Deduplicate — keep one row per order_id
        -- handles edge case of same order appearing twice
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY T.order_id
            ORDER BY T.order_date DESC
        ) = 1

    ) AS SRC
    ON TGT.order_id = SRC.order_id

    -- Order already exists — update mutable fields only
    -- Do NOT update order_sk (surrogate key — never changes)
    -- Do NOT update customer_sk, product_sk, supplier_sk
    -- (these are static facts about this order)
    WHEN MATCHED THEN UPDATE SET
        TGT.order_status    = SRC.order_status,
        TGT.payment_status  = SRC.payment_status,
        TGT.shipment_sk     = SRC.shipment_sk,
        TGT.delay_days      = SRC.delay_days,
        TGT.inventory_level = SRC.inventory_level,
        TGT.total_amount    = SRC.total_amount,
        TGT.updated_at      = CURRENT_TIMESTAMP()

    -- New order — insert full row
    -- order_sk auto-generated by sequence default
    WHEN NOT MATCHED THEN INSERT (
        order_id,
        customer_sk,
        product_sk,
        supplier_sk,
        warehouse_sk,
        shipment_sk,
        order_date,
        order_status,
        payment_status,
        quantity,
        unit_price,
        total_amount,
        delay_days,
        inventory_level,
        ingested_at
    ) VALUES (
        SRC.order_id,
        SRC.customer_sk,
        SRC.product_sk,
        SRC.supplier_sk,
        SRC.warehouse_sk,
        SRC.shipment_sk,
        SRC.order_date,
        SRC.order_status,
        SRC.payment_status,
        SRC.quantity,
        SRC.unit_price,
        SRC.total_amount,
        SRC.delay_days,
        SRC.inventory_level,
        SRC.ingested_at
    );

    -- --------------------------------------------------------
    -- PART 7 — CLEANUP
    -- Drop temp table — no longer needed
    -- Keeps session clean for next run
    -- --------------------------------------------------------
    DROP TABLE IF EXISTS SILVER_SCH.TEMP_STREAM_DATA;

    RETURN 'SP_SILVER_TO_STAR completed successfully';

END;



-- ============================================================
-- STEP 6 — TASK: SILVER_TO_STAR_TSK
-- Purpose  : Orchestrates SP_SILVER_TO_STAR every 1 minute
-- Schedule : 1 minute
-- When     : Only when stg_orders_stream has data
--            No data = no run = no cost
-- Depends  : bronze_to_silver_tsk must run first
--            (stg_orders feeds stg_orders_stream)
-- ============================================================

CREATE OR REPLACE TASK SILVER_SCH.SILVER_TO_STAR_TSK
    WAREHOUSE = FLOWBRIDGE_PIPLELINE_WH
    SCHEDULE  = '1 minute'
    COMMENT   = 'Task calls SP_SILVER_TO_STAR every minute when stg_orders_stream has data'
WHEN
    SYSTEM$STREAM_HAS_DATA('SILVER_SCH.STG_ORDERS_STREAM')
AS
    CALL SILVER_SCH.SP_SILVER_TO_STAR();

-- Resume task (created suspended by default)
ALTER TASK SILVER_SCH.SILVER_TO_STAR_TSK RESUME;

SHOW TASKS;

-- ============================================================
-- STEP 7 — VERIFICATION
-- Run these after calling the SP to confirm all counts
-- ============================================================

-- Test the stored procedure manually
CALL SILVER_SCH.SP_SILVER_TO_STAR();
show tasks ;

-- Check stream has data before calling
SELECT SYSTEM$STREAM_HAS_DATA('SILVER_SCH.STG_ORDERS_STREAM');

-- Verify all dim and fact counts
SELECT 'dim_customers'  AS table_name, COUNT(*) AS row_count FROM SILVER_SCH.DIM_CUSTOMERS
UNION ALL
SELECT 'dim_product',                  COUNT(*)              FROM SILVER_SCH.DIM_PRODUCT
UNION ALL
SELECT 'dim_supplier',                 COUNT(*)              FROM SILVER_SCH.DIM_SUPPLIER
UNION ALL
SELECT 'dim_warehouse',                COUNT(*)              FROM SILVER_SCH.DIM_WAREHOUSE
UNION ALL
SELECT 'dim_shipment',                 COUNT(*)              FROM SILVER_SCH.DIM_SHIPMENT
UNION ALL
SELECT 'fact_orders',                  COUNT(*)              FROM SILVER_SCH.FACT_ORDERS
UNION ALL
SELECT 'stg_orders (source)',          COUNT(*)              FROM SILVER_SCH.STG_ORDERS;

-- Expected counts (per 300 record historical load):
-- dim_customers  : 6   (6 unique customers)
-- dim_product    : 6   (6 unique products)
-- dim_supplier   : 5   (5 unique suppliers)
-- dim_warehouse  : 5   (5 unique warehouses)
-- dim_shipment   : varies (only shipped orders)
-- fact_orders    : ~263 (matches stg_orders count)
-- stg_orders     : ~263

-- Spot check dim data
SELECT * FROM SILVER_SCH.DIM_CUSTOMERS  ORDER BY customer_sk;
SELECT * FROM SILVER_SCH.DIM_PRODUCT    ORDER BY product_sk;
SELECT * FROM SILVER_SCH.DIM_SUPPLIER   ORDER BY supplier_sk;
SELECT * FROM SILVER_SCH.DIM_WAREHOUSE  ORDER BY warehouse_sk;
SELECT * FROM SILVER_SCH.DIM_SHIPMENT   ORDER BY shipment_sk LIMIT 10;

-- Spot check fact table with dim joins (mini star schema query)
-- This is what the gold layer will build on top of
SELECT
    F.order_id,
    F.order_date,
    F.order_status,
    C.customer_name,
    C.customer_region,
    P.product_name,
    P.category,
    S.supplier_name,
    W.warehouse_location,
    F.quantity,
    F.unit_price,
    F.total_amount,
    F.delay_days,
    F.payment_status
FROM SILVER_SCH.FACT_ORDERS     F
JOIN SILVER_SCH.DIM_CUSTOMERS   C  ON F.customer_sk  = C.customer_sk
JOIN SILVER_SCH.DIM_PRODUCT     P  ON F.product_sk   = P.product_sk
JOIN SILVER_SCH.DIM_SUPPLIER    S  ON F.supplier_sk  = S.supplier_sk
JOIN SILVER_SCH.DIM_WAREHOUSE   W  ON F.warehouse_sk = W.warehouse_sk
LEFT JOIN SILVER_SCH.DIM_SHIPMENT SH ON F.shipment_sk = SH.shipment_sk
LIMIT 20;

-- ============================================================
-- TWO BUGS FIXED FROM YOUR ORIGINAL CODE — NOTE THESE
-- ============================================================
-- BUG 1: face_orders → fact_orders (typo in table name)
--        Original: CREATE OR REPLACE TABLE SILVER_SCH.face_orders
--        Fixed   : CREATE OR REPLACE TABLE SILVER_SCH.FACT_ORDERS

-- BUG 2: sp_silver_to_star had syntax errors
--        Original: "creaet or replace silver_sch.sp_silver_to_star()"
--        Missing PROCEDURE keyword, typos in LANGUAGE/RETURNS
--        Fixed   : Full correct syntax

-- BUG 3: dim_customers merge had wrong column update
--        Original: tgt.customer_id = src.customer_name (wrong!)
--        Fixed   : tgt.customer_name = src.customer_name

-- BUG 4: dim_supplier had unit_price column (wrong table)
--        unit_price belongs to dim_product, not dim_supplier
--        Removed from dim_supplier definition

-- BUG 5: temp_stream_data WHERE clause syntax
--        Original: "when metadata$action = 'insert'"
--        (WHEN is not valid here — needs WHERE)
--        Fixed   : WHERE METADATA$ACTION = 'INSERT'
-- ============================================================