# Food Delivery Platform Data Pipeline 

Food Delivery Platform — End-to-End Data Engineering Pipeline on Snowflake

A full ELT pipeline that simulates how a food-delivery platform (Swiggy-style) would land raw operational data in Snowflake, clean and conform it, model it into a star schema with historical tracking, and serve it to a Streamlit revenue dashboard.

This project was built end-to-end in Snowflake — from raw CSV ingestion through a 3-layer warehouse to a BI-ready fact table and a live analytics app — as a hands-on exercise in change data capture, dimensional modeling, and metadata-driven lineage tracking.

Adapted and rebuilt from the walkthrough in "SWIGGY — End To End Data Engineering Project" by Data Engineering Simplified (video). All SQL and Streamlit code in this repo was written/typed out and run against a live Snowflake account while following and adapting that design.

Table of Contents
What this project demonstrates
Architecture
Repository structure
Environment setup
Pipeline walkthrough, entity by entity
Date dimension
Fact table — Order Item Fact
Consumption-layer KPI views
Streamlit revenue dashboard
Dashboard preview
How to run this end-to-end
Design notes, trade-offs, and known limitations
Possible extensions
What this project demonstrates
Layered warehouse design (raw → clean → consumption) instead of a single flat load — each layer has one job, which keeps debugging and re-processing cheap.
Change data capture with Snowflake Streams, so downstream layers only process what actually changed instead of re-scanning full tables.
SCD Type 2 dimensional modeling with SHA1-hash surrogate keys, so the warehouse can answer "what did this restaurant's data look like on date X" and not just "what does it look like now."
Metadata-driven lineage — every raw and clean table carries the source filename, file load timestamp, and an MD5 checksum, so any row can be traced back to the exact file it came from.
Column-level PII governance — sensitive columns (mobile, email, gender, DOB, restaurant phone) are tagged at the DDL level so they're identifiable and maskable without touching downstream queries.
A conformed star schema (one fact table, seven dimensions) that collapses nine operational entities into something a BI tool or analyst can query directly.
A working consumption layer: SQL views pre-aggregate revenue KPIs, and a Streamlit app (running natively inside Snowflake via Snowpark) turns those views into an interactive dashboard.
Architecture
High-level data flow
CSV files (initial + delta batches)
        │
        ▼
Snowflake internal stage (@stage_sch.csv_stg)
        │  COPY INTO ... (all columns as TEXT, plus audit columns)
        ▼
stage_sch.*          <- raw, as-is landing tables
        │  append-only STREAM (captures newly copied rows)
        ▼
clean_sch.*          <- typed, validated, deduplicated (current-state only)
        │  standard STREAM (captures INSERT / UPDATE / DELETE)
        ▼
consumption_sch.*_dim   <- SCD2 dimensions, hash surrogate keys
        │
        ▼
consumption_sch.order_item_fact   <- grain: one row per order line item
        │
        ▼
consumption_sch.vw_*_kpis   <- pre-aggregated SQL views
        │
        ▼
Streamlit app (Snowpark, runs inside Snowflake)  <- revenue dashboard
The three schemas (and common)
Schema	Role	Column typing	History
stage_sch	Raw landing zone, mirrors the source CSVs 1:1	Everything is TEXT	None — append-only
clean_sch	Typed, validated, "current state" entities	Proper types (NUMBER, TIMESTAMP_TZ, etc.) via TRY_CAST / TRY_TO_*	None (upsert only, no SCD2)
consumption_sch	Star schema for BI: dimensions + fact + views	Fully typed, hash surrogate keys	SCD2 on dimensions
common	Shared governance objects	—	—

Why three layers instead of loading straight into a star schema?

Raw data should never fail to load. Every stage_sch column is TEXT. If a CSV has a malformed date or an unexpected value, the COPY INTO still succeeds — the failure (if any) happens later, deliberately, in the clean layer using TRY_CAST / TRY_TO_TIMESTAMP / TRY_TO_DECIMAL, which return NULL instead of aborting a batch load over one bad row.
Replayability. Because stage_sch is an untouched copy of the source file, the entire clean/consumption pipeline can be rebuilt from scratch at any time without re-pulling source data.
Debuggability. If a number in a dashboard looks wrong, you can walk it backwards: KPI view → fact table → dimension → clean table → stage table → source CSV, at each step comparing "what came in" vs. "what came out."
Governance and history belong at different layers. PII tags are applied as early as stage_sch (so sensitive columns are flagged from the moment they enter the warehouse). SCD2 history, on the other hand, is deliberately not built in clean_sch — it's only built once in consumption_sch, where it's actually consumed. Maintaining two copies of history (once in clean, once in consumption) would be redundant and a common source of drift.
Change data capture (Streams)

Two kinds of streams are used, and the choice of each is deliberate:

append_only = true streams on every stage_sch table. Staging tables are never updated or deleted from — new data always arrives as new rows from a new file — so an append-only stream is the cheapest and most accurate way to say "give me only what was copied in since I last consumed this."
Standard streams on clean_sch tables. These need to see INSERT, UPDATE, and (potentially) DELETE, because that's what drives the SCD2 logic in consumption_sch — a standard stream exposes METADATA$ACTION and METADATA$ISUPDATE, which the dimension MERGE statements branch on to decide whether to insert a brand-new SCD2 row or close out an old one.
Surrogate keys: HASH(SHA1_HEX(...)) instead of sequences

Every dimension and the fact table use a deterministic hash of business attributes as the surrogate key (e.g. RESTAURANT_HK, CUSTOMER_HK) rather than an AUTOINCREMENT sequence. This is a deliberate choice:

It lets the MERGE statement compute the key for a new SCD2 row inline, without a separate lookup or sequence call.
Because the hash is derived from the tracked attributes themselves, two rows with identical attributes always produce the same key — which is exactly the property SCD2 matching needs (ON target.key = source.key).
It keeps dimension population fully declarative — one MERGE, no round-trips.
Metadata / lineage columns

Every stage_sch and clean_sch table carries the same four audit columns:

Column	Purpose
_stg_file_name	Which source file this row came from (metadata$filename)
_stg_file_load_ts	When that file was last modified (metadata$file_last_modified)
_stg_file_md5	Content checksum of the file (metadata$file_content_key) — enables dedup/reprocessing detection
_copy_data_ts	When the row was actually copied into Snowflake

This is what makes the pipeline "metadata-driven": every row is traceable to an exact file and load event, without needing a separate orchestration/logging system.

PII governance

A shared tag (common.pii_policy_tag, allowed values PII, PRICE, SENSITIVE, EMAIL) is applied directly on column definitions — mobile, email, gender, dob on customer; restaurant_phone on restaurant. Three masking policies (pii_masking_policy, email_masking_policy, phone_masking_policy) are also defined in common, ready to be attached to those tagged columns via ALTER TABLE ... MODIFY COLUMN ... SET MASKING POLICY. Tagging at the DDL level means sensitive columns are discoverable (via INFORMATION_SCHEMA / tag-based queries) even before a masking policy is actively attached.

Repository structure
.
├── <your .sql file>       # every DDL/DML script in this README, in execution order
├── streamlit.py            # the revenue dashboard (Snowpark-native Streamlit app)
└── README.md                # this file

Update the filenames above to match what's actually in your repo.

Environment setup

Before any entity-specific script runs, the pipeline sets up the shared objects every later script depends on:

sql
use role sysadmin;

create warehouse if not exists adhoc_wh
     warehouse_size = 'x-small'
     auto_resume = true
     auto_suspend = 60
     initially_suspended = true;

create database if not exists sandbox;
create schema if not exists stage_sch;
create schema if not exists clean_sch;
create schema if not exists consumption_sch;
create schema if not exists common;

create file format if not exists stage_sch.csv_file_format
    type = 'csv'
    field_delimiter = ','
    skip_header = 1
    field_optionally_enclosed_by = '\042'
    null_if = ('\\N');

create stage stage_sch.csv_stg
    directory = ( enable = true );

create or replace tag common.pii_policy_tag
    allowed_values 'PII','PRICE','SENSITIVE','EMAIL';
x-small, auto-suspend 60s, initially suspended — this is a throwaway/dev warehouse for an adhoc/learning workload, so it's sized to cost almost nothing when idle.
skip_header = 1 + field_optionally_enclosed_by — matches a standard CSV export with a header row and optionally quoted fields.
Directory-enabled internal stage — lets you list @stage_sch.csv_stg/... and browse/verify files before running COPY INTO, which is useful when initial and delta files land in different sub-paths (/initial/..., /delta/..., /daily/...).
Pipeline walkthrough, entity by entity

Each entity below follows the same three-step shape: stage table → clean table → consumption dimension. Rather than repeat the full pattern nine times, this section calls out what's specific to each entity.

1. Location → restaurant_location_dim
Clean layer enriches the raw location with business rules that don't exist in the source data:
Delhi → normalized to New Delhi
A hardcoded state → state-code lookup (Maharashtra → MH, etc.)
is_union_territory derived from a fixed list of states
capital_city_flag derived from state+city combinations
city_tier (Tier-1 / Tier-2 / Tier-3) derived from a hardcoded city list
These are classic "enrichment belongs in the clean layer, not the source" transformations — the source system has no concept of city tiers, but analysts need it.
Consumption dimension is SCD2, but its MERGE ... ON clause matches on LOCATION_ID + ACTIVE_FLAG rather than the full attribute set the way most other dimensions do. In practice this means a location is only versioned when its active/inactive status changes, not on every attribute change — worth being aware of if you extend this table (see Design notes).
2. Restaurant → restaurant_dim
restaurant_phone is tagged SENSITIVE at both the stage and clean layer.
Clean layer load is split into an initial bulk INSERT (first load, no stream involved) followed by a MERGE driven off stage_sch.restaurant_stm for every subsequent (delta) load — a pattern that shows up because the very first load has nothing to "merge into" yet.
Consumption dimension is full SCD2, matched on the complete attribute set — any change to name, cuisine, pricing, hours, address, or geo-coordinates versions the row.
3. Customer → customer_dim
mobile, gender, dob tagged PII; email tagged EMAIL — separately from SENSITIVE, since email has different regulatory handling than a name/DOB.
dob / anniversary are converted from text to DATE with TRY_TO_DATE.
Full SCD2 on the consumption dimension, matched on every business attribute.
4. Customer Address → customer_address_dim
One customer can have several addresses (primary_flag, address_type distinguish them), so the natural key here is address_id, not customer_id.
Same SCD2 pattern as customer, keyed on the full address attribute set.
5. Menu → menu_dim
price converted TEXT → DECIMAL(10,2), availability converted from a text 'true'/'false' to a real BOOLEAN, string fields TRIM'd — this is the layer that absorbs "source system stored booleans as strings" so nothing downstream has to special-case it.
SCD2 on price/description/category/availability/type changes — this is what lets a KPI view say "how much did we sell at the price in effect at the time of the order" rather than today's price.
6. Delivery Agent → delivery_agent_dim
rating converted to NUMBER(4,2).
SCD2 on name/phone/vehicle/location/status/gender/rating.
7. Delivery (transactional — no SCD2)
Deliberately not modeled as an SCD2 dimension. A delivery is an event tied 1:1 to an order, not a slowly-changing attribute of some other entity — its clean_sch.delivery table is a straight current-state MERGE (matched on delivery_id + order_id + delivery_agent_id), and it feeds the fact table directly for delivery_status / estimated_time.
8. Orders (transactional — no SCD2)
Same reasoning as Delivery: an order is an event, not a dimension. clean_sch.orders is a current-state MERGE keyed on order_id.
Note the MATCHED branch only updates total_amount, status, payment_method, and modified_dt — the FK columns (customer_id_fk, restaurant_id_fk) are intentionally left alone on update, since an existing order shouldn't be re-attributed to a different customer or restaurant.
9. Order Item (transactional — feeds the fact table directly)
Grain here is one row per line item within an order (order_item_id).
quantity, price, subtotal converted to numeric types — these three become the fact table's core measures.
Date dimension

Rather than hardcode a date range or load an external calendar file, consumption_sch.date_dim is generated with a recursive CTE, anchored to the actual data:

sql
with recursive my_date_dim_cte as (
    select current_date() as today, ...
    union all
    select dateadd('day', -1, today) as today_r, ...
    from my_date_dim_cte
    where today_r > (select date(min(order_date)) from clean_sch.orders)
)
select hash(SHA1_hex(today)) as DATE_DIM_HK, today, year, quarter, month, week,
       day_of_year, day_of_week, day_of_the_month, day_name
from my_date_dim_cte;

Why this approach: it walks backward one day at a time from today until it passes the earliest order date already in the warehouse, so the date dimension is exactly as long as it needs to be — no manually chosen start/end year, and no risk of the calendar table going stale relative to the actual order history it needs to cover.

Fact table — order_item_fact

Grain: one row per order item (i.e., per line item within an order — not per order).

Column	Role
order_item_id, order_id	Natural/degenerate keys from the source
customer_dim_key, customer_address_dim_key, restaurant_dim_key, restaurant_location_dim_key, menu_dim_key, delivery_agent_dim_key, order_date_dim_key	FKs to the seven SCD2/date dimensions
quantity, price, subtotal	Measures
delivery_status, estimated_time	Carried through from the Delivery entity — kept as fact attributes rather than a separate "delivery" dimension, since they're transactional and don't need history

The MERGE that populates it joins all nine clean-layer streams/tables together in one statement, resolving every dimension's current-effective surrogate key at the same time — so the fact table is refreshed incrementally, driven off changes captured in clean_sch.order_item_stm, rather than a full rebuild every run.

Foreign key constraints are added afterward with ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES ... — worth noting that in Snowflake these are informational only (not enforced at write time), but they're valuable anyway: they document the star-schema relationships explicitly and let BI tools (and query optimizers, via join elimination) understand the model.

Consumption-layer KPI views

All views live in consumption_sch and are built directly on top of order_item_fact joined to date_dim (and, for one view, restaurant_dim):

View	Grain	Metrics
vw_yearly_revenue_kpis	Year	total_revenue, total_orders, avg_revenue_per_order, avg_revenue_per_item, max_order_value
vw_monthly_revenue_kpis	Year + Month	Same metrics, monthly
vw_daily_revenue_kpis	Year + Month + Day-of-month	Same metrics, daily
vw_day_revenue_kpis	Year + Month + Day-of-week name	Same metrics, by weekday (e.g. "are Fridays bigger than Mondays?")
vw_monthly_revenue_by_restaurant	Year + Month + Restaurant + Delivery status	Same metrics, per restaurant

Most of these filter to DELIVERY_STATUS = 'Delivered', so KPIs reflect realized revenue, not cancelled/pending orders. This is the single most important business rule in the whole consumption layer — everything the dashboard shows is gated on it.

Streamlit revenue dashboard

streamlit.py runs as a Snowpark-native Streamlit app inside Snowflake — it calls get_active_session() rather than opening its own connector/credentials, meaning it runs with the privileges of whoever's logged into Snowflake and needs no separate auth setup.

Layout, top to bottom:

All-time KPI cards — total revenue, total orders, and max order value across every year, using vw_yearly_revenue_kpis.
Year selector, defaulting to the most recent year, driving a second row of KPI cards for that year — each one shows a delta against the previous year (via st.metric(..., delta=...)), so the dashboard communicates trend, not just a snapshot.
Monthly revenue trend for the selected year, shown as both a bar chart and a line chart (Altair). Months are explicitly mapped to abbreviated names and cast to a pd.Categorical with a fixed calendar order — otherwise Streamlit/pandas would sort them alphabetically (Apr, Aug, Dec, Feb, ...) instead of chronologically.
Month selector, populated dynamically from whatever months actually have data for the selected year (via vw_monthly_revenue_by_restaurant) rather than a hardcoded 1–12 list.
Top 10 restaurants table for the selected year/month, ranked by revenue, with alternating row-color styling for readability.
Dashboard preview
<!-- Replace the placeholders below with real screenshots once the app is deployed. Suggested shots: the all-time + yearly KPI cards, the monthly trend chart, and the top-10-restaurants table. Store images in a /screenshots folder in the repo root and update the paths accordingly. -->

All-time & yearly KPI cards

<img width="1480" height="767" alt="image" src="https://github.com/user-attachments/assets/a880e727-f052-4ee0-bc25-4ac51a1f9ed5" />


Monthly revenue trend

<img width="1331" height="610" alt="image" src="https://github.com/user-attachments/assets/0acd24d2-6f28-4507-a104-04e870c92b03" />


Top 10 restaurants for a selected month

<img width="1481" height="767" alt="image" src="https://github.com/user-attachments/assets/c74adf91-f0c3-45b2-baee-6097961eefa0" />


How to run this end-to-end
Set up the environment — run the warehouse/database/schema/stage/tag/masking-policy block at the top of the SQL script.
Upload the sample CSVs to the internal stage using Snowsight's data-loading UI (or PUT), matching the folder structure the scripts expect (/initial/..., /delta/..., /daily/... per entity).
Run the SQL script top to bottom, entity by entity — each entity's block creates its stage table, its stream, runs the initial COPY INTO, builds the clean table + stream + merge, then builds the consumption dimension + merge. Order matters where dimensions depend on each other's clean data (e.g., Restaurant's clean load references location_id_fk).
Load delta files for each entity (the copy into ... from @stage_sch.csv_stg/delta/... blocks) to see the CDC/SCD2 logic actually version rows.
Build the Date Dimension, Order Item Fact, and KPI views, in that order — the fact table depends on every dimension already being populated, and the views depend on the fact table.
Deploy streamlit.py as a Streamlit-in-Snowflake app pointed at the sandbox database, and open it to see the dashboard.
