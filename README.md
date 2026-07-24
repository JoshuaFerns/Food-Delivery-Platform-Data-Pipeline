# Swiggy — End-to-End Data Engineering Pipeline on Snowflake

An ELT pipeline that simulates how a food-delivery platform would land raw operational data in Snowflake, clean it, model it into a star schema with historical tracking, and serve it to a Streamlit revenue dashboard.

Built to practice **change data capture, dimensional modeling, and metadata-driven lineage tracking** in Snowflake — from raw CSV ingestion to a live analytics app.

> Adapted from ["SWIGGY — End To End Data Engineering Project"](https://data-engineering-simplified.medium.com/swiggy-end-to-end-data-engineering-project-3f1af55005bf) by Data Engineering Simplified ([video](https://youtu.be/IktHL33Wgug)). All SQL and Streamlit code was written/run against a live Snowflake account while following and adapting that design.

---

## Architecture

```
CSV files (initial + delta)
        │
        ▼
Internal stage (@stage_sch.csv_stg)  →  COPY INTO (all TEXT + audit columns)
        │
        ▼
stage_sch.*     raw, as-is landing tables
        │  append-only STREAM
        ▼
clean_sch.*     typed, validated, current-state only
        │  standard STREAM (insert/update/delete)
        ▼
consumption_sch.*_dim     SCD2 dimensions, hash surrogate keys
        │
        ▼
consumption_sch.order_item_fact     grain: one row per order line item
        │
        ▼
consumption_sch.vw_*_kpis     pre-aggregated views
        │
        ▼
Streamlit app (Snowpark, runs inside Snowflake)
```

| Schema | Role | Typing | History |
|---|---|---|---|
| `stage_sch` | Raw landing zone, mirrors source CSVs | Everything `TEXT` | None — append-only |
| `clean_sch` | Typed, current-state entities | Proper types via `TRY_CAST`/`TRY_TO_*` | None (upsert only) |
| `consumption_sch` | Star schema for BI | Fully typed, hash surrogate keys | SCD2 on dimensions |
| `common` | Shared governance (PII tags, masking policies) | — | — |

**Key design decisions:**
- **Stage columns are all `TEXT`** so a bad row never fails the whole load — casting happens deliberately in `clean_sch` with `TRY_CAST`/`TRY_TO_*`, which return `NULL` instead of aborting.
- **Streams, not full reloads.** Append-only streams on `stage_sch` (source rows are never updated, only appended) and standard streams on `clean_sch` (need insert/update/delete to drive SCD2).
- **Hash surrogate keys** (`HASH(SHA1_HEX(...))`) instead of sequences — deterministic, so a `MERGE` can compute a new dimension key inline without a lookup, and identical attributes always produce the same key (what SCD2 matching needs).
- **SCD2 lives only in `consumption_sch`**, not `clean_sch` — maintaining history in two places is redundant and drifts.
- **Audit columns** (`_stg_file_name`, `_stg_file_load_ts`, `_stg_file_md5`, `_copy_data_ts`) on every stage/clean table trace any row back to its source file — this is what makes the pipeline "metadata-driven."
- **PII tagging** (`common.pii_policy_tag`: `PII`/`PRICE`/`SENSITIVE`/`EMAIL`) applied at the DDL level on sensitive columns (mobile, email, gender, DOB, restaurant phone). Masking policies exist in `common` but aren't yet attached to columns — see [Known limitations](#known-limitations).

---

## Entity pipeline

Every entity follows the same shape: stage table → clean table → consumption dimension. Transactional entities (Delivery, Orders, Order Item) skip SCD2 since they're events, not slowly-changing attributes.

| Entity | Clean-layer specifics | Consumption dim | SCD2? |
|---|---|---|---|
| Location | Delhi→New Delhi normalization, state→code lookup, `is_union_territory`, `capital_city_flag`, `city_tier` derived | `restaurant_location_dim` | Yes — but matched on `LOCATION_ID + ACTIVE_FLAG`, narrower than the others |
| Restaurant | Phone tagged `SENSITIVE`; initial bulk insert then delta `MERGE` | `restaurant_dim` | Yes, full attribute match |
| Customer | Mobile/gender/DOB tagged `PII`, email tagged `EMAIL` separately; DOB cast to `DATE` | `customer_dim` | Yes, full attribute match |
| Customer Address | Natural key is `address_id`, not `customer_id` (one customer, many addresses) | `customer_address_dim` | Yes, full attribute match |
| Menu | Price cast to `DECIMAL`, availability `'true'/'false'` text → real `BOOLEAN` | `menu_dim` | Yes — lets fact rows reflect the price *at order time* |
| Delivery Agent | Rating cast to `NUMBER(4,2)` | `delivery_agent_dim` | Yes, full attribute match |
| Delivery | 1:1 with an order — an event, not a dimension | feeds fact directly | No |
| Orders | `MATCHED` branch updates status/amount/payment only — FK columns never re-attributed on update | feeds fact directly | No |
| Order Item | Grain = one row per line item; quantity/price/subtotal become fact measures | feeds fact directly | No |

---

## Date dimension

`consumption_sch.date_dim` is built with a **recursive CTE** that walks backward one day at a time from today until it passes `MIN(order_date)` in `clean_sch.orders`. No hardcoded start year, no external calendar file — the dimension is always exactly as long as the order history needs.

---

## Fact table — `order_item_fact`

**Grain: one row per order item.**

- **Measures:** `quantity`, `price`, `subtotal`
- **FKs:** customer, customer_address, restaurant, restaurant_location, menu, delivery_agent, date (7 dimensions)
- **Carried through:** `delivery_status`, `estimated_time` — kept as fact attributes, not a separate dimension, since they're transactional
- Populated by one `MERGE` joining all nine clean-layer entities together, driven off the `order_item` stream — incremental, not a full rebuild each run
- FK constraints are added via `ALTER TABLE` after creation — informational only in Snowflake (not enforced), but document the model for BI tools and query optimizers

---

## KPI views (`consumption_sch`)

| View | Grain | Metrics |
|---|---|---|
| `vw_yearly_revenue_kpis` | Year | revenue, orders, avg/order, avg/item, max order |
| `vw_monthly_revenue_kpis` | Year + Month | same |
| `vw_daily_revenue_kpis` | Year + Month + Day | same |
| `vw_day_revenue_kpis` | Year + Month + Weekday name | same |
| `vw_monthly_revenue_by_restaurant` | Year + Month + Restaurant + Status | same |

Most views filter to `DELIVERY_STATUS = 'Delivered'` so KPIs reflect realized revenue, not cancelled/pending orders — the single most important business rule in the consumption layer.

---

## Streamlit dashboard

Runs **natively inside Snowflake** via `get_active_session()` — no separate credentials needed.

- All-time KPI cards (total revenue, orders, max order value)
- Year selector with YoY delta on each KPI
- Monthly revenue trend — bar + line chart (Altair), months forced into calendar order via `pd.Categorical`
- Month selector populated dynamically from actual data
- Top 10 restaurants table for the selected month, alternating row colors

### Dashboard preview

<!-- Add real screenshots to /screenshots and update paths -->
![KPI cards](screenshots/kpi-cards.png)
![Monthly trend](screenshots/monthly-trend.png)
![Top restaurants](screenshots/top-restaurants.png)

---

## How to run

1. Run the environment setup block (warehouse, database, schemas, stage, file format, tags).
2. Upload sample CSVs to the internal stage (`/initial/...`, `/delta/...` per entity).
3. Run the SQL script top to bottom, entity by entity.
4. Load delta files to see CDC/SCD2 versioning in action.
5. Build Date Dimension → Order Item Fact → KPI views, in that order.
6. Deploy `streamlit.py` as a Streamlit-in-Snowflake app.

---

## Known limitations

- Masking policies exist in `common` but aren't attached to any column yet — tagging alone doesn't mask data.
- `vw_day_revenue_kpis` is missing the `DELIVERY_STATUS = 'Delivered'` filter the other KPI views have.
- `restaurant_location_dim`'s SCD2 match key is narrower than the other dimensions'.
- FK constraints on `order_item_fact` are informational only — Snowflake doesn't enforce them.

## Possible extensions

- Orchestrate the pipeline with Snowflake Tasks instead of running scripts manually.
- Attach the existing masking policies to tagged PII columns.
- Add data-quality checks (row-count reconciliation, null-rate thresholds).
- Cluster `order_item_fact` on `order_date_dim_key` as volume grows.
- Add delivery-performance KPIs (on-time rate by city tier) to the dashboard.

---

## Repository structure

```
├── pipeline.sql       # every DDL/DML script, in execution order
├── streamlit.py        # the revenue dashboard
└── README.md
```
