# Performance Analysis Report - Aptos Models

## Executive Summary

This report identifies **72+ instances** of performance anti-patterns across the dbt project. The most critical issues involve:

1. **Missing incremental predicates** on 72 of 86 incremental models (84%)
2. **N+1 query patterns** with 58 `run_query()` calls across 32 files
3. **Non-sargable JOIN conditions** using `LOWER()` function calls
4. **Cartesian product** in observability model
5. **Repeated subquery execution** for `MAX(_inserted_timestamp)`

---

## Critical Issues

### 1. Missing Incremental Predicates (HIGH SEVERITY)

**Impact**: Full table scans during incremental runs instead of partition pruning

**Statistics**:
- Total incremental models: **86**
- Models with `incremental_predicates`: **14** (16%)
- Models missing predicates: **72** (84%)

**Models with proper configuration** (these serve as templates):
```
models/silver/core/silver__transactions.sql
models/silver/core/silver__events.sql
models/silver/core/silver__blocks.sql
models/silver/core/silver__changes.sql
models/gold/defi/lending/*.sql (5 models)
models/gold/defi/defi__fact_bridge_activity.sql
models/gold/defi/defi__fact_dex_swaps.sql
models/gold/defi/defi__ez_dex_swaps.sql
models/gold/defi/defi__ez_bridge_activity.sql
```

**Example of proper configuration** (from `silver__transactions.sql:5`):
```sql
incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"]
```

**Missing in these high-volume models**:
| File | Lines |
|------|-------|
| `silver/nft/sales/silver__nft_sales_combined.sql` | 1-9 |
| `silver/nft/sales/silver__nft_sales_mercato.sql` | config |
| `silver/nft/sales/silver__nft_sales_bluemove*.sql` | config |
| `silver/nft/mints/silver__nft_mints_*.sql` | config |
| `silver/defi/dex/silver__dex_swaps_*.sql` (12 models) | config |
| `silver/defi/bridge/silver__bridge_*.sql` (4 models) | config |
| `silver/defi/lending/*/*.sql` (15 models) | config |

**Recommendation**: Add `incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"]` to all incremental models that filter on `block_timestamp`.

---

### 2. N+1 Query Patterns via `run_query()` (HIGH SEVERITY)

**Impact**: Multiple sequential database round-trips during model compilation, causing slower builds and higher compute costs.

**Statistics**: 58 `run_query()` calls across 32 files

**Worst offenders**:

| File | Count | Issue |
|------|-------|-------|
| `silver__transactions.sql` | 4 | Creates 2 temp tables + 2 metadata queries |
| `silver__transfers_fungible.sql` | 4 | Sequential owner data queries |
| `macros/tags/snowflake_tagging.sql` | 4 | Loop with individual tag checks |
| `silver__dex_swaps_*.sql` (12 files) | 2 each | Duplicate min_block_date queries |

**Example - `silver__transactions.sql:13-86`**:
```sql
{% set max_ins = run_query(max_inserted_query) [0] [0] %}  -- Query 1
{% do run_query(query_blocks) %}                            -- Query 2 (temp table)
{% do run_query(query_tx_batch) %}                          -- Query 3 (temp table)
{% set tx_batch_dates_result = run_query(tx_batch_dates_query) %}  -- Query 4
```

**Recommendation**:
- Consolidate multiple `run_query()` calls into CTEs within the main query
- Cache repeated metadata queries in Jinja variables at the start
- Use temporary tables sparingly; prefer CTEs for intermediate results

---

### 3. Non-Sargable JOIN Conditions (MEDIUM-HIGH SEVERITY)

**Impact**: `LOWER()` function calls on JOIN columns prevent index usage, causing full scans

**Affected files**:

| File | Line | Pattern |
|------|------|---------|
| `silver__hourly_prices_priority.sql` | 42-57 | 3 JOINs with `LOWER()` on both sides |
| `silver__hourly_prices_all_providers.sql` | 48-52 | `LOWER()` in JOIN |
| `core__ez_transfers.sql` | 48-52, 55-58 | `LOWER()` in 2 JOINs |
| `defi__ez_lending_borrows.sql` | 146 | `LOWER()` in price JOIN |
| `defi__ez_lending_repayments.sql` | 150 | `LOWER()` in price JOIN |
| `defi__ez_lending_liquidations.sql` | 159, 165 | `LOWER()` in 2 price JOINs |
| `defi__ez_lending_withdraws.sql` | 146 | `LOWER()` in price JOIN |

**Example - `silver__hourly_prices_priority.sql:40-57`**:
```sql
LEFT JOIN {{ ref('bronze__manual_token_price_metadata') }} b
ON LOWER(p.token_address) = LOWER(b.token_address_raw)  -- Non-sargable
LEFT JOIN {{ ref('silver__asset_metadata_priority') }} m
ON LOWER(p.token_address) = LOWER(m.token_address)      -- Non-sargable
LEFT JOIN {{ ref('silver__coin_info') }} C
ON LOWER(C.coin_type) = LOWER(COALESCE(b.token_address, p.token_address))  -- Non-sargable
```

**Recommendation**:
- Pre-compute `LOWER()` in source models and store as indexed column
- Add `_lower` suffixed columns during data ingestion
- Example: Add `token_address_lower` column to dimension tables

---

### 4. Cartesian Product (HIGH SEVERITY)

**File**: `silver_observability__transactions_completeness.sql:137`

```sql
FROM summary_stats
JOIN impacted_blocks ON 1 = 1  -- CARTESIAN PRODUCT!
```

**Impact**: Creates `rows(summary_stats) × rows(impacted_blocks)` result set. While summary_stats typically has 1 row, this pattern is dangerous and unclear.

**Recommendation**: Use `CROSS JOIN` explicitly or restructure to avoid:
```sql
FROM summary_stats
CROSS JOIN impacted_blocks
-- Or better: SELECT subqueries in column list
```

---

### 5. Repeated MAX(_inserted_timestamp) Subqueries (MEDIUM SEVERITY)

**Impact**: Same expensive aggregation query executed multiple times per model

**Affected files**:

| File | Occurrences |
|------|-------------|
| `silver__nft_sales_combined.sql` | 4 (lines 23, 44, 62, 83) |
| `silver__nft_mints_combined.sql` | 4 (lines 23, 44, 59, 78) |
| `silver__nft_mints_v2.sql` | 2+ |

**Example - `silver__nft_sales_combined.sql`**:
```sql
-- CTE 1 (line 42-47)
WHERE _inserted_timestamp >= (SELECT MAX(_inserted_timestamp) FROM {{ this }})

-- CTE 2 (line 60-65) - SAME QUERY!
AND _inserted_timestamp >= (SELECT MAX(_inserted_timestamp) FROM {{ this }})

-- CTE 3 (line 81-86) - SAME QUERY AGAIN!
AND _inserted_timestamp >= (SELECT MAX(_inserted_timestamp) FROM {{ this }})
```

**Recommendation**: Compute once at the top and reuse:
```sql
{% if is_incremental() %}
{% set max_ts_query %}
SELECT MAX(_inserted_timestamp) FROM {{ this }}
{% endset %}
{% set max_ts = run_query(max_ts_query)[0][0] %}
{% endif %}

-- Then use '{{ max_ts }}' in all CTEs
```

---

### 6. SELECT * Patterns (MEDIUM SEVERITY)

**Impact**: Retrieves unnecessary columns, increases I/O and memory usage

**Affected files**:

| File | Lines |
|------|-------|
| `silver__transactions.sql` | 183, 193, 207 |
| `silver__fungiblestore_owners.sql` | 63, 65 |
| `silver__fungiblestore_metadata.sql` | 55, 57 |
| `silver__nft_sales_combined.sql` | 36, 76, 128 |
| `silver__nft_sales_mercato.sql` | 75, 78-80, 92-96 |

**Example - `silver__nft_sales_combined.sql:34-37`**:
```sql
WITH all_nft_platform_sales AS (
    SELECT *  -- Retrieves ALL columns from view
    FROM {{ ref('silver__nft_sales_combined_view') }}
```

**Recommendation**: Explicitly list required columns to reduce data transfer.

---

### 7. Expensive Functions in JOIN Conditions (MEDIUM SEVERITY)

**File**: `core__ez_transfers.sql:60-63`

```sql
LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
ON LOWER(A.token_address) = LOWER(p.token_address)
AND DATE_TRUNC('hour', A.block_timestamp) = p.hour  -- Expensive!
```

**Impact**: `DATE_TRUNC()` computed for every row in left table before join comparison

**Recommendation**:
- Pre-compute the truncated hour in the source CTE
- Store `block_hour` as a physical column in `core__fact_transfers`

---

### 8. Views as Sources for Incremental Models (MEDIUM SEVERITY)

**Issue**: Incremental models querying non-incremental views defeat the incremental optimization

**Example - `silver__nft_sales_combined.sql`**:
- This is an incremental model (line 2)
- But it queries `silver__nft_sales_combined_view` (line 38)
- The view unions 8 marketplace sources without incremental filtering
- Every incremental run still computes the full view

**Affected pattern**:
```
silver__nft_sales_combined (incremental)
    → silver__nft_sales_combined_view (view)
        → 8 individual marketplace models
```

**Recommendation**: Either:
1. Make the combined view incremental
2. Push incremental predicates to the individual marketplace models
3. Materialize the view as a table with incremental refresh

---

## Materialization Strategy Issues

### Views That Should Be Tables

Several complex views with multiple JOINs and UNIONs are queried by downstream incremental models:

| View | Downstream Models | Recommendation |
|------|-------------------|----------------|
| `silver__nft_sales_combined_view` | silver__nft_sales_combined | Materialize as incremental table |
| `silver__dex_swaps_combined` | defi__fact_dex_swaps | Consider materialization |
| `silver__bridge_combined` | defi__fact_bridge_activity | Consider materialization |
| `silver__transfers_vw` | Multiple NFT models | Review usage pattern |

---

## Summary Statistics

| Category | Count | Severity |
|----------|-------|----------|
| Missing incremental_predicates | 72 models | HIGH |
| Excessive run_query() calls | 58 instances | HIGH |
| Non-sargable JOINs (LOWER()) | 10+ instances | MEDIUM-HIGH |
| Cartesian products | 1 confirmed | HIGH |
| Repeated MAX() subqueries | 10+ instances | MEDIUM |
| SELECT * usage | 15+ instances | MEDIUM |
| Expensive functions in JOINs | 5+ instances | MEDIUM |

---

## Prioritized Recommendations

### Immediate (High Impact, Low Effort)
1. Add `incremental_predicates` to all silver DEX, NFT, bridge, and lending models
2. Fix the Cartesian product in transactions_completeness.sql
3. Cache `MAX(_inserted_timestamp)` as Jinja variable in combined models

### Short-term (High Impact, Medium Effort)
4. Add `token_address_lower` columns to price/token dimension tables
5. Consolidate `run_query()` calls in silver__transactions.sql
6. Replace SELECT * with explicit column lists in high-volume models

### Medium-term (Medium Impact, Higher Effort)
7. Materialize combined views that serve incremental models
8. Pre-compute `block_hour` in source tables to avoid DATE_TRUNC in JOINs
9. Refactor snowflake_tagging macro to batch operations

---

## Files Requiring Immediate Attention

1. ~~`models/silver/core/silver__transactions.sql` - 4 run_query() calls~~ **FIXED**
2. ~~`models/silver/nft/sales/silver__nft_sales_combined.sql` - repeated subqueries~~ **FIXED**
3. ~~`models/silver/_observability/silver_observability__transactions_completeness.sql` - Cartesian product~~ **FIXED**
4. ~~`models/silver/price/silver__hourly_prices_priority.sql` - 3 non-sargable JOINs~~ **FIXED**
5. ~~`models/gold/core/core__ez_transfers.sql` - LOWER() + DATE_TRUNC in JOINs~~ **FIXED**

---

## Applied Fixes

### Fix 1: silver__transactions.sql - Reduced run_query() calls from 4 to 1

**Problem**: 4 sequential `run_query()` calls causing multiple database round-trips during compilation.

**Solution**: Converted temp tables to CTEs, replaced Jinja for-loop with SQL subquery.

**Benefits**: 75% reduction in compilation-time database round-trips.

---

### Fix 2: silver__nft_sales_combined.sql - Cached MAX(_inserted_timestamp)

**Problem**: Same `MAX(_inserted_timestamp)` subquery executed 3 times in different CTEs.

**Solution**:
- Added `incremental_predicates` for partition pruning
- Cached `max_its` as Jinja variable, reused across all CTEs

**Benefits**: Eliminates redundant subqueries during incremental runs.

---

### Fix 3: silver_observability__transactions_completeness.sql - Fixed Cartesian Product

**Problem**: Implicit Cartesian join using `JOIN impacted_blocks ON 1 = 1`.

**Solution**: Changed to explicit `CROSS JOIN` for clarity and to signal intentional single-row merge.

---

### Fix 4: silver__hourly_prices_priority.sql - Fixed Non-Sargable JOINs

**Problem**: 3 JOINs with `LOWER()` on both sides preventing index usage.

**Solution**: Pre-compute `LOWER()` values in CTEs (`prices_base`, `manual_metadata`, `asset_metadata`, `coin_info`), then join on pre-computed columns.

**Benefits**: Enables index usage on join columns.

---

### Fix 5: core__ez_transfers.sql - Fixed LOWER() + DATE_TRUNC() JOINs

**Problem**: `LOWER()` on join columns + `DATE_TRUNC('hour', block_timestamp)` computed for every row.

**Solution**: Pre-compute `token_address_lower` and `block_hour` in base CTEs (`transfers_base`, `tokens_base`, `prices_base`).

**Benefits**: Eliminates per-row function calls during join operations.

---

### Fix 6: Added incremental_predicates to 24 silver models

**Problem**: 84% of incremental models missing partition pruning configuration.

**Solution**: Added `incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"]` to:
- 17 DEX swap models (animeswap, auxexchange, cetus, hippo, liquidswap, pancake, sushi, thala, thala_v2, aires, batswap, cellana, cetus_clmm, hyperfluid, thala_v0, tsunami, tapp)
- 3 NFT sales models (mercato, souffl3, topaz)
- 2 NFT mint models (mints_combined, mints_v2)
- 2 bridge models (wormhole_transfers, layerzero_transfers)

**Benefits**: Enables partition pruning during incremental runs, reducing full table scans.

---

*Generated: 2026-01-07*
