%md
# Talk Track: Architecture, Decisions, and Issues

> Narrative reference for explaining the real estate platform's design, key roadblocks, and the reasoning behind major decisions. Useful for onboarding, demos, and stakeholder conversations.

---

## 1. Why Spark Declarative Pipelines (SDP)

The platform uses **Lakeflow Spark Declarative Pipelines** (SDP, formerly DLT / Delta Live Tables) as the execution engine for the bronze → silver → gold medallion architecture. This was an early design decision.

### What SDP gives us

| Capability | Why it matters here |
| --- | --- |
| **Declarative table definitions** | You define *what* each table should look like (columns, cleaning, dedup) via `@dp.table()`, not *how* to orchestrate the steps. SDP figures out the DAG, dependencies, and execution order. |
| **Streaming + batch in one pipeline** | Data for fact tables lands continuously via Auto Loader (streaming), while HUD reference data is static (batch). Both coexist in the same pipeline file — no separate streaming vs. batch infrastructure. |
| **Built-in data quality** | `@dp.expect_or_drop` is used to drop bad rows at ingest time. No separate validation step. |
| **Declarative dedup** | `replace_using` + `sequence_by` on the table decorator gives MERGE semantics — "keep the latest row per key" — without writing window functions or manual MERGE statements. |
| **Auto-optimization** | `pipelines.autoOptimize.zOrderCols` in table properties handles Z-ordering automatically. No separate `OPTIMIZE` jobs. |
| **Serverless compute** | No cluster to manage. The pipeline runs on serverless, scales automatically, and we pay only for what we use. |

### What it costs us

- **Runtime version constraints:** `replace_using` / `sequence_by` requires DBR 18.2+. The workspace was on a pre-18.2 runtime when we first ran the pipeline, causing a `TypeError`. (See Issue #1 below.)
- **No arbitrary Spark in streaming tables:** Window functions like `row_number()` don't work in streaming queries. Dedup must be declarative (`replace_using`) or deferred to batch reads. (See Issue #2.)
- **Pipeline libraries are explicit:** The `spark-pipeline.yml` glob (`src/pipelines/*.py`) is a DAB config that is NOT applied to the actual pipeline. New files must be manually added. (See Issue #6.)

### API evolution: `dlt` → `pyspark.pipelines`

The bronze pipeline uses the older `import dlt` API. The silver and gold pipelines use the newer `from pyspark import pipelines as dp` API. Both are valid — the newer API is the forward path, but bronze hasn't been migrated yet.

---

## 2. Ingestion Is Separate from the Pipeline

Data ingestion (fetching from external sources) and data transformation (cleaning, dedup, joins) are deliberately separated into different code paths:

```
src/ingestion/          ← Standalone notebooks: fetch from API/download, save raw files to UC volumes
    ├── redfin.ipynb              ← Downloads Redfin CSVs to /Volumes/bronze_dev/redfin/
    ├── hud_reference_data.py     ← Fetches HUD states/counties from API → JSON in volume
    ├── hud_fmr_ingestion.py      ← Fetches HUD Fair Market Rents → volume
    ├── hud_zip_crosswalk.ipynb   ← Downloads HUD USPS ZIP crosswalk CSV → volume
    ├── census_zcta_reference.py  ← (planned) Downloads Census ZCTA Gazetteer → volume
    └── hud_common.py             ← Shared HUD API helpers (auth, endpoints)

src/pipelines/          ← SDP pipeline files: read from volumes/bronze, transform, write tables
    ├── real_estate_pipeline_bronze.py   ← Auto Loader + batch reads from volumes
    ├── real_estate_pipeline_silver.py   ← Streaming + batch reads from bronze
    └── real_estate_pipeline_gold.py     ← Batch reads from silver
```

### Why separate them?

| Concern | Ingestion notebooks | SDP pipeline |
| --- | --- | --- |
| **Trigger** | Manual or scheduled job (on demand) | Pipeline update (incremental or full) |
| **What it does** | HTTP download / API call → save file to UC volume | Read files → clean → dedup → write Delta tables |
| **Failure mode** | Network error, API rate limit — retry the notebook | Data quality issue — expectation drops bad rows |
| **Scope of change** | Adding a new source = new notebook + volume path | Adding a new source = new bronze/silver/gold table defs |

If ingestion were inside the pipeline, an API outage would block the entire pipeline update. By separating, the pipeline always works with whatever data is already in the volumes — ingestion failures are isolated and retryable.

### How data flows from ingestion to pipeline

```
External source (API / download)
    ↓
src/ingestion/<notebook>           ← fetch + save raw file
    ↓
/Volumes/bronze_dev/<schema>/...   ← raw files in UC volume
    ↓
Bronze pipeline (Auto Loader)     ← watches volume, streams new files into bronze table
    ↓
Silver pipeline                   ← reads bronze, cleans/dedups/types
    ↓
Gold pipeline                     ← joins multiple silver sources, drops metadata
```

---

## 3. Fact vs. Dimension Tables: Role Drives the Implementation

What makes a table a fact or a dimension is its **role**, not its read pattern. Facts capture events/measurements at a grain — rows accumulate over time, and the same key can arrive repeatedly, so they need dedup keyed on grain + uniqueness. Dimensions are static reference data with naturally unique keys, replaced wholesale rather than appended to. Streaming vs. batch is an implementation choice that *follows* from that role: accumulating fact data is processed incrementally (streaming), static reference data is simply re-read (batch).

### Fact tables: streaming with dedup

Redfin tables (housing market, price drops, delistings/relistings), HUD FMR, ACS, and Opportunity Insights all use the **streaming pattern**:

```python
@dp.table(
    name="silver_dev.redfin.housing_market_county",
    replace_using=["period_begin", "period_end", "region_name"],  # MERGE key
    sequence_by="last_updated",                                    # latest wins
)
def load():
    df = spark.readStream.table("bronze_dev.redfin.housing_market_county")  # streaming
    df = clean_columns_silver(df)
    df = cast_redfin_columns(df)
    return df
```

- **`spark.readStream.table()`** — incremental processing, handles new files as they arrive
- **`replace_using`** — identifies the natural key; incoming rows MERGE with existing
- **`sequence_by`** — tiebreaker; highest value wins (most recent `last_updated`)
- Result: a **streaming table** that stays deduplicated as new data arrives

### Dimension tables: batch, no dedup

HUD states and counties are static reference data that rarely changes. They use the **batch pattern**:

```python
@dp.table(
    name="silver_dev.hud.dim_states",
)
def dim_states():
    df = spark.read.table("bronze_dev.hud.states")  # batch read, not streaming
    df = clean_columns_silver(df)
    return df
```

- **`spark.read.table()`** — reads the full table once, no incremental processing
- **No `replace_using` / `sequence_by`** — no dedup needed (reference data is overwritten, not appended)
- Result: a **materialized view** that refreshes on each pipeline update

### Gold fact tables: batch reads from silver

Gold fact tables join multiple silver sources. They also use batch reads, but for a different reason — joining multiple streaming sources in SDP is complex, and gold tables are meant for analytics snapshots:

```python
@dp.table(name="gold_dev.default.county_housing_profile")
def load():
    hm = spark.read.table("silver_dev.redfin.housing_market_county")   # batch
    pd = spark.read.table("silver_dev.redfin.price_drops_county")       # batch
    dr = spark.read.table("silver_dev.redfin.delistings_relistings_county")  # batch
    df = hm.join(pd, on=[...], how="left").join(dr, on=[...], how="left")
    return df
```

- **Batch reads from silver** — gets the current materialized state of each silver table
- **Left joins** — base table (housing market) enriched with optional data (price drops, delistings)
- **No streaming** — gold tables are materialized views, refreshed on each pipeline update

### Summary table

| Table type | Read pattern | Dedup | SDP artifact | Examples |
| --- | --- | --- | --- | --- |
| Bronze fact | Auto Loader (streaming from volume) | None | Streaming table | Redfin, FMR, ACS, social capital |
| Bronze reference | `spark.read.format()` (batch from volume) | None | Materialized view | HUD states, counties |
| Silver fact | `spark.readStream.table()` (streaming from bronze) | `replace_using` + `sequence_by` | Streaming table | All Redfin, FMR, ACS, social capital |
| Silver dimension | `spark.read.table()` (batch from bronze) | None | Materialized view | HUD dim_states, dim_counties |
| Gold fact | `spark.read.table()` (batch from silver) | None (already deduped) | Materialized view | county_housing_profile, zip_market_health |
| Gold dimension | `spark.read.table()` (batch from silver) | None | Materialized view | (planned) geo_re.dim_state, dim_county |

---

## 4. Medallion Architecture Choices

### Three catalogs, not one

| Catalog | Purpose | Schemas |
| --- | --- | --- |
| `bronze_dev` | Raw ingestion, no cleaning | redfin, hud, census_bureau, opportunity_insights |
| `silver_dev` | Cleaned, typed, deduplicated | redfin, hud, census_bureau, opportunity_insights |
| `gold_dev` | Analytics-ready, business-facing | default (fact tables), geo_re (planned dimensions) |

Separate catalogs make access control simple — dashboards and apps only need `USE CATALOG gold_dev`, never touching bronze or silver directly.

### Gold schema strategy

- **`gold_dev.default`** — fact tables (county_housing_profile, zip_market_health, neighborhood_market_activity)
- **`gold_dev.geo_re`** (planned) — geographic dimension tables (dim_state, dim_county, dim_zipcode, dim_zcta). Named `geo_re` to avoid collision with the existing `gold_dev.geo` schema used by another project.

### Gold layer conventions

- **Drop metadata columns** — `rescued_data`, `ingest_timestamp`, `source_file`, `_processed_timestamp` are pipeline plumbing; gold exposes only business-relevant columns.
- **Add `gold_refresh_timestamp`** — a single `F.current_timestamp()` for freshness tracking, replacing the multiple metadata timestamps from silver.
- **Drop common columns from enrichment tables** — when joining housing_market (base) with price_drops and delistings_relistings, the enrichment tables skip shared columns (`last_updated`, `frequency`, `region_type`, `metro`) to avoid conflicts.

---

## 5. Redfin Grain Tables: Kept Separate, Not Stacked

The Redfin silver layer has 9 tables — three grains (county, ZIP, neighborhood) × three data types (housing_market, price_drops, delistings_relistings). Within each data type, the schemas are identical across grains (same column names, same types) with one exception: `region_name` is `STRING` for county/neighborhood but `INT` for zipcode. Since the `region_type` column already discriminates cleanly (`'County'`, `'Zip'`, `'Neighborhood'`), the question came up: why not "stack" the three grain tables into one per data type (3 tables instead of 9) using `UNION ALL` + `CAST(region_name AS STRING)` for the zipcode tables?

### Decision: keep separate grain tables — do not stack

The gold tables are enrichment tables, and the enrichment strategy differs fundamentally by grain:

| Grain | External enrichment | Join complexity |
| --- | --- | --- |
| County | HUD FMR (aggregate ZIPs → county), ACS (aggregate ZCTAs → county), dim_county | High — needs crosswalks + aggregation |
| ZIP | social_capital_zip, ACS (direct), HUD FMR (direct) | Medium — direct joins |
| Neighborhood | None available | Low — Redfin only |

Stacking these into one gold table would mean either carrying a forest of NULLs for grain-specific enrichments or stripping enrichment down to the common denominator (losing the gold layer's value-add). The `region_name` column also becomes polymorphic — a county name, a ZIP integer, or a neighborhood name — so downstream joins need conditional logic keyed on `region_type`, which is fragile.

### Tradeoffs

| | Separate grain tables (chosen) | Stacked tables (rejected) |
| --- | --- | --- |
| **Pipeline code** | Triplicated join logic across 3 grains (mitigated with a shared factory function) | Write join logic once, filter by `region_type` |
| **Gold enrichment** | Each table carries only its grain's enrichment — no NULLs, clear semantics | Must handle all enrichment strategies in one table or drop grain-specific enrichment |
| **Downstream joins** | Each table joins to its dimension tables on a single, clear key | Joins must branch on `region_type` — polymorphic `region_name` key |
| **Partition pruning** | Each query naturally scans only its grain | Stacked table includes all grains; partitioning on `region_type` adds overhead |
| **Table count** | 3 gold tables (one per grain) | 1–3 stacked tables (one per data type, or a single wide table) |

### Implementation

The Redfin join logic (housing_market + price_drops + delistings_relistings at the same grain) is identical across the three grains. To avoid triplication, the gold pipeline uses the shared factory function `_create_redfin_gold_table(grain, table_name, comment)` — each gold table is one call with the grain as a parameter, instead of the same join logic written three times.

---

## 6. Issues Encountered

These are the roadblocks hit during development, with context on why they happened and how they were resolved.

### Issue 1: `replace_using` / `sequence_by` not supported on this runtime

When running the silver pipeline, the pipeline fails with:

```
TypeError: table() got an unexpected keyword argument 'replace_using'
```

The `replace_using` and `sequence_by` parameters on `@dp.table()` require **Databricks Runtime 18.2+**. This workspace was on a pre-18.2 runtime — the `dp.table()` signature has `replace_where` but no `replace_using` or `sequence_by`, and `@dp.replace_flow` doesn't exist in the module at all.

**All tables using `replace_using` are equally affected** — the Redfin silver tables use the same `replace_using` + `sequence_by` pattern and would hit the identical `TypeError` if run now. The only silver table that would work on this runtime is **ACS** (`american_community_survey_zcta`), which doesn't use `replace_using` at all.

**Unclear why this appeared to work before** — best guess is that something changed in the compute environment. The parameters have never been valid on the *current* runtime, for any table.

**Options to fix:**

| Option | What it involves | Works now? |
| --- | --- | --- |
| **Upgrade DBR to 18.2+** | Change the pipeline's DBR — existing code works as-is | After upgrade |
| **Use `dp.create_auto_cdc_flow()`** | Available on < 18.2 runtime — has `keys` + `sequence_by`. Requires CDF enabled on bronze source tables | Yes, if CDF is enabled |
| **Skip dedup in silver** | Append all rows in silver, dedup at query time or in gold with batch read + window function | Yes |

**Status:** The pipeline now runs successfully (DBR was upgraded or the runtime caught up). `replace_using` + `sequence_by` work as expected.

---

### Issue 2: Window functions (`row_number()`) not supported in streaming queries

The silver layer originally tried to deduplicate records using `row_number()` over a window partitioned by the natural key and ordered by `last_updated desc`. This **fails in SDP** because silver tables read from bronze with `spark.readStream.table(...)`, making them streaming tables:

```
AnalysisException: Non-time-based window functions are not supported in streaming queries;
if you must use them, use a batch read (spark.read.table) instead of a streaming read.
```

`row_number()`, `rank()`, and `dense_rank()` are **stateful operations** — Spark would need to buffer all data per partition and sort it, which doesn't fit the incremental micro-batch model.

**Fallback `dropDuplicates` also failed:**

```python
df = df.dropDuplicates(F.col("primary_key"), F.col("last_updated"))
# TypeError: dropDuplicates() expects column name strings, not Column objects
```

Even with string arguments, `dropDuplicates` in a streaming context keeps an **arbitrary** row per key — not the latest by `last_updated` — so it doesn't achieve "keep most recent" semantics.

**Resolution:** Switched to `replace_using` + `sequence_by` in the `@dp.table` decorator, which delegates MERGE semantics to SDP. This is the SDP-native way to dedup streaming tables.

---

### Issue 3: Local Databricks Connect setup challenges

Setting up local development with Databricks Connect + VS Code + serverless compute surfaced several issues (all resolved, documented in `LOCAL_SETUP.md`):

| Issue | Cause | Resolution |
| --- | --- | --- |
| `NameError: name 'spark' is not defined` | Standard Jupyter kernel, no Spark session | Explicit `DatabricksSession.builder.serverless().profile(...).getOrCreate()` |
| `Cluster id or serverless are required but were not specified` | No compute target configured | Select **Serverless** in Databricks extension; use `.serverless()` |
| `No module named 'databricks.connect'` | Wrong interpreter or incomplete env setup | Select project `.venv` kernel; rerun Full Environment Setup |
| `display()` shows only 20 rows | Spark Connect HTML representation limit | Collect to pandas (`df.toPandas()`) with `pd.set_option('display.max_rows', N)` |
| `spark.sql.repl.eagerEval.maxNumRows` not writable | Serverless doesn't expose this config | Use `toPandas()` or `df.show(N)` instead |
| VS Code extension install `ERR_CONNECTION_RESET` | Marketplace access blocked (VPN/network) | Retry without VPN or allow `*.gallery.vsassets.io` hosts |

---

### Issue 4: `databricks sync` incompatible with Git repo folders

`databricks sync` syncs a local directory to a **workspace directory** (`/Workspace/Users/...`), not to a **Git repo folder** (`/Repos/...`). Since this project lives in a Git repo, `databricks sync` doesn't work. Must use Git push/pull to sync between the local PC (`pc-branch`) and Databricks (`platform-dev` / `shared-dev`).

---

### Issue 5: Bronze layer lacks column cleaning

The bronze pipeline (`real_estate_pipeline_bronze.py`) loads raw data via Auto Loader without any column name cleaning — original CSV headers with spaces, special characters, and mixed case are preserved as-is. All cleaning happens in silver. This means bronze table columns have names like `"MEDIAN SALE PRICE ($)"` and `"PRICE DROPS YoY (%)"`, which require backtick-quoted SQL queries.

---

### Issue 6: Pipeline libraries use explicit file paths, not glob pattern

The pipeline `real_estate_pipeline_new` is configured with **explicit file paths** in its `libraries` setting — `real_estate_pipeline_bronze.py`, `real_estate_pipeline_silver.py`, and `real_estate_pipeline_gold.py` are each listed individually. The glob pattern (`src/pipelines/*.py`) in `spark-pipeline.yml` is a DAB config that is **not** applied to the actual pipeline at runtime. New pipeline files must be **manually added** to the pipeline libraries via `editPipelineSettings` or the pipeline UI, otherwise the pipeline update will succeed but the new tables won't be created.

---

### Issue 7: Incomplete null checks on composite key columns

An external review recommended adding `expect_or_fail` checks on every component of each silver table's composite key, pointing out that HUD FMR checked `zip_code` and `year` but not `county_name`.

**Why the gap mattered:** the keys drive `replace_using` dedup — a row with a null key component won't match anything in the MERGE and can seed duplicate/ambiguous rows in silver (and propagate into gold joins).

**What was actually wrong (broader than the review said):**

- HUD FMR: `county_name` null check missing (the review's example — correct)
- Redfin: `region_name` (third key column) unchecked; `period_begin`/`period_end` were already implicitly covered because a NULL comparison fails `valid_period`
- ACS: **no expectations at all**, and also missing `replace_using`/`sequence_by` despite `silver_key` existing in its config — so no dedup either
- Opportunity Insights: already fully covered (single-column key `zip_code`)

**Why `expect_or_drop`, not `expect_or_fail`:** `expect_or_fail` fails the flow for the affected table — and any downstream flows that depend on it, such as gold tables reading that silver table. Independent parallel flows in the same update are *not* affected, but the failed flow blocks its part of the update until the offending row is dealt with. Since a row with a null key is unusable anyway, dropping it is the proportionate response: `expect_or_drop` removes just that row and records the drop in the pipeline event log, so drop-rate spikes can be alerted on. `expect_or_fail` is reserved for a true fail-fast data contract — where bad data should stop the flow, not just one row — if one is ever needed.

**Resolution:** Added `expect_or_drop` null checks for `region_name` (Redfin), `county_name` (HUD FMR), and `zcta_code` (ACS), and added `replace_using` (`zcta_code`) + `sequence_by` (`_ingest_timestamp`) to ACS so it dedups like the other silver facts.

---

## 7. Visualization: Databricks App (Node.js + Bootstrap)

The interactive visualization website is a **Databricks App** — a lightweight web app hosted in the Databricks environment, built with **Node.js** and **Bootstrap**, and developed with **Codex** (AI coding agent).

### Decision: custom micro-app instead of an out-of-the-box BI tool

| Concern | Custom Databricks App (chosen) | Out-of-the-box BI (Power BI / Tableau) (rejected) |
| --- | --- | --- |
| **Flexibility** | Full control over UI, interactions, and custom visualization behavior | Constrained to the tool's chart types, layouts, and extension model |
| **Speed of development** | Fast iteration, accelerated by AI-assisted development (Codex) | Quick start for standard dashboards, but custom behavior needs workarounds |
| **Databricks integration** | Native — runs where the data lives, reads gold tables directly, no extra connector or gateway | External tool: connectors, gateways, separate licensing and governance |
| **Cost** | No additional BI license | Per-seat licensing |
| **Tradeoff** | We own the code, testing, and maintenance | Vendor handles hosting, updates, and sharing |

The deciding factors were **flexibility and speed of development** — the app needs custom interactions (grain switching, trend exploration, drill-downs) that are awkward to build in BI tools — and **easy integration with the Databricks environment**: the app runs where the data already lives and queries the gold layer directly.

---

## 8. Future Enhancements

### Pipeline

- **Glob pattern for pipeline libraries** — replace the explicit file list in `real_estate_pipeline_new` with the glob pattern (`src/pipelines/*.py`) so new pipeline files are picked up automatically instead of added manually (resolves Issue #6).
- **Bronze column cleaning** — normalize column names to snake_case at bronze so downstream SQL no longer needs backtick-quoted names like `"MEDIAN SALE PRICE ($)"` (resolves Issue #5).

### App features

- **Neighborhood-level zoom** — drill from county → ZIP → neighborhood in the map views.
- **Additional trends** — more time-series views: inventory, days on market, months of supply, sale-to-list ratios, across all three grains.
- **Investment-area identification** — composite scoring of price trends, supply, and social-capital / ACS indicators to flag potential investment areas.

### Data & enrichment

- **Complete the geo dimensions** — build the planned `gold_dev.geo_re` dimension tables (dim_state, dim_county, dim_zipcode, dim_zcta) and ingest the Census ZCTA Gazetteer reference data.
- **Crosswalk-based aggregation** — use the ingested HUD USPS ZIP crosswalk to roll up ZIP-level metrics to county where direct county data is missing.
- **Historical snapshots** — keep periodic gold snapshots so the app can show real change-over-time rather than current-state only.

### Operations

- **Scheduled updates + alerting** — schedule pipeline updates and alert on failures or expectation drop-rate spikes (drop metrics are already recorded in the pipeline event log).
- **Metric layer** — define canonical KPIs as Unity Catalog metric views so the app and any future BI consumers share the same formulas.
- **Data freshness indicator** — surface `gold_refresh_timestamp` in the app so users know how current the data is.
