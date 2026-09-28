# Change history since last session

## Overview of recent work (past ~2 weeks)

**Data ingestion pipelines fixed:**
- **Redfin** 
- **HUD  data**
- **HUD Fair Market Rents**
- **Census Bureau ACS** 
- **Opportunity Insights** 

**Pipeline layers built:**
- **Bronze** (`real_estate_pipeline_bronze.py`): Auto Loader streaming tables for all sources (9 Redfin, HUD states/counties/FMR, ACS, social capital) with schema evolution.
- **Silver** (`real_estate_pipeline_silver.py`): Column cleaning (snake_case), type casting, NA→null, data quality expectations (`expect_or_drop`), and dedup via `replace_using` + `sequence_by`.

**Dev environment & docs:**
- Configured local Databricks Connect (v18.0) with VS Code, serverless compute, OAuth profile. Documented in `LOCAL_SETUP.md` with troubleshooting for common local issues.
- Created `DATA_SOURCES.md`, `NOTES.md` (workflow tips, billing, dedup explanation), and DABs config (`databricks.yml`, `spark-pipeline.yml`, `pyproject.toml`).

**Git workflow:**
- Pipeline fixes committed on `platform-dev` branch (Databricks side).
- Created `shared-dev` branch from `platform-dev` for both local PC and Databricks to share.
- Local PC work on `pc-branch`.

# Issues Encountered

## `replace_using` / `sequence_by` not supported on this runtime

When running the silver pipeline, the pipeline fails with:

```
TypeError: table() got an unexpected keyword argument 'replace_using'
```

The `replace_using` and `sequence_by` parameters on `@dp.table()` require **Databricks Runtime 18.2+** (see [Databricks docs](https://docs.databricks.com/aws/en/ldp/flows-replace-using/)). This workspace is on a pre-18.2 runtime — the `dp.table()` signature has `replace_where` but no `replace_using` or `sequence_by`, and `@dp.replace_flow` doesn't exist in the module at all.

**All tables using `replace_using` are equally affected** — the Redfin silver tables use the same `replace_using` + `sequence_by` pattern and would hit the identical `TypeError` if run now. The only silver table that would work on this runtime is **ACS** (`american_community_survey_zcta`), which doesn't use `replace_using` at all.

**Unclear why this appeared to work before** — we don't know why the runtime would have changed, but best guess is that something changed in the compute environment since then. The parameters have never been valid on the *current* runtime, for any table.

**Options to fix:**

| Option | What it involves | Works now? |
| --- | --- | --- |
| **Upgrade DBR to 18.2+** | Change the pipeline's DBR — existing code works as-is | After upgrade |
| **Use `dp.create_auto_cdc_flow()`** | Available on this runtime — has `keys` + `sequence_by`. Requires CDF enabled on bronze source tables | Yes, if CDF is enabled |
| **Skip dedup in silver** | Append all rows in silver, dedup at query time or in gold with batch read + window function | Yes |

## Window functions (`row_number()`) not supported in streaming queries

The silver layer originally tried to deduplicate records using `row_number()` over a window partitioned by the natural key and ordered by `last_updated desc`. This **fails in Spark Declarative Pipelines** because silver tables read from bronze with `spark.readStream.table(...)`, making them streaming tables:

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

**Resolution:** Switched to `replace_using` + `sequence_by` in the `@dp.table` decorator (see issue above), which delegates MERGE semantics to SDP. That in turn requires DBR 18.2+, which is the current blocker.

---

## Local Databricks Connect setup challenges

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

## `databricks sync` incompatible with Git repo folders

`databricks sync` syncs a local directory to a **workspace directory** (`/Workspace/Users/...`), not to a **Git repo folder** (`/Repos/...`). Since this project lives in a Git repo, `databricks sync` doesn't work. Must use Git push/pull to sync between the local PC (`pc-branch`) and Databricks (`platform-dev` / `shared-dev`).

---

## Incomplete refactor in `transformations/bronze.py`

The file `src/pipelines/transformations/bronze.py` contains an in-progress refactor with commented-out code and imports referencing modules that don't exist in the current project structure (`src.config.sources.redfin`, `src.ingestion.read_table`, `src.bronze.prep`). The active bronze pipeline (`src/pipelines/real_estate_pipeline_bronze.py`) uses the older `dlt` module directly and works — the refactor appears abandoned or paused.

---

## Large Redfin tables excluded from pipeline for testing

Two of the nine Redfin tables — `housing_market_zipcode` and `housing_market_neighborhood` — are filtered out in `sources_ref.py` to keep pipeline runs fast during development. Full production coverage will require re-enabling these (they are the largest grain tables and would significantly increase pipeline runtime).

---

## Bronze layer lacks column cleaning

The bronze pipeline (`real_estate_pipeline_bronze.py`) loads raw data via Auto Loader without any column name cleaning — original CSV headers with spaces, special characters, and mixed case are preserved as-is. All cleaning happens in silver. This means bronze table columns have names like `"MEDIAN SALE PRICE ($)"` and `"PRICE DROPS YoY (%)"`, which require backtick-quoted SQL queries. The bronze pipeline TODO notes acknowledge this (`#todo Add LAST_UPDATED to redfin keys for bronze`).
When Genie's "thinking" stream has ten "Actually"'s 💀🥀🫠