

## Useful Workflow Tips
### Monitoring Databricks Usage

#### Viewing Serverless Compute Resource Usage

You can track serverless compute DBU consumption using the `system.billing.usage` system table:

```sql
SELECT
  usage_metadata.serverless_compute_id,
  identity_metadata.run_as,
  SUM(usage_quantity) AS total_dbus
FROM system.billing.usage
WHERE billing_origin_product IN ('JOBS', 'DLT', 'INTERACTIVE')
  AND usage_metadata.serverless_compute_id IS NOT NULL
  AND usage_date >= date_add(now(), -30)
GROUP BY 1, 2
ORDER BY 3 DESC;
```

Other built-in monitoring options:
- **System tables** — create dashboards and alerts from `system.billing.usage`
- **Serverless usage policies** — tag workloads to attribute usage
- **Budget alerts** — set up in your Databricks account
- **Pre-configured usage dashboard** — importable into your workspace

> ⚠️ There can be up to a **24-hour delay** between when you run a workload and when its usage appears in the billing system table.

#### Understanding Quotas & Resets

1. **Serverless rate limits (quotas)** — DBU-per-hour caps enforced on serverless compute for notebooks, jobs, pipelines, and SQL warehouses. Measured **per hour**, so the cap resets every hour. Sizes:
   - Small: ~60 DBUs/hr
   - Medium: ~120 DBUs/hr
   - Large: ~240 DBUs/hr
   - X-Large: ~480 DBUs/hr
   - 2X-Large: ~960 DBUs/hr
   - Default applies your workspace's default cap: Medium for premium-tier, Large for enterprise-tier.
2. **Billing/usage tracking** — No "reset" on usage itself; it accumulates continuously and is billed per your account's plan. System tables just record what's been consumed.
3. **Databricks Free Edition** — Serverless compute has limited usage and limited compute size, but there's no published "monthly reset" — you simply get restricted access to serverless resources.

#### How Billing Actually Works (Important)

Serverless compute **does not have a fixed monthly allotment**. It's pay-as-you-go — you're billed for exactly what you consume, and there's no "pool" that runs out or resets.

- **Rate limits (quotas)** are **per-hour caps** on how fast you can consume DBUs. They reset every hour.
  - Default for Premium-tier workspaces: Medium (~120 DBUs/hr)
  - Default for Enterprise-tier workspaces: Large (~240 DBUs/hr)
- **Billing/usage** accumulates continuously across the month — there's no monthly total to exhaust.
- **To check your workspace's rate limit**: go to **Admin Settings → Compute → Serverless** or ask your workspace admin. The per-hour quota is a workspace-level setting, not exposed in system tables.

#### What "Running Out of DBUs" Actually Means

Serverless compute **does not "run out" of DBUs** — there's no finite pool to exhaust.

| Scenario | What it means | When it resets |
|---|---|---|
| **Rate limit hit** | Too much usage in one hour — workloads get throttled | Next hour |
| **High bill** | Too much cumulative usage over the billing period — a cost concern, not a hard stop | Never — it just keeps accumulating |

- If you hit a rate limit, it means you're consuming too many DBUs **right now** (within a single hour). Workloads get throttled until the next hour resets the cap.
- If you're worried about cost, that's cumulative over the billing period. DBUs keep accumulating and you get billed for the total — there's no cap on the total.
- The only time you'd need to worry about "running out" is if a very heavy concurrent workload spikes past the per-hour rate limit cap.

#### Current Workspace Usage (as of Sep 2026)

Last 30 days: ~29.5 total DBUs across 2 serverless compute objects, 11 days of activity (Aug 27 – Sep 25). Very light usage, nowhere near hourly caps.

Docs:
- Monitor serverless cost: https://docs.databricks.com/aws/en/admin/system-tables/serverless-billing/
- Manage serverless compute: https://docs.databricks.com/aws/en/compute/serverless/manage-serverless-compute/


### Running the pipeline on your local machine

There are two ways to run the pipeline code, and they behave very differently:

- **Databricks CLI / Databricks Asset Bundles** (`databricks bundle deploy && databricks bundle run`)
  - The code from your PC is **uploaded to Databricks** and executes on **Databricks compute** (the platform).
  - It runs *your local code* but *on the platform* — the `.py` files are read from your PC, shipped up, and run by Databricks infrastructure.
  - The version that runs is whatever is on your PC at deploy time.

- **Plain `python real_estate_pipeline_silver.py`** on your PC
  - Runs **entirely locally** using your local Python/Spark environment.
  - It does not touch Databricks compute unless the code explicitly connects to a Databricks cluster via a Spark connect string.

> **Note:** If you're running from a local branch (e.g. `pc-branch`) and also making pipeline changes in Databricks on a different branch (e.g. `platform-dev`), the two codebases are independent. Push/pull between branches to sync before deploying from either side.

### Syncing between local and Databricks

`databricks sync` syncs a local directory to a **workspace directory** (like `/Workspace/Users/.../my-folder`), not to a **Git repo folder** (`/Repos/...`). Since this project lives in a Git repo, `databricks sync` won't work — use Git itself:

```bash
# Push from your PC (pc-branch)
git push origin pc-branch

# Push from Databricks (platform-dev)
git push origin platform-dev

# Pull the other branch's changes when you need them locally
git fetch origin
git merge origin/platform-dev
```

Other options:

- **`databricks bundle sync --watch`** (part of DABs) — continuously syncs local files to the bundle's deployment target in the workspace. This is for **deployment**, not for keeping two Git branches in sync.
- **A Git alias or script on your PC** — e.g. `git push origin pc-branch && git fetch origin` run periodically, so you can merge `platform-dev` changes whenever they land.

Git push/pull isn't automatic like `--watch`, but it's reliable, handles conflicts explicitly, and keeps a clean history.

### Why `row_number()` deduplication doesn't work in the silver pipeline

The silver layer originally tried to deduplicate records using a window function with `row_number()`. This approach **does not work in Spark Declarative Pipelines (SDP)** for two reasons:

**1. Window functions with `row_number()` are not supported in streaming queries**

The silver tables read from bronze with `spark.readStream.table(...)`, making them **streaming tables**. In Structured Streaming, window functions that require a total ordering within a partition (like `row_number()`, `rank()`, `dense_rank()`) are **stateful operations** — Spark would need to buffer all data per partition and sort it before assigning row numbers, which doesn't fit the incremental micro-batch model. The pipeline throws an error like:

```
AnalysisException: Non-time-based window functions are not supported in streaming queries;
if you must use them, use a batch read (spark.read.table) instead of a streaming read.
```

The code that triggered this:

```python
from pyspark.sql.window import Window

natural_key_cols = [col.replace(" ", "").lower() for col in natural_key]
window_spec = Window.partitionBy(*natural_key_cols).orderBy(F.col("last_updated").desc())
df = df.withColumn("_row_num", F.row_number().over(window_spec))
df = df.filter(F.col("_row_num") == 1).drop("_row_num")
```

**2. `dropDuplicates` with `Column` objects instead of strings**

The alternative approach using `dropDuplicates` also failed because `dropDuplicates()` expects **column name strings**, not `F.col(...)` objects:

```python
df = df.dropDuplicates(F.col("primary_key"), F.col("last_updated"))
# TypeError: dropDuplicates() expects column name strings, not Column objects
```

Even if the arguments were passed as strings (`df.dropDuplicates(["primary_key", "last_updated"])`), `dropDuplicates` in a streaming context keeps an **arbitrary** row per key, not the one with the latest `last_updated` — so it wouldn't achieve the intended "keep most recent" semantics.

**3. What works instead: `replace_using` + `sequence_by` in the `@dp.table` decorator**

SDP handles deduplication declaratively through the table decorator:

```python
@dp.table(
    name=f"silver_dev.redfin.{table_name}",
    replace_using=natural_key_silver,   # merge key — identifies the row to update
    sequence_by="last_updated",        # ordering — latest value wins on conflict
)
```

This tells the pipeline to use `MERGE` semantics: when a row arrives with the same natural key, the `sequence_by` column determines which version wins (highest value = most recent). This is the SDP-native way to keep only the latest record per key, and it works with streaming reads because the framework manages the merge internally rather than requiring a window function in the transformation code.
