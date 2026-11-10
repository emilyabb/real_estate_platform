# Real Estate Platform

Real estate data platform on Databricks, built on a medallion architecture (bronze / silver / gold) with Lakeflow Spark Declarative Pipelines.

- **Bronze** (`bronze_dev`) — raw files landed in UC volumes and ingested by `src/pipelines/real_estate_pipeline_bronze.py` (Auto Loader for streaming sources, batch reads for static reference data)
- **Silver** (`silver_dev`) — cleaned, deduplicated, standardized tables (`src/pipelines/real_estate_pipeline_silver.py`)
- **Gold** (`gold_dev`) — analytics-ready fact tables and geographic dimensions (`src/pipelines/real_estate_pipeline_gold.py`)

External sources: Redfin housing market data, Opportunity Insights social capital, Census Bureau ACS, HUD Fair Market Rents and state/county reference data, and USPS ZIP codes. See `docs/DATA_SOURCES.md` for the source overview and `docs/DATA_DICTIONARY.md` for column-level detail.

## Documentation

- `docs/DATA_SOURCES.md` — external sources overview
- `docs/DATA_DICTIONARY.md` — column-level silver/gold reference
- `docs/TALK_TRACK.md` — design narrative and key decisions

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

Serverless compute **does not "run out"** of DBUs — there's no finite pool to exhaust.

| Scenario | What it means | When it resets |
|---|---|---|
| **Rate limit hit** | Too much usage in one hour — workloads get throttled | Next hour |
| **High bill** | Too much cumulative usage over the billing period — a cost concern, not a hard stop | Never — it just keeps accumulating |

- If you hit a rate limit, it means you're consuming too many DBUs **right now** (within a single hour). Workloads get throttled until the next hour resets the cap.
- If you're worried about cost, that's cumulative over the billing period. DBUs keep accumulating and you get billed for the total — there's no cap on the total.
- The only time you'd need to worry about "running out" is if a very heavy concurrent workload spikes past the per-hour rate limit cap.

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

Why `databricks sync` doesn't work for this project is covered in `docs/TALK_TRACK.md` (Issue #4). Git quick-reference:

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

## Local Databricks setup

This project is configured for local development in Visual Studio Code with the official Databricks extension, Jupyter notebooks, and Databricks Connect. Python code runs in the local virtual environment, while Spark DataFrame operations run on Databricks compute.

### Current project configuration

- Python: 3.12
- Databricks Connect: 18.0
- Databricks serverless environment: version 5
- Databricks workspace: `https://dbc-61c13420-8917.cloud.databricks.com`
- Development target: `dev` in `databricks.yml`
- Local environment: `.venv`, managed by `uv`
- Authentication: OAuth profile, currently named `dbc-61c13420-8917`

The generated dependency constraints in `pyproject.toml` are managed by `databricks environments setup-local` and should not be edited by hand.

### Prerequisites

Install:

1. Visual Studio Code.
2. The official **Databricks** extension (`databricks.databricks`).
3. The Microsoft **Python** extension (`ms-python.python`).
4. The Microsoft **Python Debugger** extension (`ms-python.debugpy`).
5. The Microsoft **Jupyter** extension (`ms-toolsai.jupyter`).

If extension installation fails with `ERR_CONNECTION_RESET` or an error fetching `ms-python.debugpy`, the failure is access to the Visual Studio Marketplace rather than Databricks. Retry without a VPN or have the network allow these hosts:

```text
marketplace.visualstudio.com
*.gallery.vsassets.io
vscode.download.prss.microsoft.com
www.vscode-unpkg.net
```

Extensions can also be installed from the command line:

```powershell
code --install-extension ms-python.python
code --install-extension ms-python.debugpy
code --install-extension ms-toolsai.jupyter
code --install-extension databricks.databricks
```

If `code` is not found, add the VS Code command-line launcher to `PATH`. On Linux, configuration profiles are stored in `~/.databrickscfg`; on Windows, the equivalent location is `%USERPROFILE%\.databrickscfg`. The project setup creates the platform-appropriate virtual environment automatically: `.venv/bin/python` on Linux and `.venv\Scripts\python.exe` on Windows.

### First-time setup in VS Code

1. Clone the repository and open its root folder in VS Code. The root is the directory containing `databricks.yml` and `pyproject.toml`.
2. Select the Databricks icon in the Activity Bar.
3. If the extension reports that no Databricks project configuration was detected, select **Create configuration**.
4. Select the workspace ending in `8917`.
5. For authentication, select the existing `dbc-61c13420-8917` profile and authenticate with OAuth. If that profile does not exist on a new computer, choose **OAuth — Create a profile and authenticate using OAuth**. A Personal Access Token is not required.
6. Select the `dev` target if prompted.
7. Under compute, select **Serverless**.
8. For the Python environment, select **Full Environment Setup**. This creates or updates `.venv`, installs the compatible Databricks Connect package, and prepares the Jupyter dependencies.
9. Wait for the setup to finish. If VS Code shows **Databricks Connect disabled**, select it and complete the prompts until it shows **Databricks Connect enabled**.
10. Open a `.ipynb` notebook. Use the kernel picker in the upper-right corner to select the project's `.venv` interpreter. It may appear as `real-estate-platform (3.12.x)`.
11. Restart the notebook kernel after changing the environment or compute selection.

The **SSH Tunnel** option is not required for this workflow. It opens a separate remote VS Code session. This project uses the local VS Code session with Databricks Connect.

### Start a Spark session

In a local Jupyter kernel, initialize the session explicitly. Specifying serverless compute prevents the `Cluster id or serverless are required but were not specified` error.

```python
from databricks.connect import DatabricksSession

spark = (
    DatabricksSession.builder
    .serverless()
    .profile("dbc-61c13420-8917")
    .getOrCreate()
)
```

Then validate the connection:

```python
spark.sql("SELECT current_user(), current_catalog()").show()
```

For code intended to run both locally and inside Databricks, the local profile can instead be configured with `serverless_compute_id = auto`. After that, the generic initializer works locally while Databricks uses its existing global Spark session:

```python
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()
```

Do not configure Databricks Connect with `.master("local[*]")`.

### Running notebooks cell by cell

Open an `.ipynb` file and use the run button beside a cell. Ordinary Python runs in `.venv` on the local computer. Spark operations are submitted to Databricks serverless compute, and their results are returned to the notebook.

Some notebook commands also run locally:

- `%pip` installs packages into the local environment.
- `%sh` executes on the local computer.
- `%sql` is routed through `spark.sql`.

### Troubleshooting

#### `NameError: name 'spark' is not defined`

The notebook is using a standard Jupyter kernel and no Spark session has been initialized. Run the explicit `DatabricksSession.builder.serverless()` initialization shown above. Also confirm that the selected kernel belongs to this project's `.venv` and that Databricks Connect is enabled in VS Code.

#### `Cluster id or serverless are required but were not specified`

Authentication succeeded, but Databricks Connect has no compute target. Select **Serverless** in the Databricks extension and use `.serverless()` when building the session.

For classic compute, select a cluster in the extension and configure its cluster ID instead:

```python
from databricks.connect import DatabricksSession
from databricks.sdk.core import Config

config = Config(
    profile="dbc-61c13420-8917",
    cluster_id="YOUR-CLUSTER-ID",
)

spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
```

#### `No module named 'databricks.connect'`

The notebook is using the wrong Python interpreter, or Full Environment Setup did not finish. Select the project's `.venv` kernel. If necessary, rerun **Full Environment Setup** from the Databricks extension.

#### `display()` shows only 20 Spark DataFrame rows

In a local VS Code Jupyter notebook, `display(spark_dataframe)` uses IPython and Spark Connect's HTML representation rather than the Databricks notebook data grid. That representation may render only 20 rows even when the SQL query has a larger `LIMIT`. The query limit and the display limit are separate.

Do not try to change the display limit with:

```python
spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 1000)
```

Databricks serverless does not make that configuration writable and returns:

```text
AnalysisException: [CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION]
Configuration spark.sql.repl.eagerEval.maxNumRows is not available.
```

The working resolution is to explicitly collect a bounded result as a pandas DataFrame and configure pandas to render the desired number of rows:

```python
import pandas as pd

pd.set_option("display.max_rows", 1000)

df = spark.sql("""
SELECT table_catalog, table_schema, table_name
FROM silver_dev.INFORMATION_SCHEMA.TABLES
ORDER BY table_schema, table_name
LIMIT 1000
""")

display(df.toPandas())
```

Because `toPandas()` loads the result into local memory, always keep a reasonable SQL `LIMIT`. For plain-text output that does not collect into pandas, use `df.show(1000, truncate=False)` instead.

#### OAuth/profile problems

Return to the Databricks extension's Configuration panel and reselect the authentication profile. Prefer OAuth. Profile credentials are stored outside the repository in the user's Databricks configuration; `.databricks` is ignored by Git.

### Relevant project files

- `databricks.yml`: workspace and development target configuration.
- `pyproject.toml`: Python version, Databricks Connect version, and generated runtime constraints.
- `uv.lock`: locked local dependencies.
- `.venv/`: local Python environment; do not commit it.
- `.vscode/settings.json`: Databricks/Jupyter notebook cell markers.

### Reference documentation

- [Databricks IDE extension configuration](https://docs.databricks.com/aws/en/dev-tools/vscode-ext/configure)
- [Run notebook cells with Databricks Connect](https://docs.databricks.com/aws/en/dev-tools/vscode-ext/notebooks)
- [Configure compute for Databricks Connect](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/cluster-config)
- [VS Code network configuration](https://code.visualstudio.com/docs/setup/network)

## Future additions

- **HUD USPS ZIP Code Crosswalk** — quarterly ZIP → county FIPS mapping files from HUD (https://www.huduser.gov/portal/pps_crosswalk.html), with allocation ratios covering what fraction of a ZIP's residential/business addresses fall in each county. The current ZIP dimension (`geo_re.dim_zipcode`, from `bronze_dev.geo_re.usps_zip_codes`) carries county names but not FIPS codes, so the crosswalk will bridge data across different grains — joining county-grain sources (Redfin county, HUD FMR, ACS county) with ZIP-grain sources (Redfin ZIP, social capital, ACS ZCTA) via ZIP → county FIPS. The ingestion notebook is scaffolded at `src/ingestion/hud_zip_crosswalk.ipynb` but has not been run (blocked on the HUD site being down).
