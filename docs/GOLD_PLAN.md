# Gold Layer Plan

> Temporary planning doc — delete once the gold pipeline is built.

## 1. Geographic Dimensions Schema (`geo_re`)

Create a new schema `gold_dev.geo_re` to hold geographic dimension tables shared across all gold fact tables. Named `geo_re` (geo for real estate) to avoid collision with the existing `gold_dev.geo` schema used by another project.

**Schema creation:**

```sql
CREATE SCHEMA IF NOT EXISTS gold_dev.geo_re
  COMMENT 'Real estate geographic dimension tables (state, county, ZIP, ZCTA)';
```

All tables in this schema are **batch** (not streaming) — they're built from static reference tables and rarely-changing lookups.

Each dimension below traces to an **authoritative reference source** — not derived from fact tables. State and county already have authoritative sources in the pipeline (HUD `listStates` / `listCounties` APIs). ZIP and ZCTA need new reference sources ingested via `src/ingestion/`, following the same pattern as `hud_reference_data.py` (fetch → save to volumes → bronze batch read → silver materialized view).

**Ingestion pattern (HUD reference data template):**

```
src/ingestion/<source>_reference_data.py   ← notebook: fetch from API/download, save raw to UC volume
  ↓
/Volumes/bronze_dev/<schema>/<schema>_raw/reference_data/   ← raw files in volume
  ↓
src/pipelines/real_estate_pipeline_bronze.py   ← batch spark.read (NOT Auto Loader)
  ↓
bronze_dev.<schema>.<table>   ← bronze table
  ↓
src/pipelines/real_estate_pipeline_silver.py   ← @dp.table batch read from bronze
  ↓
silver_dev.<schema>.dim_<name>   ← silver materialized view
  ↓
gold_dev.geo_re.dim_<name>   ← gold: direct read from silver, drop metadata columns
```

---

### 1a. `gold_dev.geo_re.dim_state` — HUD `listStates` API ✅ (existing)

**Purpose:** State dimension table — one row per US state.

**Authoritative source:** HUD FMR API `listStates` endpoint — already ingested.

**Ingestion chain (existing, no changes needed):**

| Stage | Location | Notes |
| --- | --- | --- |
| Ingestion notebook | `src/ingestion/hud_reference_data.py` | Fetches from HUD API, saves JSON to volume |
| Raw volume | `/Volumes/bronze_dev/hud/hud_raw/reference_data/states_*.json` | Timestamped JSON |
| Bronze | `bronze_dev.hud.states` | Batch `spark.read.format("json")` in bronze pipeline |
| Silver | `silver_dev.hud.dim_states` | Materialized view — `state_code`, `state_name` |
| **Gold** | **`gold_dev.geo_re.dim_state`** | Direct read from silver, drop metadata columns |

| Gold column | Silver source |
| --- | --- |
| `state_code` | hud.dim_states.state_code (2-letter USPS code, e.g. "CA") |
| `state_name` | hud.dim_states.state_name (full name, e.g. "California") |

---

### 1b. `gold_dev.geo_re.dim_county` — HUD `listCounties` API ✅ (existing)

**Purpose:** County dimension table for joins and labels — one row per US county.

**Authoritative source:** HUD FMR API `listCounties` endpoint — already ingested.

**Ingestion chain (existing, no changes needed):**

| Stage | Location | Notes |
| --- | --- | --- |
| Ingestion notebook | `src/ingestion/hud_reference_data.py` | Fetches counties for all states, saves JSON |
| Raw volume | `/Volumes/bronze_dev/hud/hud_raw/reference_data/counties_*.json` | Timestamped JSON |
| Bronze | `bronze_dev.hud.counties` | Batch `spark.read.format("json")` in bronze pipeline |
| Silver | `silver_dev.hud.dim_counties` | Materialized view — `fips_code`, `state_code`, `county_name`, `cntyname`, `category`, `town_name` |
| **Gold** | **`gold_dev.geo_re.dim_county`** | Join to `dim_states` for `state_name`, drop metadata |

| Gold column | Silver source |
| --- | --- |
| `fips_code` | hud.dim_counties.fips_code (5-digit FIPS, e.g. "06037") |
| `state_code` | hud.dim_counties.state_code |
| `state_name` | hud.dim_states.state_name (via join on `state_code`) |
| `county_name` | hud.dim_counties.county_name (e.g. "Los Angeles County") |
| `cntyname` | hud.dim_counties.cntyname (HUD compact format, e.g. "Los Angeles County, CA") |
| `category` | hud.dim_counties.category (e.g. "0" = non-metro, "1" = metro) |
| `town_name` | hud.dim_counties.town_name (populated for New England states) |

> **Note:** This supersedes the earlier `gold_dev.dim_county` proposal (see Proposed gold tables below).

---

### 1c. `gold_dev.geo_re.dim_zipcode` — NEW: HUD USPS ZIP Code Crosswalk

**Purpose:** ZIP code dimension with authoritative ZIP → county mapping — one row per ZIP code (or one row per ZIP × county where a ZIP spans multiple counties).

**Authoritative source:** HUD USPS ZIP Code Crosswalk files.

- Published quarterly by HUD, based on USPS address data — this is the authoritative federal source for ZIP code geographic mapping.
- Download page: `https://www.huduser.gov/portal/pps_crosswalk.html`
- Format: CSV/Excel files, one row per ZIP × county pair.
- Includes allocation ratios (what fraction of the ZIP's addresses fall in each county).
- No API endpoint — files are downloaded directly (same concept as existing reference data, just file download instead of API call).

**New ingestion chain (follows HUD reference data pattern):**

| Stage | Location | Notes |
| --- | --- | --- |
| Ingestion notebook | `src/ingestion/hud_zip_crosswalk.py` (NEW) | Downloads ZIP-county crosswalk file, saves to volume |
| Raw volume | `/Volumes/bronze_dev/hud/hud_raw/reference_data/zip_county_crosswalk_*.csv` | Timestamped CSV |
| Bronze | `bronze_dev.hud.zip_county_crosswalk` (NEW) | Batch `spark.read.format("csv")` in bronze pipeline |
| Silver | `silver_dev.hud.dim_zipcode` (NEW) | Materialized view — deduplicated, cleaned |
| **Gold** | **`gold_dev.geo_re.dim_zipcode`** | Direct read from silver, drop metadata columns |

**HUD USPS ZIP-county crosswalk columns:**

| Crosswalk column | Description |
| --- | --- |
| `ZIP` | 5-digit USPS ZIP code |
| `COUNTY` | 5-digit county FIPS code |
| `RES_RATIO` | Fraction of residential addresses in this ZIP that fall in this county |
| `BUS_RATIO` | Fraction of business addresses |
| `OTH_RATIO` | Fraction of other addresses |
| `TOT_RATIO` | Overall allocation ratio |

| Gold column | Source |
| --- | --- |
| `zip_code` | crosswalk ZIP (5-digit string) |
| `county_fips_code` | crosswalk COUNTY (5-digit FIPS) |
| `residential_ratio` | crosswalk RES_RATIO |
| `business_ratio` | crosswalk BUS_RATIO |
| `other_ratio` | crosswalk OTH_RATIO |
| `total_ratio` | crosswalk TOT_RATIO |

**Multi-county ZIPs:** Some ZIP codes span multiple counties. The crosswalk includes one row per ZIP × county pair. For a one-row-per-ZIP dimension, filter to `TOT_RATIO` = max per ZIP (primary county). For the full many-to-many relationship, keep all rows and join through `county_fips_code` to `dim_county`.

**Enrichment options (join from existing silver tables, not used as source):**
- `metro_name` — from `silver_dev.hud.fair_market_rents_county` (dedup to one row per ZIP)
- `redfin_metro` — from `silver_dev.redfin.housing_market_zipcode` (`region_name` = ZIP, latest period)
- `state_code` — derivable from `county_fips_code` → `dim_county` join

**Ingestion notebook sketch** (`src/ingestion/hud_zip_crosswalk.py`):

```python
# Download the latest ZIP-county crosswalk from HUD USPS crosswalk page
# URL pattern: https://www.huduser.gov/portal/datasets/usps/ZIP_COUNTY_<YYYYMM>.zip
# 1. Download ZIP file
# 2. Extract CSV
# 3. Save to /Volumes/bronze_dev/hud/hud_raw/reference_data/
#    with timestamped filename (zip_county_crosswalk_YYYYMMDD_HHMMSS.csv)
# Uses hud_common.save_to_volume() or equivalent
```

---

### 1d. `gold_dev.geo_re.dim_zcta` — NEW: Census Bureau Gazetteer Files

**Purpose:** ZIP Code Tabulation Area (ZCTA) dimension from Census Bureau reference data — one row per ZCTA.

**Authoritative source:** Census Bureau Gazetteer Files for ZCTAs.

- Published by the Census Bureau — the authoritative source for ZCTA geography.
- Download page: `https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-file2020.html`
- Format: CSV, free, no auth required.
- Provides ZCTA code, land area, water area, and internal point (lat/lon) coordinates.
- Updated with each decennial census (2020 is current).

**New ingestion chain (follows reference data pattern):**

| Stage | Location | Notes |
| --- | --- | --- |
| Ingestion notebook | `src/ingestion/census_zcta_reference.py` (NEW) | Downloads Gazetteer CSV, saves to volume |
| Raw volume | `/Volumes/bronze_dev/census_bureau/census_bureau_raw/zcta_reference/` | Timestamped CSV |
| Bronze | `bronze_dev.census_bureau.zcta_reference` (NEW) | Batch `spark.read.format("csv")` in bronze pipeline |
| Silver | `silver_dev.census_bureau.dim_zcta` (NEW) | Materialized view — cleaned column names |
| **Gold** | **`gold_dev.geo_re.dim_zcta`** | Direct read from silver, drop metadata columns |

**Census Gazetteer ZCTA columns:**

| Gazetteer column | Description |
| --- | --- |
| `GEOID` | 5-digit ZCTA code |
| `ALAND` | Land area (square meters) |
| `AWATER` | Water area (square meters) |
| `ALAND_SQMI` | Land area (square miles) |
| `AWATER_SQMI` | Water area (square miles) |
| `INTPTLAT` | Internal point latitude |
| `INTPTLONG` | Internal point longitude |

| Gold column | Source |
| --- | --- |
| `zcta_code` | Gazetteer GEOID (5-digit string) |
| `land_area_sq_miles` | Gazetteer ALAND_SQMI |
| `water_area_sq_miles` | Gazetteer AWATER_SQMI |
| `internal_point_lat` | Gazetteer INTPTLAT |
| `internal_point_lon` | Gazetteer INTPTLONG |

**ZCTA vs ZIP:** ZCTAs are Census-defined approximations of USPS ZIP codes. They don't map 1:1 — some ZIPs have no ZCTA and vice versa. In practice, `zcta_code` approximates `zip_code` for most areas. The HUD USPS ZIP crosswalk (§1c) can provide an approximate ZIP → ZCTA mapping for joins that need to cross between the two systems.

**Ingestion notebook sketch** (`src/ingestion/census_zcta_reference.py`):

```python
# Download the 2020 ZCTA Gazetteer file from Census Bureau
# URL: https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2020_Gazetteer/2020_Gaz_zcta_national.zip
# 1. Download ZIP file
# 2. Extract CSV
# 3. Save to /Volumes/bronze_dev/census_bureau/census_bureau_raw/zcta_reference/
#    with timestamped filename
# No API token needed — Census Bureau data is public
```

---

## What we have in silver

| Source | Silver table | Grain | Key columns |
| --- | --- | --- | --- |
| Redfin housing market | `silver_dev.redfin.housing_market_{county,zipcode,neighborhood}` | {county,zip,neighborhood} × period | `region_name`, `period_begin`, `period_end` |
| Redfin price drops | `silver_dev.redfin.price_drops_{county,zipcode,neighborhood}` | {county,zip,neighborhood} × period | `region_name`, `period_begin`, `period_end` |
| Redfin delistings/relistings | `silver_dev.redfin.delistings_relistings_{county,zipcode,neighborhood}` | {county,zip,neighborhood} × period | `region_name`, `period_begin`, `period_end` |
| Opportunity Insights | `silver_dev.opportunity_insights.social_capital_zip` | ZIP (static) | `zip_code` |
| Census ACS | `silver_dev.census_bureau.american_community_survey_zcta` | ZCTA (static) | `zcta_code` |
| HUD FMR | `silver_dev.hud.fair_market_rents_county` | county × ZIP × year | `county_name`, `zip_code`, `year` |

Reference tables now in silver: `silver_dev.hud.dim_states`, `silver_dev.hud.dim_counties` (batch materialized views, not streaming).

**Note:** Housing market data (sales, prices, inventory) is now available at all three grains — county, ZIP, and neighborhood (previously ZIP and neighborhood were excluded for pipeline testing).

---

## Proposed gold tables

### 1. ~~`gold_dev.dim_county`~~ → Moved to `gold_dev.geo_re.dim_county` (§1b above)

**Purpose:** County dimension table for joins and labels.

**Source:** `silver_dev.hud.dim_counties` joined to `silver_dev.hud.dim_states` on `state_code`.

> **Superseded:** This dimension has been moved to `gold_dev.geo_re.dim_county` (see §1b above). The definition there is identical but also includes `town_name`.

| Column | Source |
| --- | --- |
| `fips_code` | hud.counties.fips_code |
| `state_code` | hud.counties.state_code |
| `state_name` | hud.states.state_name |
| `county_name` | hud.counties.county_name |
| `cntyname` | hud.counties.cntyname |
| `category` | hud.counties.category |

**Transformation:** Batch read from silver dim tables. Simple left join of dim_counties to dim_states on `state_code`.

---

### 2. `gold_dev.county_housing_profile`

**Purpose:** One-stop county-level table for housing market analytics. Answers "what's the housing market like in this county?"

**Grain:** One row per county per time period.

**Joins:**

1. `silver_dev.redfin.housing_market_county` (base) — sales, prices, inventory, days on market, months of supply
2. `silver_dev.redfin.price_drops_county` — join on `region_name` + `period_begin` + `period_end`. Adds price drop activity.
3. `silver_dev.redfin.delistings_relistings_county` — same join keys. Adds delisting/relisting activity.
4. `gold_dev.geo_re.dim_county` (§1b) — join on county name (or via a county name → FIPS mapping). Adds state info.
5. HUD FMR aggregated to county × year — `AVG(one_bedroom)`, `AVG(two_bedroom)`, etc. across ZIPs within a county. Join on county name + year (matching the Redfin period's year).
6. ACS aggregated from ZCTA to county — `SUM(total_population)`, `AVG(median_age)`, `AVG(median_household_income)`, `SUM(population_below_poverty_level)`. Need a ZCTA → county crosswalk (from `social_capital_zip.county_fips_code` or HUD counties).

**Derived fields worth adding:**
- `poverty_rate` = `population_below_poverty_level` / `total_population`
- `rent_to_income_ratio` = `avg_two_bedroom_rent` / `median_household_income` (annualize: ×12 / annual income)
- `price_drop_rate` = `homes_sold_with_price_drops` / `price_drops` (if both present)

**Note:** This is the most complex gold table — it crosses 6 sources. Worth building incrementally (start with just the 3 Redfin county tables, add enrichment later).

---

### 3. `gold_dev.zip_market_health`

**Purpose:** ZIP-level market health + community context. Answers "which ZIP codes have healthy/struggling real estate markets?"

**Grain:** One row per ZIP code (snapshot) or one row per ZIP per period (time-series).

**Joins:**

1. `silver_dev.redfin.housing_market_zipcode` — base: sales, prices, inventory, days on market, months of supply at ZIP grain
2. `silver_dev.redfin.price_drops_zipcode` — join on `region_name` + `period_begin` + `period_end`. Adds price drop activity.
3. `silver_dev.redfin.delistings_relistings_zipcode` — same join keys. Adds delisting/relisting activity.
4. `silver_dev.opportunity_insights.social_capital_zip` — join on `zip_code`. Already static, no aggregation needed.
5. `silver_dev.census_bureau.american_community_survey_zcta` — join on `zcta_code` = `zip_code` (ZCTA ≈ ZIP). Already static.
6. `silver_dev.hud.fair_market_rents_county` — filter to latest year, join on `zip_code`.

**Aggregation:** The Redfin ZIP tables are time-series. For a static snapshot gold table, take the latest period (or rolling average of last 4 periods). For a time-series gold table, keep the period grain and join the static tables as slowly-changing dimensions.

**Decision point:** Snapshot (one row per ZIP, latest data) is simpler for dashboards. Time-series preserves trend analysis but is heavier. Pick one — don't try to do both.

---

### 4. `gold_dev.neighborhood_market_activity`

**Purpose:** Neighborhood-level market dynamics. Answers "how active is the market in this neighborhood?"

**Grain:** One row per neighborhood per time period.

**Joins:**

1. `silver_dev.redfin.housing_market_neighborhood` (base) — sales, prices, inventory at neighborhood grain
2. `silver_dev.redfin.price_drops_neighborhood` — join on `region_name` + `period_begin` + `period_end`
3. `silver_dev.redfin.delistings_relistings_neighborhood` — same join keys

**Note:** Now 3 Redfin sources at neighborhood grain (housing market, price drops, delistings/relistings). Still no external enrichment data at this level. Could be a view joining the three silver tables, or a gold table if derived fields are added.

---

## What's fine as-is in silver

- **`social_capital_zip`** — already at the right grain (one row per ZIP), clean column names, proper types. Use directly in gold joins.
- **`american_community_survey_zcta`** — same, already at ZCTA grain with descriptive column names. Aggregation to county is a gold concern.
- **`fair_market_rents_county`** — already at county × ZIP × year with proper types. Aggregation to county-only is a gold concern.
- **All Redfin silver tables** — already cleaned, typed, and deduplicated. Joins across them at the same grain are a gold concern.

## Reference data to promote

- **`silver_dev.hud.dim_states`** and **`silver_dev.hud.dim_counties`** — ✅ Done. Promoted to silver as batch materialized views. Use directly in gold joins.
- **HUD USPS ZIP Code Crosswalk** — NEW. Needs new ingestion notebook + bronze/silver pipeline tables. See §1c.
- **Census Bureau ZCTA Gazetteer** — NEW. Needs new ingestion notebook + bronze/silver pipeline tables. See §1d.

## Crosswalks needed

- **ZIP → county:** HUD USPS ZIP Code Crosswalk provides the authoritative mapping with allocation ratios. Built into `geo_re.dim_zipcode` (§1c). The old approach of using `social_capital_zip.county_fips_code` was derived from a fact table — the crosswalk is the proper reference source.
- **ZCTA → county:** ZCTA codes approximate ZIP codes, so the HUD USPS ZIP Crosswalk (§1c) can bridge ZCTA → ZIP → county. For direct ZCTA → county, a Census ZCTA-to-county crosswalk could be ingested later if needed.
- **County name → FIPS:** `geo_re.dim_county` (§1b) has both `county_name` and `fips_code`. Redfin uses `region_name` (county name) as the key. Need to match Redfin county names to HUD county names (watch for naming inconsistencies, e.g. "County" suffix differences).

## Suggested build order

1. ~~Promote HUD states + counties to silver~~ ✅ Done
2. Create `gold_dev.geo_re` schema
3. `geo_re.dim_state` (direct read from hud.dim_states — existing silver table)
4. `geo_re.dim_county` (join dim_counties + dim_states — existing silver tables)
5. Ingest HUD USPS ZIP Code Crosswalk → bronze → silver → `geo_re.dim_zipcode`
6. Ingest Census Bureau ZCTA Gazetteer → bronze → silver → `geo_re.dim_zcta`
7. `county_housing_profile` — ✅ Base created (`src/pipelines/real_estate_pipeline_gold.py`). Joins 3 Redfin county tables. FMR + ACS enrichment pending.
8. `zip_market_health` — ✅ Base created. Time-series (one row per ZIP per period). Snapshot can be derived via `WHERE period_begin = MAX(...)`. Social capital + ACS + FMR enrichment pending.
9. `neighborhood_market_activity` — ✅ Base created. Joins 3 Redfin neighborhood tables. No external enrichment available at this grain yet.

## Gold layer conventions

- **Drop metadata columns:** `_ingest_timestamp` and `_processed_timestamp` should not be carried into gold. They're pipeline plumbing useful in bronze/silver for debugging, but gold should expose only business-relevant columns. If a freshness indicator is needed, add a single `gold_refresh_timestamp` at the gold layer instead.
