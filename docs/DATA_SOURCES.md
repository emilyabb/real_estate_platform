
# Data Sources

General overview of the external sources that feed the bronze layer of the real estate platform. For column-level bronze → silver mappings, see `docs/DATA_DICTIONARY.md`.

Source locations, keys, and bronze table names are configured centrally in `src/utils/sources_ref.py`; `src/pipelines/real_estate_pipeline_bronze.py` reads those configs and creates the bronze tables (Auto Loader for streaming sources, batch reads for static reference data).

---

## Redfin

Monthly housing market data from the Redfin Data Center — three metrics at three grains (county, ZIP code, neighborhood) for nine tables total.

- **Raw files:** `/Volumes/bronze_dev/redfin/redfin_raw/<table_name>/` — one directory per table (e.g. `housing_market_county`, `price_drops_zipcode`)
- **Ingestion:** `src/ingestion/redfin.ipynb`
- **Bronze tables:** `bronze_dev.redfin.{housing_market, price_drops, delistings_relistings}_{county, zipcode, neighborhood}` (9 tables)
- **Natural key:** `PERIOD BEGIN`, `PERIOD END`, `REGION NAME`

## Opportunity Insights — Social Capital

- **Upstream:** `social_capital_zip.csv` from Opportunity Insights, via the HDX data portal (https://data.humdata.org/dataset/85ee8e10-0c66-4635-b997-79b6fad44c71/resource/ab878625-279b-4bef-a2b3-c132168d536e/download/social_capital_zip.csv)
- **Raw files:** `/Volumes/bronze_dev/opportunity_insights/opportunity_insights_raw/social_capital_zip/`
- **Ingestion:** `src/ingestion/others.ipynb`
- **Bronze table:** `bronze_dev.opportunity_insights.social_capital_zip`
- **Natural key:** `zip`

## Census Bureau — American Community Survey (ACS 5-year)

- **Upstream:** Census API `https://api.census.gov/data/2023/acs/acs5` (requires an API key; stored in the Databricks `api-secrets` secret scope)
- **Raw files:** `/Volumes/bronze_dev/census_bureau/census_bureau_raw/american_community_survey/` (JSON, list-of-lists format) - one shared directory for all three grains; files land as `american_community_survey_{grain}_{timestamp}.json` (zcta, county, state). Legacy files named `american_community_survey_{timestamp}.json` (and older misnamed `social_capital_zip_{timestamp}.json`) are still ingested by the zcta bronze table.
- **Ingestion:** `src/ingestion/others.ipynb` (`fetch_census_bureau_acs` with a `grain` parameter)
- **Bronze tables:** `bronze_dev.census_bureau.american_community_survey_{zcta, county, state}` - one bronze table per grain; the raw JSON column layout differs per grain, so each bronze table filters the shared directory to its own grain's files
- **Natural key:** zcta: `zip`; county: `state` + `county`; state: `state`

## HUD

- **States & counties (static reference data)**
  - Raw files: `/Volumes/bronze_dev/hud/hud_raw/reference_data/states_*.json`, `counties_*.json`
  - Ingestion: `src/ingestion/hud_reference_data.py`
  - Bronze tables: `bronze_dev.hud.states`, `bronze_dev.hud.counties`
- **Fair Market Rents (county / MSA / ZIP levels)**
  - Raw files: `/Volumes/bronze_dev/hud/hud_raw/fair_market_rents/fmr_data_*.json`
  - Ingestion: `src/ingestion/hud_fmr_ingestion.py`
  - Bronze table: `bronze_dev.hud.fair_market_rents` (geography-neutral - the raw data holds county-, MSA-, and ZIP-level rows in one file)
  - Silver tables: `silver_dev.hud.fair_market_rents_{county, msa, zipcode}` - one per geographic level; `metro_status` is the level discriminator
  - Natural key (bronze): `county_name`, `zip_code`, `year`; silver dedup keys per level: county `county_name` + `year`, MSA `metro_name` + `year`, ZIP `zip_code` + `year`
- **USPS ZIP Code Crosswalk (future addition — see the README)**
  - Upstream: HUD USPS ZIP Code Crosswalk files (https://www.huduser.gov/portal/pps_crosswalk.html)
  - Ingestion: `src/ingestion/hud_zip_crosswalk.ipynb` (built, not yet run — blocked on the HUD site)

## USPS ZIP Codes (static reference data)

- **Role:** ZIP code reference dimension — source for `gold_dev.geo_re.dim_zipcode` (see `_temp_utilities/GOLD_PLAN.md` §1c)
- **Upstream:** `zip_code_database.csv` (https://raw.githubusercontent.com/seanpianka/zipcodes/refs/heads/master/scripts/data/zip_code_database.csv)
- **Raw files:** `/Volumes/bronze_dev/geo_re/geo_re_raw/usps_zip_codes/`
- **Ingestion:** `src/ingestion/others.ipynb`
- **Bronze table:** `bronze_dev.geo_re.usps_zip_codes`
- **Natural key:** `zip` (INT in bronze — cast to string and zero-pad to 5 digits for ZIP-code joins)

---

## References

- ACS technical documentation: https://www.census.gov/programs-surveys/acs/technical-documentation.html
- Census API discovery tool: https://www.census.gov/data/developers/updates/new-discovery-tool.html
- Census available data sets: https://www.census.gov/data/developers/data-sets.html