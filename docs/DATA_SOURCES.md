
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
- **Raw files:** `/Volumes/bronze_dev/census_bureau/census_bureau_raw/american_community_survey/` (JSON, list-of-lists format)
- **Ingestion:** `src/ingestion/others.ipynb`
- **Bronze table:** `bronze_dev.census_bureau.american_community_survey_zcta`
- **Natural key:** `zip` (ZCTA)

## HUD

- **States & counties (static reference data)**
  - Raw files: `/Volumes/bronze_dev/hud/hud_raw/reference_data/states_*.json`, `counties_*.json`
  - Ingestion: `src/ingestion/hud_reference_data.py`
  - Bronze tables: `bronze_dev.hud.states`, `bronze_dev.hud.counties`
- **Fair Market Rents (county × ZIP code)**
  - Raw files: `/Volumes/bronze_dev/hud/hud_raw/fair_market_rents_county/fmr_data_*.json`
  - Ingestion: `src/ingestion/hud_fmr_ingestion.py`
  - Bronze table: `bronze_dev.hud.fair_market_rents_county`
  - Natural key: `county_name`, `zip_code`, `year`
- **USPS ZIP Code Crosswalk (in progress)**
  - Upstream: HUD USPS ZIP Code Crosswalk files (https://www.huduser.gov/portal/pps_crosswalk.html)
  - Ingestion: `src/ingestion/hud_zip_crosswalk.ipynb`

## USPS ZIP Codes (static reference data)

- **Upstream:** `zip_code_database.csv` (https://raw.githubusercontent.com/seanpianka/zipcodes/refs/heads/master/scripts/data/zip_code_database.csv)
- **Raw files:** `/Volumes/bronze_dev/geo_re/geo_re_raw/usps_zip_codes/`
- **Ingestion:** `src/ingestion/others.ipynb`
- **Bronze table:** `bronze_dev.geo_re.usps_zip_codes`
- **Natural key:** `zip_code`

---

## Planned sources

- **Census Bureau ZCTA Gazetteer files** (https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-file2020.html) — free CSV, no auth; for the `dim_zcta` table. See `docs/GOLD_PLAN.md`.

## References

- ACS technical documentation: https://www.census.gov/programs-surveys/acs/technical-documentation.html
- Census API discovery tool: https://www.census.gov/data/developers/updates/new-discovery-tool.html
- Census available data sets: https://www.census.gov/data/developers/data-sets.html