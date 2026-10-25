# Data Dictionary: Bronze → Silver Column Mapping

This document maps every column from the bronze layer to its corresponding silver layer column for each table in the real estate platform pipeline. Every bronze column has a silver counterpart; some silver tables add metadata columns (e.g., `_processed_timestamp`) that do not exist in bronze.

---

## Table of Contents

1. [Redfin: common columns (all nine tables)](#1-redfin-common-columns-all-nine-tables)
    - [1a. Redfin: housing_market (unique columns)](#1a-redfin-housing_market-unique-columns)
    - [1b. Redfin: price_drops (unique columns)](#1b-redfin-price_drops-unique-columns)
    - [1c. Redfin: delistings_relistings (unique columns)](#1c-redfin-delistings_relistings-unique-columns)
4. [Opportunity Insights: social_capital_zip](#4-opportunity-insights-social_capital_zip)
5. [Census Bureau: american_community_survey_zcta](#5-census-bureau-american_community_survey_zcta)
6. [HUD: fair_market_rents_county](#6-hud-fair_market_rents_county)
7. [Gold Layer: Fact Tables](#7-gold-layer-fact-tables)
    - [7a. county_housing_profile](#7a-county_housing_profile)
    - [7b. zip_market_health](#7b-zip_market_health)
    - [7c. neighborhood_market_activity](#7c-neighborhood_market_activity)

---

## 1. Redfin: common columns (all nine tables)

**Bronze tables:** `bronze_dev.redfin.{housing_market,price_drops,delistings_relistings}_{county,zipcode,neighborhood}`
**Silver tables:** `silver_dev.redfin.{housing_market,price_drops,delistings_relistings}_{county,zipcode,neighborhood}`
**Grain:** One row per region (county / ZIP code / neighborhood) per time period (`period_begin` × `period_end`)

The `region_type` column discriminates grains (`'County'`, `'Zip'`, `'Neighborhood'`). The `region_name` column is `STRING` for county and neighborhood tables, `INT` for the zipcode table.

Silver transformations: column names standardized to lowercase snake_case; `"NA"` strings replaced with null; date columns cast to DateType; PPTS columns divided by 100; all other non-string columns cast to DoubleType.

The following columns are common to all nine Redfin tables (three table types × three grains):

| Bronze Column | Silver Column | Notes |
| --- | --- | --- |
| `LAST UPDATED` | `last_updated` | Cast to DateType |
| `FREQUENCY` | `frequency` | Kept as string |
| `PERIOD BEGIN` | `period_begin` | Cast to DateType |
| `PERIOD END` | `period_end` | Cast to DateType |
| `REGION ID` | `region_id` | Kept as string |
| `REGION TYPE` | `region_type` | Kept as string |
| `REGION NAME` | `region_name` | Kept as string (INT for zipcode tables) |
| `METRO` | `metro` | Kept as string |
| `_ingest_timestamp` | `_ingest_timestamp` | Metadata, unchanged |
| `_source_file` | `_source_file` | Metadata, unchanged |
| `_rescued_data` | `_rescued_data` | Auto Loader metadata, unchanged |
| *(none)* | `_processed_timestamp` | **Silver only** — added via `F.current_timestamp()` |

The columns unique to each table type are listed in the subsections below.

---

## 1a. Redfin: housing_market (unique columns)

**Bronze tables:** 
* `bronze_dev.redfin.housing_market_county` 
* `bronze_dev.redfin.housing_market_zipcode`
* `bronze_dev.redfin.housing_market_neighborhood`

**Silver tables:** 
* `silver_dev.redfin.housing_market_county` 
* `silver_dev.redfin.housing_market_zipcode`
* `silver_dev.redfin.housing_market_neighborhood`

| Bronze Column | Silver Column | Notes |
| --- | --- | --- |
| `HOMES SOLD` | `homes_sold` | Cast to DoubleType |
| `HOMES SOLD MOM (%)` | `homes_sold_mom` | Cast to DoubleType |
| `HOMES SOLD YOY (%)` | `homes_sold_yoy` | Cast to DoubleType |
| `MEDIAN SALE PRICE NSA ($)` | `median_sale_price_nsa` | Cast to DoubleType |
| `MEDIAN SALE PRICE NSA MOM (%)` | `median_sale_price_nsa_mom` | Cast to DoubleType |
| `MEDIAN SALE PRICE NSA YOY (%)` | `median_sale_price_nsa_yoy` | Cast to DoubleType |
| `MEDIAN DAYS ON MARKET (DAYS)` | `median_days_on_market_days` | Cast to DoubleType |
| `MEDIAN DAYS ON MARKET MOM (%)` | `median_days_on_market_mom` | Cast to DoubleType |
| `MEDIAN DAYS ON MARKET YOY (%)` | `median_days_on_market_yoy` | Cast to DoubleType |
| `AVERAGE SALE TO LIST RATIO (%)` | `average_sale_to_list_ratio` | Cast to DoubleType |
| `AVERAGE SALE TO LIST RATIO MOM (PPTS)` | `average_sale_to_list_ratio_mom_ppts` | Divided by 100, cast to DoubleType |
| `AVERAGE SALE TO LIST RATIO YOY (PPTS)` | `average_sale_to_list_ratio_yoy_ppts` | Divided by 100, cast to DoubleType |
| `SHARE SOLD ABOVE ORIGINAL LIST (%)` | `share_sold_above_original_list` | Cast to DoubleType |
| `SHARE SOLD ABOVE ORIGINAL LIST MOM (PPTS)` | `share_sold_above_original_list_mom_ppts` | Divided by 100, cast to DoubleType |
| `SHARE SOLD ABOVE ORIGINAL LIST YOY (PPTS)` | `share_sold_above_original_list_yoy_ppts` | Divided by 100, cast to DoubleType |
| `NEW LISTINGS` | `new_listings` | Cast to DoubleType |
| `NEW LISTINGS MOM (%)` | `new_listings_mom` | Cast to DoubleType |
| `NEW LISTINGS YOY (%)` | `new_listings_yoy` | Cast to DoubleType |
| `ACTIVE LISTINGS` | `active_listings` | Cast to DoubleType |
| `ACTIVE LISTINGS MOM (%)` | `active_listings_mom` | Cast to DoubleType |
| `ACTIVE LISTINGS YOY (%)` | `active_listings_yoy` | Cast to DoubleType |
| `INVENTORY` | `inventory` | Cast to DoubleType |
| `INVENTORY MOM (%)` | `inventory_mom` | Cast to DoubleType |
| `INVENTORY YOY (%)` | `inventory_yoy` | Cast to DoubleType |
| `PENDING SALES` | `pending_sales` | Cast to DoubleType |
| `PENDING SALES MOM (%)` | `pending_sales_mom` | Cast to DoubleType |
| `PENDING SALES YOY (%)` | `pending_sales_yoy` | Cast to DoubleType |
| `MEDIAN NEW LISTING PRICE ($)` | `median_new_listing_price` | Cast to DoubleType |
| `MEDIAN NEW LISTING PRICE MOM (%)` | `median_new_listing_price_mom` | Cast to DoubleType |
| `MEDIAN NEW LISTING PRICE YOY (%)` | `median_new_listing_price_yoy` | Cast to DoubleType |
| `MEDIAN NEW LISTING PRICE PER SQ.FT. ($)` | `median_new_listing_price_per_sq_ft` | Cast to DoubleType |
| `MEDIAN NEW LISTING PRICE PER SQ.FT. MOM (%)` | `median_new_listing_price_per_sq_ft_mom` | Cast to DoubleType |
| `MEDIAN NEW LISTING PRICE PER SQ.FT. YOY (%)` | `median_new_listing_price_per_sq_ft_yoy` | Cast to DoubleType |
| `MEDIAN SALE PRICE PER SQ.FT. ($)` | `median_sale_price_per_sq_ft` | Cast to DoubleType |
| `MEDIAN SALE PRICE PER SQ.FT. MOM (%)` | `median_sale_price_per_sq_ft_mom` | Cast to DoubleType |
| `MEDIAN SALE PRICE PER SQ.FT. YOY (%)` | `median_sale_price_per_sq_ft_yoy` | Cast to DoubleType |
| `MONTHS OF SUPPLY` | `months_of_supply` | Cast to DoubleType |
| `MONTHS OF SUPPLY MOM (%)` | `months_of_supply_mom` | Cast to DoubleType |
| `MONTHS OF SUPPLY YOY (%)` | `months_of_supply_yoy` | Cast to DoubleType |
| `PERCENT OFF MARKET IN TWO WEEKS (%)` | `percent_off_market_in_two_weeks` | Cast to DoubleType |
| `PERCENT OFF MARKET IN TWO WEEKS MOM (PPTS)` | `percent_off_market_in_two_weeks_mom_ppts` | Divided by 100, cast to DoubleType |
| `PERCENT OFF MARKET IN TWO WEEKS YOY (PPTS)` | `percent_off_market_in_two_weeks_yoy_ppts` | Divided by 100, cast to DoubleType |

---

## 1b. Redfin: price_drops (unique columns)

**Bronze tables:** `bronze_dev.redfin.price_drops_county`, `bronze_dev.redfin.price_drops_zipcode`, `bronze_dev.redfin.price_drops_neighborhood`
**Silver tables:** `silver_dev.redfin.price_drops_county`, `silver_dev.redfin.price_drops_zipcode`, `silver_dev.redfin.price_drops_neighborhood`

| Bronze Column | Silver Column | Notes |
| --- | --- | --- |
| `PRICE DROPS` | `price_drops` | Cast to DoubleType |
| `PRICE DROPS MOM (%)` | `price_drops_mom` | Cast to DoubleType |
| `PRICE DROPS YOY (%)` | `price_drops_yoy` | Cast to DoubleType |
| `AVERAGE SIZE OF PRICE DROP (%)` | `average_size_of_price_drop` | Cast to DoubleType |
| `AVERAGE SIZE OF PRICE DROP MOM (PPTS)` | `average_size_of_price_drop_mom_ppts` | Divided by 100, cast to DoubleType |
| `AVERAGE SIZE OF PRICE DROP YOY (PPTS)` | `average_size_of_price_drop_yoy_ppts` | Divided by 100, cast to DoubleType |
| `PERCENT ACTIVE WITH PRICE DROPS (%)` | `percent_active_with_price_drops` | Cast to DoubleType |
| `PERCENT ACTIVE WITH PRICE DROPS MOM (PPTS)` | `percent_active_with_price_drops_mom_ppts` | Divided by 100, cast to DoubleType |
| `PERCENT ACTIVE WITH PRICE DROPS YOY (PPTS)` | `percent_active_with_price_drops_yoy_ppts` | Divided by 100, cast to DoubleType |
| `HOMES SOLD WITH PRICE DROPS` | `homes_sold_with_price_drops` | Cast to DoubleType |
| `HOMES SOLD WITH PRICE DROPS MOM (%)` | `homes_sold_with_price_drops_mom` | Cast to DoubleType |
| `HOMES SOLD WITH PRICE DROPS YOY (%)` | `homes_sold_with_price_drops_yoy` | Cast to DoubleType |

---

## 1c. Redfin: delistings_relistings (unique columns)

**Bronze tables:** `bronze_dev.redfin.delistings_relistings_county`, `bronze_dev.redfin.delistings_relistings_zipcode`, `bronze_dev.redfin.delistings_relistings_neighborhood`
**Silver tables:** `silver_dev.redfin.delistings_relistings_county`, `silver_dev.redfin.delistings_relistings_zipcode`, `silver_dev.redfin.delistings_relistings_neighborhood`

| Bronze Column | Silver Column | Notes |
| --- | --- | --- |
| `TOTAL DELISTINGS` | `total_delistings` | Cast to DoubleType |
| `TOTAL DELISTINGS MOM (%)` | `total_delistings_mom` | Cast to DoubleType |
| `TOTAL DELISTINGS YOY (%)` | `total_delistings_yoy` | Cast to DoubleType |
| `TOTAL RELISTINGS` | `total_relistings` | Cast to DoubleType |
| `TOTAL RELISTINGS MOM (%)` | `total_relistings_mom` | Cast to DoubleType |
| `TOTAL RELISTINGS YOY (%)` | `total_relistings_yoy` | Cast to DoubleType |
| `SHARE OF LISTINGS DELISTED (%)` | `share_of_listings_delisted` | Cast to DoubleType |
| `SHARE OF LISTINGS DELISTED MOM (PPTS)` | `share_of_listings_delisted_mom_ppts` | Divided by 100, cast to DoubleType |
| `SHARE OF LISTINGS DELISTED YOY (PPTS)` | `share_of_listings_delisted_yoy_ppts` | Divided by 100, cast to DoubleType |
| `SHARE OF LISTINGS RELISTED (%)` | `share_of_listings_relisted` | Cast to DoubleType |
| `SHARE OF LISTINGS RELISTED MOM (PPTS)` | `share_of_listings_relisted_mom_ppts` | Divided by 100, cast to DoubleType |
| `SHARE OF LISTINGS RELISTED YOY (PPTS)` | `share_of_listings_relisted_yoy_ppts` | Divided by 100, cast to DoubleType |

---

## 4. Opportunity Insights: social_capital_zip

**Bronze table:** `bronze_dev.opportunity_insights.social_capital_zip`
**Silver table:** `silver_dev.opportunity_insights.social_capital_zip`
**Grain:** One row per ZIP code

Silver transformations: columns renamed to descriptive names; `zip` and `county` cast to string (FIPS codes preserve leading zeros); numeric columns kept as-is; deduplicated by `zip_code`.

| Bronze Column | Silver Column | Notes |
| --- | --- | --- |
| `zip` | `zip_code` | Cast to string |
| `county` | `county_fips_code` | Cast to string |
| `num_below_p50` | `children_below_median_parental_income_count` | Renamed |
| `pop2018` | `population_2018` | Renamed |
| `ec_zip` | `economic_connectedness_low_ses` | Renamed |
| `ec_se_zip` | `economic_connectedness_low_ses_standard_error` | Renamed |
| `nbhd_ec_zip` | `neighborhood_economic_connectedness_low_ses` | Renamed |
| `ec_grp_mem_zip` | `group_economic_connectedness_low_ses` | Renamed |
| `ec_high_zip` | `economic_connectedness_high_ses` | Renamed |
| `ec_high_se_zip` | `economic_connectedness_high_ses_standard_error` | Renamed |
| `nbhd_ec_high_zip` | `neighborhood_economic_connectedness_high_ses` | Renamed |
| `ec_grp_mem_high_zip` | `group_economic_connectedness_high_ses` | Renamed |
| `exposure_grp_mem_zip` | `group_exposure_to_high_ses_low_ses` | Renamed |
| `exposure_grp_mem_high_zip` | `group_exposure_to_high_ses_high_ses` | Renamed |
| `nbhd_exposure_zip` | `neighborhood_exposure_to_high_ses` | Renamed |
| `bias_grp_mem_zip` | `group_friending_bias_low_ses` | Renamed |
| `bias_grp_mem_high_zip` | `group_friending_bias_high_ses` | Renamed |
| `nbhd_bias_zip` | `neighborhood_friending_bias_low_ses` | Renamed |
| `nbhd_bias_high_zip` | `neighborhood_friending_bias_high_ses` | Renamed |
| `clustering_zip` | `friend_network_clustering` | Renamed |
| `support_ratio_zip` | `within_zcta_friendship_support_ratio` | Renamed |
| `volunteering_rate_zip` | `volunteering_group_membership_rate` | Renamed |
| `civic_organizations_zip` | `civic_organizations_per_1000_users` | Renamed |
| `_rescued_data` | `_rescued_data` | Auto Loader metadata, unchanged |
| `_ingest_timestamp` | `_ingest_timestamp` | Metadata, unchanged |
| `_source_file` | `_source_file` | Metadata, unchanged |
| *(none)* | `_processed_timestamp` | **Silver only** — added via `F.current_timestamp()` |

---

## 5. Census Bureau: american_community_survey_zcta

**Bronze table:** `bronze_dev.census_bureau.american_community_survey_zcta`
**Silver table:** `silver_dev.census_bureau.american_community_survey_zcta`
**Grain:** One row per ZCTA (ZIP Code Tabulation Area)

Silver transformations: Census variable codes renamed to descriptive names; `NAME` and `zip` cast to string (ZCTA codes preserve leading zeros); demographic variables cast to int or double.

| Bronze Column | Silver Column | Notes |
| --- | --- | --- |
| `NAME` | `zcta_name` | Cast to string, renamed |
| `zip` | `zcta_code` | Cast to string, renamed |
| `B01003_001E` | `total_population` | Cast to int, renamed |
| `B01002_001E` | `median_age` | Cast to double, renamed |
| `B19013_001E` | `median_household_income` | Cast to int, renamed |
| `B17001_002E` | `population_below_poverty_level` | Cast to int, renamed |
| `B02001_002E` | `white_alone_population` | Cast to int, renamed |
| `B02001_003E` | `black_or_african_american_alone_population` | Cast to int, renamed |
| `B02001_005E` | `asian_alone_population` | Cast to int, renamed |
| `B03003_003E` | `hispanic_or_latino_population` | Cast to int, renamed |
| `_ingest_timestamp` | `_ingest_timestamp` | Metadata, unchanged |
| `_source_file` | `_source_file` | Metadata, unchanged |
| *(none)* | `_processed_timestamp` | **Silver only** — added via `F.current_timestamp()` |

---

## 6. HUD: fair_market_rents_county

**Bronze table:** `bronze_dev.hud.fair_market_rents_county`
**Silver table:** `silver_dev.hud.fair_market_rents_county`
**Grain:** One row per county × ZIP code × year

Silver transformations: `year` cast from string to int; all other columns kept as-is; deduplicated by `county_name`, `zip_code`, `year`.

| Bronze Column | Silver Column | Notes |
| --- | --- | --- |
| `county_name` | `county_name` | Unchanged |
| `metro_name` | `metro_name` | Unchanged |
| `area_name` | `area_name` | Unchanged |
| `metro_status` | `metro_status` | Unchanged |
| `smallarea_status` | `smallarea_status` | Unchanged |
| `year` | `year` | Cast to int |
| `zip_code` | `zip_code` | Unchanged |
| `efficiency` | `efficiency` | Unchanged |
| `one_bedroom` | `one_bedroom` | Unchanged |
| `two_bedroom` | `two_bedroom` | Unchanged |
| `three_bedroom` | `three_bedroom` | Unchanged |
| `four_bedroom` | `four_bedroom` | Unchanged |
| `source_url` | `source_url` | Unchanged |
| `_ingest_timestamp` | `_ingest_timestamp` | Metadata, unchanged |
| `_source_file` | `_source_file` | Metadata, unchanged |
| *(none)* | `_processed_timestamp` | **Silver only** — added via `F.current_timestamp()` |

---

## 7. Gold Layer: Fact Tables

**Pipeline file:** `src/pipelines/real_estate_pipeline_gold.py`

**Pattern:** Batch reads (`spark.read.table()`) from silver → materialized views. Each gold fact table joins three Redfin silver tables at the same grain: `housing_market` (base) left-joined with `price_drops` and `delistings_relistings` on `region_name` + `period_begin` + `period_end`.

**Gold transformations:**
- Pipeline metadata columns dropped (`rescued_data`, `ingest_timestamp`, `source_file`, `_processed_timestamp`) — gold exposes only business-relevant columns.
- Common columns (`last_updated`, `frequency`, `region_type`, `metro`) retained from the base `housing_market` table only; enrichment tables skip these to avoid conflicts.
- `gold_refresh_timestamp` added via `F.current_timestamp()` for freshness tracking.

**Future enrichment (not yet included):**
- `county_housing_profile`: `geo_re.dim_county` (state/FIPS), HUD FMR (rental context), ACS (demographics)
- `zip_market_health`: `social_capital_zip` (opportunity metrics), ACS ZCTA (demographics), HUD FMR (rental context)
- `neighborhood_market_activity`: no external enrichment sources available at this grain

---

### 7a. county_housing_profile

**Gold table:** `gold_dev.default.county_housing_profile`
**Grain:** One row per county per time period (`region_name` × `period_begin` × `period_end`)
**Row count:** ~529,000

**Business rationale:** Provides a single, analytics-ready county-level housing market view by combining Redfin sales activity (prices, inventory, days on market) with price drop trends and delisting/relisting behavior. Designed for county-level market analysis, trend comparison across regions, and dashboard reporting. Will be enriched with HUD fair market rents and ACS demographics in a future iteration to support affordability and demographic context analysis.

**Source tables:**

| Silver Source | Join Type | Role |
| --- | --- | --- |
| `silver_dev.redfin.housing_market_county` | base | Sales, prices, inventory, days on market |
| `silver_dev.redfin.price_drops_county` | left join | Price drop activity |
| `silver_dev.redfin.delistings_relistings_county` | left join | Delisting/relisting activity |

**Columns (from housing_market_county — base):**

| Silver Column | Type | Notes |
| --- | --- | --- |
| `last_updated` | date | Redfin data freshness |
| `frequency` | string | Reporting frequency |
| `period_begin` | date | Join key |
| `period_end` | date | Join key |
| `region_id` | int | Redfin region identifier |
| `region_type` | string | Always `'County'` |
| `region_name` | string | Join key — county name (e.g., "Adams County, PA") |
| `metro` | string | Metro area name |
| `homes_sold` | double | Homes sold in period |
| `homes_sold_mom` | double | Month-over-month change |
| `homes_sold_yoy` | double | Year-over-year change |
| `median_sale_price_nsa` | double | Median sale price (not seasonally adjusted) |
| `median_sale_price_nsa_mom` | double | MoM change |
| `median_sale_price_nsa_yoy` | double | YoY change |
| `median_days_on_market_days` | double | Median days on market |
| `median_days_on_market_mom` | double | MoM change |
| `median_days_on_market_yoy` | double | YoY change |
| `average_sale_to_list_ratio` | double | Sale-to-list ratio |
| `average_sale_to_list_ratio_mom_ppts` | double | MoM change (percentage points) |
| `average_sale_to_list_ratio_yoy_ppts` | double | YoY change (percentage points) |
| `share_sold_above_original_list` | double | Share sold above list price |
| `share_sold_above_original_list_mom_ppts` | double | MoM change |
| `share_sold_above_original_list_yoy_ppts` | double | YoY change |
| `new_listings` | double | New listings in period |
| `new_listings_mom` | double | MoM change |
| `new_listings_yoy` | double | YoY change |
| `active_listings` | double | Active listings |
| `active_listings_mom` | double | MoM change |
| `active_listings_yoy` | double | YoY change |
| `inventory` | double | Housing inventory |
| `inventory_mom` | double | MoM change |
| `inventory_yoy` | double | YoY change |
| `pending_sales` | double | Pending sales |
| `pending_sales_mom` | double | MoM change |
| `pending_sales_yoy` | double | YoY change |
| `median_new_listing_price` | double | Median new listing price |
| `median_new_listing_price_mom` | double | MoM change |
| `median_new_listing_price_yoy` | double | YoY change |
| `median_new_listing_price_per_sq_ft` | double | Median new listing price per sq ft |
| `median_new_listing_price_per_sq_ft_mom` | double | MoM change |
| `median_new_listing_price_per_sq_ft_yoy` | double | YoY change |
| `median_sale_price_per_sq_ft` | double | Median sale price per sq ft |
| `median_sale_price_per_sq_ft_mom` | double | MoM change |
| `median_sale_price_per_sq_ft_yoy` | double | YoY change |
| `months_of_supply` | double | Months of housing supply |
| `months_of_supply_mom` | double | MoM change |
| `months_of_supply_yoy` | double | YoY change |
| `percent_off_market_in_two_weeks` | double | % off market in 2 weeks |
| `percent_off_market_in_two_weeks_mom_ppts` | double | MoM change |
| `percent_off_market_in_two_weeks_yoy_ppts` | double | YoY change |

**Columns (from price_drops_county — left join):**

| Silver Column | Type | Notes |
| --- | --- | --- |
| `price_drops` | double | Number of price drops |
| `price_drops_mom` | double | MoM change |
| `price_drops_yoy` | double | YoY change |
| `average_size_of_price_drop` | double | Avg % reduction |
| `average_size_of_price_drop_mom_ppts` | double | MoM change |
| `average_size_of_price_drop_yoy_ppts` | double | YoY change |
| `percent_active_with_price_drops` | double | % active listings with price drops |
| `percent_active_with_price_drops_mom_ppts` | double | MoM change |
| `percent_active_with_price_drops_yoy_ppts` | double | YoY change |
| `homes_sold_with_price_drops` | double | Homes sold after price drop |
| `homes_sold_with_price_drops_mom` | double | MoM change |
| `homes_sold_with_price_drops_yoy` | double | YoY change |

**Columns (from delistings_relistings_county — left join):**

| Silver Column | Type | Notes |
| --- | --- | --- |
| `total_delistings` | double | Total delistings in period |
| `total_delistings_mom` | double | MoM change |
| `total_delistings_yoy` | double | YoY change |
| `total_relistings` | double | Total relistings in period |
| `total_relistings_mom` | double | MoM change |
| `total_relistings_yoy` | double | YoY change |
| `share_of_listings_delisted` | double | Share of listings delisted |
| `share_of_listings_delisted_mom_ppts` | double | MoM change |
| `share_of_listings_delisted_yoy_ppts` | double | YoY change |
| `share_of_listings_relisted` | double | Share of listings relisted |
| `share_of_listings_relisted_mom_ppts` | double | MoM change |
| `share_of_listings_relisted_yoy_ppts` | double | YoY change |

**Gold-added column:**

| Gold Column | Type | Notes |
| --- | --- | --- |
| `gold_refresh_timestamp` | timestamp | Pipeline refresh time, added via `F.current_timestamp()` |

---

### 7b. zip_market_health

**Gold table:** `gold_dev.default.zip_market_health`
**Grain:** One row per ZIP code per time period (`region_name` × `period_begin` × `period_end`) — time-series
**Row count:** ~4.9 million

**Business rationale:** Delivers a comprehensive ZIP-level market health view combining Redfin sales metrics with price drop and delisting/relisting signals. Built as time-series to preserve trend analysis; dashboards needing a single-row-per-ZIP snapshot can filter to the latest period (`WHERE period_begin = (SELECT MAX(period_begin) FROM gold_dev.default.zip_market_health)`). Will be enriched with Opportunity Insights social capital data, Census ACS demographics, and HUD fair market rents to support neighborhood-level affordability and opportunity analysis.

**Source tables:**

| Silver Source | Join Type | Role |
| --- | --- | --- |
| `silver_dev.redfin.housing_market_zipcode` | base | Sales, prices, inventory, days on market |
| `silver_dev.redfin.price_drops_zipcode` | left join | Price drop activity |
| `silver_dev.redfin.delistings_relistings_zipcode` | left join | Delisting/relisting activity |

**Columns:** Same structure as `county_housing_profile` (§7a) — all housing market, price drop, and delisting/relisting columns from the three Redfin zipcode silver tables, plus `gold_refresh_timestamp`.

---

### 7c. neighborhood_market_activity

**Gold table:** `gold_dev.default.neighborhood_market_activity`
**Grain:** One row per neighborhood per time period (`region_name` × `period_begin` × `period_end`)
**Row count:** ~221 million

**Business rationale:** Provides the most granular Redfin housing market view by combining sales activity, price drops, and delisting/relisting behavior at the neighborhood level. Enables hyperlocal market trend analysis and micro-market identification. No external enrichment sources are currently available at the neighborhood grain — this table stands alone as the complete neighborhood-level market picture.

**Source tables:**

| Silver Source | Join Type | Role |
| --- | --- | --- |
| `silver_dev.redfin.housing_market_neighborhood` | base | Sales, prices, inventory, days on market |
| `silver_dev.redfin.price_drops_neighborhood` | left join | Price drop activity |
| `silver_dev.redfin.delistings_relistings_neighborhood` | left join | Delisting/relisting activity |

**Columns:** Same structure as `county_housing_profile` (§7a) — all housing market, price drop, and delisting/relisting columns from the three Redfin neighborhood silver tables, plus `gold_refresh_timestamp`.
