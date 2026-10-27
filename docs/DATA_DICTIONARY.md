# Data Dictionary: Silver and Gold Table Columns

Column-level reference for every silver and gold table in the real estate platform pipeline. Each table section lists column names with descriptions and a link to the upstream data source.

---

## Table of Contents

1. [Redfin: common columns (all nine tables)](#1-redfin-common-columns-all-nine-tables)
    - [1a. Redfin: housing_market (unique columns)](#1a-redfin-housing_market-unique-columns)
    - [1b. Redfin: price_drops (unique columns)](#1b-redfin-price_drops-unique-columns)
    - [1c. Redfin: delistings_relistings (unique columns)](#1c-redfin-delistings_relistings-unique-columns)
2. [HUD: dim_states (reference dimension)](#2-hud-dim_states-reference-dimension)
3. [HUD: dim_counties (reference dimension)](#3-hud-dim_counties-reference-dimension)
4. [Opportunity Insights: social_capital_zip](#4-opportunity-insights-social_capital_zip)
5. [Census Bureau: american_community_survey_zcta](#5-census-bureau-american_community_survey_zcta)
6. [HUD: fair_market_rents_county](#6-hud-fair_market_rents_county)
7. [Gold Layer: Fact Tables](#7-gold-layer-fact-tables)
    - [7a. county_housing_profile](#7a-county_housing_profile)
    - [7b. zip_market_health](#7b-zip_market_health)
    - [7c. neighborhood_market_activity](#7c-neighborhood_market_activity)

---

## 1. Redfin: common columns (all nine tables)

**Source:** [Redfin Data Center](https://www.redfin.com/news/data-center/)

**Bronze tables:** `bronze_dev.redfin.{housing_market,price_drops,delistings_relistings}_{county,zipcode,neighborhood}`

**Silver tables:** `silver_dev.redfin.{housing_market,price_drops,delistings_relistings}_{county,zipcode,neighborhood}`

**Grain:** One row per region (county / ZIP code / neighborhood) per time period (`period_begin` × `period_end`)

The `region_type` column discriminates grains (`'County'`, `'Zip'`, `'Neighborhood'`). The `region_name` column is `STRING` for all tables. For county and neighborhood tables it holds the region name as-is; for the zipcode tables it is cast from bronze `INT` to string and left-padded with zeros to 5 digits (e.g. `2557` → `"02557"`) to preserve leading zeros in ZIP codes.

Silver transformations: column names standardized to lowercase snake_case; `"NA"` strings replaced with null; date columns cast to DateType; PPTS columns divided by 100; all other non-string columns cast to DoubleType; for ZIP-grain tables, `region_name` cast to string and left-padded to 5 digits.

The following columns are common to all nine Redfin tables (three table types × three grains):

| Column | Description |
| --- | --- |
| `last_updated` | Date Redfin last refreshed this row's metrics |
| `frequency` | Reporting cadence of the period (e.g. 'monthly') |
| `period_begin` | First day of the reporting period |
| `period_end` | Last day of the reporting period |
| `region_type` | Grain discriminator: 'County', 'Zip', or 'Neighborhood' |
| `region_name` | Region label for the grain (county name, 5-digit ZIP code, or neighborhood name). Join key across Redfin tables and gold fact tables |
| `metro` | Metro area the region belongs to |
| `_ingest_timestamp` | Time the bronze Auto Loader job ingested the raw file |
| `_source_file` | Raw source file path in the bronze UC volume |
| `_rescued_data` | Auto Loader quarantine column holding values that failed to parse against the schema |
| `_processed_timestamp` | Silver pipeline processing time; added via `current_timestamp()` |

> **Note:** The `region_id` column is only present in the `housing_market` family (see §1a). The `price_drops` and `delistings_relistings` silver tables do not include it.

The columns unique to each table type are listed in the subsections below.

---

## 1a. Redfin: housing_market (unique columns)

**Source:** [Redfin Data Center](https://www.redfin.com/news/data-center/)

**Bronze tables:** 
* `bronze_dev.redfin.housing_market_county` 
* `bronze_dev.redfin.housing_market_zipcode`
* `bronze_dev.redfin.housing_market_neighborhood`

**Silver tables:** 
* `silver_dev.redfin.housing_market_county` 
* `silver_dev.redfin.housing_market_zipcode`
* `silver_dev.redfin.housing_market_neighborhood`

| Column | Description |
| --- | --- |
| `region_id` | Redfin numeric region identifier |
| `homes_sold` | Homes sold in the period |
| `homes_sold_mom` | Month-over-month change in homes sold, percent |
| `homes_sold_yoy` | Year-over-year change in homes sold, percent |
| `median_sale_price_nsa` | Median sale price, not seasonally adjusted, USD |
| `median_sale_price_nsa_mom` | MoM change in median sale price (NSA), percent |
| `median_sale_price_nsa_yoy` | YoY change in median sale price (NSA), percent |
| `median_days_on_market_days` | Median days on market |
| `median_days_on_market_mom` | MoM change in median days on market, percent |
| `median_days_on_market_yoy` | YoY change in median days on market, percent |
| `average_sale_to_list_ratio` | Average sale-to-list price ratio |
| `average_sale_to_list_ratio_mom_ppts` | MoM change in sale-to-list ratio, percentage points |
| `average_sale_to_list_ratio_yoy_ppts` | YoY change in sale-to-list ratio, percentage points |
| `share_sold_above_original_list` | Share of homes sold above their original list price |
| `share_sold_above_original_list_mom_ppts` | MoM change in share sold above list, percentage points |
| `share_sold_above_original_list_yoy_ppts` | YoY change in share sold above list, percentage points |
| `new_listings` | New listings in the period |
| `new_listings_mom` | MoM change in new listings, percent |
| `new_listings_yoy` | YoY change in new listings, percent |
| `active_listings` | Active listings at period end |
| `active_listings_mom` | MoM change in active listings, percent |
| `active_listings_yoy` | YoY change in active listings, percent |
| `inventory` | Housing inventory (active listings) in the period |
| `inventory_mom` | MoM change in inventory, percent |
| `inventory_yoy` | YoY change in inventory, percent |
| `pending_sales` | Pending sales in the period |
| `pending_sales_mom` | MoM change in pending sales, percent |
| `pending_sales_yoy` | YoY change in pending sales, percent |
| `median_new_listing_price` | Median price of new listings, USD |
| `median_new_listing_price_mom` | MoM change in median new listing price, percent |
| `median_new_listing_price_yoy` | YoY change in median new listing price, percent |
| `median_new_listing_price_per_sq_ft` | Median new listing price per square foot, USD |
| `median_new_listing_price_per_sq_ft_mom` | MoM change, percent |
| `median_new_listing_price_per_sq_ft_yoy` | YoY change, percent |
| `median_sale_price_per_sq_ft` | Median sale price per square foot, USD |
| `median_sale_price_per_sq_ft_mom` | MoM change, percent |
| `median_sale_price_per_sq_ft_yoy` | YoY change, percent |
| `months_of_supply` | Months of housing supply at the current sales pace |
| `months_of_supply_mom` | MoM change in months of supply, percent |
| `months_of_supply_yoy` | YoY change in months of supply, percent |
| `percent_off_market_in_two_weeks` | Share of listings off the market within two weeks, percent |
| `percent_off_market_in_two_weeks_mom_ppts` | MoM change, percentage points |
| `percent_off_market_in_two_weeks_yoy_ppts` | YoY change, percentage points |

---

## 1b. Redfin: price_drops (unique columns)

**Source:** [Redfin Data Center](https://www.redfin.com/news/data-center/)

**Bronze tables:** `bronze_dev.redfin.price_drops_county`, `bronze_dev.redfin.price_drops_zipcode`, `bronze_dev.redfin.price_drops_neighborhood`
**Silver tables:** `silver_dev.redfin.price_drops_county`, `silver_dev.redfin.price_drops_zipcode`, `silver_dev.redfin.price_drops_neighborhood`

| Column | Description |
| --- | --- |
| `price_drops` | Number of active listings that had a price drop in the period |
| `price_drops_mom` | MoM change in price drops, percent |
| `price_drops_yoy` | YoY change in price drops, percent |
| `average_size_of_price_drop` | Average size of a price drop, percent reduction |
| `average_size_of_price_drop_mom_ppts` | MoM change, percentage points |
| `average_size_of_price_drop_yoy_ppts` | YoY change, percentage points |
| `percent_active_with_price_drops` | Share of active listings with a price drop, percent |
| `percent_active_with_price_drops_mom_ppts` | MoM change, percentage points |
| `percent_active_with_price_drops_yoy_ppts` | YoY change, percentage points |
| `homes_sold_with_price_drops` | Homes sold after a price drop |
| `homes_sold_with_price_drops_mom` | MoM change, percent |
| `homes_sold_with_price_drops_yoy` | YoY change, percent |

---

## 1c. Redfin: delistings_relistings (unique columns)

**Source:** [Redfin Data Center](https://www.redfin.com/news/data-center/)

**Bronze tables:** `bronze_dev.redfin.delistings_relistings_county`, `bronze_dev.redfin.delistings_relistings_zipcode`, `bronze_dev.redfin.delistings_relistings_neighborhood`
**Silver tables:** `silver_dev.redfin.delistings_relistings_county`, `silver_dev.redfin.delistings_relistings_zipcode`, `silver_dev.redfin.delistings_relistings_neighborhood`

| Column | Description |
| --- | --- |
| `total_delistings` | Listings removed from the market in the period |
| `total_delistings_mom` | MoM change in delistings, percent |
| `total_delistings_yoy` | YoY change in delistings, percent |
| `total_relistings` | Previously delisted listings put back on the market |
| `total_relistings_mom` | MoM change in relistings, percent |
| `total_relistings_yoy` | YoY change in relistings, percent |
| `share_of_listings_delisted` | Share of listings delisted, percent |
| `share_of_listings_delisted_mom_ppts` | MoM change, percentage points |
| `share_of_listings_delisted_yoy_ppts` | YoY change, percentage points |
| `share_of_listings_relisted` | Share of listings relisted, percent |
| `share_of_listings_relisted_mom_ppts` | MoM change, percentage points |
| `share_of_listings_relisted_yoy_ppts` | YoY change, percentage points |

---

## 2. HUD: dim_states (reference dimension)

**Source:** [HUD API](https://www.huduser.gov/hudapi/public/)

**Bronze table:** `bronze_dev.hud.states`
**Silver table:** `silver_dev.hud.dim_states`
**Grain:** One row per state or territory

Silver transformations: batch read from bronze (static reference data, not streamed); column names standardized to lowercase snake_case via `clean_columns_silver`; `_processed_timestamp` added.

| Column | Description |
| --- | --- |
| `state_code` | 2-letter state/territory abbreviation; natural key |
| `state_name` | State or territory name |
| `_ingest_timestamp` | Time the bronze job ingested the row |
| `_processed_timestamp` | Silver pipeline processing time; added via `current_timestamp()` |

---

## 3. HUD: dim_counties (reference dimension)

**Source:** [HUD API](https://www.huduser.gov/hudapi/public/)

**Bronze table:** `bronze_dev.hud.counties`
**Silver table:** `silver_dev.hud.dim_counties`
**Grain:** One row per county

Silver transformations: batch read from bronze (static reference data, not streamed); column names standardized to lowercase snake_case via `clean_columns_silver`; `_processed_timestamp` added.

| Column | Description |
| --- | --- |
| `fips_code` | 5-digit county FIPS code (2-digit state + 3-digit county); natural key |
| `state_code` | 2-letter state code |
| `county_name` | Short county name |
| `cntyname` | Long-form county name as reported by HUD (e.g., "County, ST") |
| `category` | County classification code from HUD reference data |
| `town_name` | Town name associated with the county record |
| `_ingest_timestamp` | Time the bronze job ingested the row |
| `_processed_timestamp` | Silver pipeline processing time; added via `current_timestamp()` |

---

## 4. Opportunity Insights: social_capital_zip

**Source:** [Opportunity Insights](https://opportunityinsights.org/data/)

**Bronze table:** `bronze_dev.opportunity_insights.social_capital_zip`
**Silver table:** `silver_dev.opportunity_insights.social_capital_zip`
**Grain:** One row per ZIP code

Silver transformations: columns renamed to descriptive names; `zip` and `county` cast to string (FIPS codes preserve leading zeros); numeric columns kept as-is; deduplicated by `zip_code`.

| Column | Description |
| --- | --- |
| `zip_code` | 5-digit ZIP code; string to preserve leading zeros |
| `county_fips_code` | 5-digit county FIPS code; string |
| `children_below_median_parental_income_count` | Children (1988 birth cohort) with below-median parental income in this ZIP |
| `population_2018` | 2018 population estimate |
| `economic_connectedness_low_ses` | Economic connectedness: share of high-SES friends for below-median-SES individuals |
| `economic_connectedness_low_ses_standard_error` | Standard error of economic_connectedness_low_ses |
| `neighborhood_economic_connectedness_low_ses` | Neighborhood-weighted economic connectedness, low-SES |
| `group_economic_connectedness_low_ses` | Group-membership-based economic connectedness, low-SES |
| `economic_connectedness_high_ses` | Economic connectedness: share of high-SES friends for above-median-SES individuals |
| `economic_connectedness_high_ses_standard_error` | Standard error of economic_connectedness_high_ses |
| `neighborhood_economic_connectedness_high_ses` | Neighborhood-weighted economic connectedness, high-SES |
| `group_economic_connectedness_high_ses` | Group-membership-based economic connectedness, high-SES |
| `group_exposure_to_high_ses_low_ses` | Exposure of low-SES individuals to high-SES peers via shared groups |
| `group_exposure_to_high_ses_high_ses` | Exposure of high-SES individuals to high-SES peers via shared groups |
| `neighborhood_exposure_to_high_ses` | Neighborhood-level exposure to high-SES individuals |
| `group_friending_bias_low_ses` | Friending bias in groups, low-SES individuals |
| `group_friending_bias_high_ses` | Friending bias in groups, high-SES individuals |
| `neighborhood_friending_bias_low_ses` | Friending bias at neighborhood level, low-SES |
| `neighborhood_friending_bias_high_ses` | Friending bias at neighborhood level, high-SES |
| `friend_network_clustering` | Clustering of friendship networks within the ZIP |
| `within_zcta_friendship_support_ratio` | Friendship support ratio among ZIP residents |
| `volunteering_group_membership_rate` | Volunteering / group membership rate |
| `civic_organizations_per_1000_users` | Civic organizations per 1,000 users |
| `_rescued_data` | Auto Loader quarantine column holding values that failed to parse |
| `_ingest_timestamp` | Time the bronze Auto Loader job ingested the raw file |
| `_source_file` | Raw source file path in the bronze UC volume |
| `_processed_timestamp` | Silver pipeline processing time; added via `current_timestamp()` |

---

## 5. Census Bureau: american_community_survey_zcta

**Source:** [Census Bureau ACS](https://www.census.gov/programs-surveys/acs/technical-documentation.html)

**Bronze table:** `bronze_dev.census_bureau.american_community_survey_zcta`
**Silver table:** `silver_dev.census_bureau.american_community_survey_zcta`
**Grain:** One row per ZCTA (ZIP Code Tabulation Area)

Silver transformations: Census variable codes renamed to descriptive names; `NAME` and `zip` cast to string (ZCTA codes preserve leading zeros); demographic variables cast to int or double.

| Column | Description |
| --- | --- |
| `zcta_name` | ZCTA display name |
| `zcta_code` | 5-digit ZIP Code Tabulation Area code; string to preserve leading zeros |
| `total_population` | Total population |
| `median_age` | Median age, years |
| `median_household_income` | Median household income, USD |
| `population_below_poverty_level` | Population with income below poverty level |
| `white_alone_population` | Population, White alone |
| `black_or_african_american_alone_population` | Population, Black or African American alone |
| `asian_alone_population` | Population, Asian alone |
| `hispanic_or_latino_population` | Population, Hispanic or Latino (any race) |
| `_ingest_timestamp` | Time the bronze Auto Loader job ingested the raw file |
| `_source_file` | Raw source file path in the bronze UC volume |
| `_processed_timestamp` | Silver pipeline processing time; added via `current_timestamp()` |

---

## 6. HUD: fair_market_rents_county

**Source:** [HUD Fair Market Rents](https://www.huduser.gov/portal/datasets/fmr.html)

**Bronze table:** `bronze_dev.hud.fair_market_rents_county`
**Silver table:** `silver_dev.hud.fair_market_rents_county`
**Grain:** One row per county × ZIP code × year

Silver transformations: `year` cast from string to int; all other columns kept as-is; deduplicated by `county_name`, `zip_code`, `year`.

| Column | Description |
| --- | --- |
| `county_name` | County name from HUD FMR data |
| `metro_name` | Metro area name from HUD FMR data |
| `area_name` | HUD FMR area name |
| `metro_status` | 1 if the area is a metropolitan area, 0 otherwise |
| `smallarea_status` | 1 if ZIP-level (small area) FMRs apply, 0 otherwise |
| `year` | FMR program year |
| `zip_code` | ZIP code the FMR applies to |
| `efficiency` | Fair Market Rent for an efficiency (studio) unit, USD per month |
| `one_bedroom` | Fair Market Rent for a one-bedroom unit, USD per month |
| `two_bedroom` | Fair Market Rent for a two-bedroom unit, USD per month |
| `three_bedroom` | Fair Market Rent for a three-bedroom unit, USD per month |
| `four_bedroom` | Fair Market Rent for a four-bedroom unit, USD per month |
| `source_url` | HUD API URL the row was fetched from |
| `_ingest_timestamp` | Time the bronze Auto Loader job ingested the raw file |
| `_source_file` | Raw source file path in the bronze UC volume |
| `_processed_timestamp` | Silver pipeline processing time; added via `current_timestamp()` |

---

## 7. Gold Layer: Fact Tables

**Pipeline file:** `src/pipelines/real_estate_pipeline_gold.py`

**Pattern:** Batch reads (`spark.read.table()`) from silver → materialized views. Each gold fact table joins three Redfin silver tables at the same grain: `housing_market` (base) left-joined with `price_drops` and `delistings_relistings` on `region_name` + `period_begin` + `period_end`.

**Gold transformations:**
- Pipeline metadata columns dropped (`_rescued_data`, `_ingest_timestamp`, `_source_file`, `_processed_timestamp`) — gold exposes only business-relevant columns.
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

| Column | Type | Description |
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

| Column | Type | Description |
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

| Column | Type | Description |
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

| Column | Type | Description |
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
