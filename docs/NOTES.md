## Bronze Layer Data Sources

- **census_bureau**
  - Source: Census API (`https://api.census.gov/data/2023/acs/acs5`)
  - Table: `bronze_dev.census_bureau.acs_zipcode`
  - Key: `NAME`
  - Notes: American Community Survey data by ZIP code

- **HUD (Department of Housing and Urban Development)**
  - Source: 
  - Table: tbd
  - Key: tbd

- **opportunity_insights__social_capital_zip**
  - Source: https://data.humdata.org/.../social_capital_zip.csv
  - Table: `bronze_dev.opportunity_insights.social_capital_zip`
  - Key: `zip`
  - Notes: Social capital metrics by ZIP code

- **redfin__price_drops_zipcode**
  - Source: `/Volumes/bronze_dev/redfin/redfin_raw/redfin_price_drops_monthly_all_zips_2026_Jan_to_2026_Mar.csv`
  - Table: `bronze_dev.redfin.price_drops_zipcode`
  - Key: `REGION_NAME`
  - Notes: Monthly price drops by ZIP code

- **redfin__housing_market_tracker_zipcode**
  - Source: `/Volumes/bronze_dev/redfin/redfin_raw/redfin_housing_market_monthly_all_zips_key_metrics_2026_Jan_to_2026_Mar.csv`
  - Table: `bronze_dev.redfin.housing_market_tracker_zipcode`
  - Key: `REGION_NAME`
  - Notes: Housing market key metrics by ZIP code




# Notes

ACS Technical Documentation
- https://www.census.gov/programs-surveys/acs/technical-documentation.html

Census
- Overview of the API Discovery Tool: https://www.census.gov/data/developers/updates/new-discovery-tool.html 
- Had to request a token
- Available APIs: https://www.census.gov/data/developers/data-sets.html
