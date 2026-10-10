# Gold Layer Plan

> Temporary planning doc — delete once the gold pipeline is built.

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

### 1. `gold_dev.dim_county`

**Purpose:** County dimension table for joins and labels.

**Source:** `silver_dev.hud.dim_counties` joined to `silver_dev.hud.dim_states` on `state_code`.

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
4. `gold_dev.dim_county` — join on county name (or via a county name → FIPS mapping). Adds state info.
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

## Crosswalks needed

- **ZCTA → county:** The `social_capital_zip` table has `county_fips_code` per ZIP. This can serve as a ZCTA → FIPS crosswalk for aggregating ACS to county level.
- **County name → FIPS:** HUD counties has both `county_name` and `fips_code`. Redfin uses `region_name` (county name) as the key. Need to match Redfin county names to HUD county names (watch for naming inconsistencies, e.g. "County" suffix differences).

## Suggested build order

1. ~~Promote HUD states + counties to silver~~ ✅ Done
2. `dim_county` (simple join of dim_states + dim_counties)
3. `county_housing_profile` (start with just the 3 Redfin county tables, add FMR + ACS enrichment later)
4. `zip_market_health` (now includes housing_market_zipcode — decide snapshot vs time-series first)
5. `neighborhood_market_activity` (now includes housing_market_neighborhood — may just be a view)

## Gold layer conventions

- **Drop metadata columns:** `_ingest_timestamp` and `_processed_timestamp` should not be carried into gold. They're pipeline plumbing useful in bronze/silver for debugging, but gold should expose only business-relevant columns. If a freshness indicator is needed, add a single `gold_refresh_timestamp` at the gold layer instead.
