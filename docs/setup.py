from src.utils.sources_ref import REDFIN_TABLES, REDFIN_VOLUME_BASE




# Create BRONZE_DEV catalog
spark.sql("CREATE CATALOG IF NOT EXISTS BRONZE_DEV")
print("✓ Created catalog: BRONZE_DEV")


# # Create subdirectory for each table
# for table in REDFIN_TABLES:
#     table_name = table["table_name"]
#     path = f"{REDFIN_VOLUME_BASE}/{table_name}"
#     dbutils.fs.mkdirs(path)
#     print(f"Created: {path}")


spark.sql("USE CATALOG bronze_dev")

# Create schema for raw data
spark.sql("CREATE SCHEMA IF NOT EXISTS census_bureau")
print("✓ Created schema: census_bureau")

# Create volume for raw census data
spark.sql("""
    CREATE VOLUME IF NOT EXISTS census_bureau.census_bureau_raw
""")
print("✓ Created volume: census_bureau.census_bureau_raw")

# Create directory for American Community Survey data
acs_dir = "/Volumes/bronze_dev/census_bureau/census_bureau_raw/american_community_survey"
dbutils.fs.mkdirs(acs_dir)
print(f"✓ Created directory: {acs_dir}")