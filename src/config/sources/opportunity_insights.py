import sys
sys.path.append("/Workspace/Repos/dev/real_estate_platform")

from src.config.table_config import TableConfig

volume_path = "/Volumes/bronze_dev/opportunity_insights/opportunity_insights_raw"
file_path_csv = f"{volume_path}/social_capital_zipcode.csv"
url = (
    "https://data.humdata.org/dataset/85ee8e10-0c66-4635-b997-79b6fad44c71/"
    "resource/ab878625-279b-4bef-a2b3-c132168d536e/download/social_capital_zip.csv"
)
table_name = "opportunity_insights.social_capital_zip"
natural_key = ["zip"]

# Set up as a dictionary in case more sources need to be added in the future, and to match Redfin setup
TABLES = {
    table_name: TableConfig(
        table_name=table_name,
        source_path=file_path_csv,
        remote_path=url,
        natural_key=natural_key,
        # read_type="dbutils.fs.cp"
    )
}