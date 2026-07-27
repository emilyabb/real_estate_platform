
import sys
sys.path.append("/Workspace/Repos/dev/real_estate_platform")

from src.ingestion.files import download_file
import src.config.sources.opportunity_insights


for table_name, table_cfg in src.config.sources.opportunity_insights.TABLES.items():
    download_file(
        url=table_cfg.remote_path, # contains the url to read from
        destination=table_cfg.source_path, # where to put the file
        dbutils=dbutils
    )