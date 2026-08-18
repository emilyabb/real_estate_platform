
from src.ingestion.files import download_file
from src.config.sources.opportunity_insights import TABLES as OI_TABLES
from src.config.sources.redfin import TABLES as REDFIN_TABLES


tables = OI_TABLES | REDFIN_TABLES

for table_name, table_cfg in tables.items():
    
    print(table_cfg.remote_path)
    print(table_cfg.local_path)

    dbutils.fs.cp(
        table_cfg.remote_path, # contains the url to read from
        table_cfg.local_path, # where to put the file
    )