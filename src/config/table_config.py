from dataclasses import dataclass, field

@dataclass(frozen=True)
class TableConfig:
    table_name: str
    source_path: str
    natural_key: list[str]
    remote_path:str = "" # Only applies to tables that get downloaded to a local path as part of ingestion
    file_format: str = "csv"
    read_options: dict[str, str] = field(default_factory=lambda: {
        "header": "true",
        "inferSchema": "false",
    })
    read_type="direct" # Direct for reading from an S3 bucket etc., "dbutils.fs.cp" if that is needed

    # Potential silver stuff
    # skip_columns: set[str] = field(default_factory=set)
    # casts: dict[str, str] = field(default_factory=dict)

    # Future generalizable stuff
    # source_type: str          # csv, json, delta, jdbc, api
    # source: str               # path, table name, URL, etc.

# >>> Future ingestion code
#     if cfg.source_type == "csv":
#     ...
# elif cfg.source_type == "delta":
#     ...
# elif cfg.source_type == "api":
#     ...