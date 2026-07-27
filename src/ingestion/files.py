
def download_file(
    url: str,
    destination: str,
    dbutils
):
    """
    Download/copy a file into a Databricks-accessible location.
    """
    dbutils.fs.cp(
        url,
        destination
    )