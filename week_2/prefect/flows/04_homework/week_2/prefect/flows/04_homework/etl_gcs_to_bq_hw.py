from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path)
    return Path(f"{gcs_path}")

@task()
def read(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True, )
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp3482.yellow_2019_02_03",
        project_id="de-zoomcamp-375907",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow()
def etl_gcs_to_bq_hw(year: int, month: int, color: str):
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = read(path)
    write_bq(df)
    print(f"Total number of processed rows: {df.shape[0]}")
    

@flow()
# Default parameters is 2021-01 yellow taxi data, parameters need to be set during prefect deployment build step
def etl_gcs_to_bq_parent_flow(months: list[int] = [1], year: int = 2021, color: str = "yellow"):
    for month in months:
        etl_gcs_to_bq_hw(year, month, color)

if __name__ == "__main__":
    # This is not run when flow is called by running deployment
    year = 2019
    months = [2,3]
    color = "yellow" 
    etl_gcs_to_bq_parent_flow(months, year, color)