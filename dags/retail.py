from airflow.decorators import dag, task
from datetime import datetime  # Corrected this line
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

from airflow.operators.bash_operator import BashOperator

@dag(
    start_date=datetime(2024, 1, 1),  # Corrected 'start_date'
    schedule_interval=None,
    catchup=False,
    tags=["retail"],
)

def retail():
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_csv_to_gcs",
        src="/usr/local/airflow/include/dataset/Online_Retail.csv",
        dst="raw/Online_Retail.csv",
        bucket="mukundrana_online_retail",
        gcp_conn_id="gcp",
        mime_type="text/csv"
    )
    
    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id='retail',
        gcp_conn_id='gcp',
    )
    
    gcs_to_raw = GCSToBigQueryOperator(
        task_id='gcs_to_raw',
        bucket='mukundrana_online_retail',
        source_objects=['raw/Online_Retail.csv'],
        destination_project_dataset_table='extreme-mix-410000.retail.raw_invoices',
        source_format='CSV',
        field_delimiter=',',
        skip_leading_rows=1,  # Assumes you have a header row in your CSV
        write_disposition='WRITE_TRUNCATE',  # Overwrites the table with new data
        max_bad_records=1000000,  # Set to 0 to skip all bad records
        autodetect=True,  # Auto-detects the schema
        create_disposition='CREATE_IF_NEEDED',  # Creates the table if it doesn't exist
        gcp_conn_id='gcp',
        allow_jagged_rows=True,  # Set to True to allow missing values in the last columns
        ignore_unknown_values=True  # Set to True to ignore extra values not represented in the table schema
    )
    
    
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /usr/local/airflow/include/dbt && dbt run --profiles-dir /usr/local/airflow/include/dbt/profiles'
    )

    
        
# This is missing: Create an instance of the DAG
retail_dag = retail()
