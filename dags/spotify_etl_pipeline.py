from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

from datetime import datetime
import sys
sys.path.append("/Users/christiancheung/Documents/Aptive Technical Challenge")
from pipeline.utils import get_new_access_token_air, upload_parquet_to_s3
from pipeline.extract import get_top_tracks

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

# def generate_and_upload_parquet():
#         token = get_new_access_token_air()
#         results = get_top_tracks(token)
#         print(results.head())
#         upload_parquet_to_s3("spotify-data-bucket", results, "top-tracks")


from airflow.models import Variable

def generate_and_upload_parquet():
    print('🚀 Starting upload task')

    # Debug: print environment setup (safe values only)
    print("🔍 Checking Airflow Variables...")
    print("SPOTIFY_CLIENT_ID exists:", bool(Variable.get("SPOTIFY_CLIENT_ID", default_var=None)))
    print("SPOTIFY_CLIENT_SECRET exists:", bool(Variable.get("SPOTIFY_CLIENT_SECRET", default_var=None)))
    print("SPOTIFY_REFRESH_TOKEN exists:", bool(Variable.get("SPOTIFY_REFRESH_TOKEN", default_var=None)))

    # Proceed with access token and data extraction
    token = get_new_access_token_air()
    print(" Access token received (not printing for safety)")

    results = get_top_tracks(token)
    print(" Top tracks sample:")
    print(results.head())

    upload_parquet_to_s3("spotify-data-bucket", results, "top-tracks")
    print(' Upload task finished')


with DAG(
    dag_id="spotify_etl_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["spotify"],
) as dag:

    upload_task = PythonOperator(
        task_id="generate_and_upload_parquet",
        python_callable=generate_and_upload_parquet,
    )

    copy_into = SnowflakeOperator(
        task_id="copy_into_top_tracks",
        snowflake_conn_id="snowflake_conn", 
        sql="""
            copy into raw.spotify.top_tracks
            from (
              select
                $1:track_name::string as track_name,
                $1:track_id::string as track_id,
                $1:artists_name::string as artists_name,
                $1:popularity::number as popularity,
                current_timestamp() as loaded_at
              from @raw.spotify.spotify_top_tracks_stage
            )
            file_format = (type = parquet)
            pattern = '.*\\.parquet';
        """
    )

    run_dbt = BashOperator(
        task_id="run_dbt_model",
        bash_command="cd /Users/christiancheung/Documents/Aptive Technical Challenge/spotify_models && dbt run --select stg_top_tracks"
    )

    upload_task >> copy_into >> run_dbt
