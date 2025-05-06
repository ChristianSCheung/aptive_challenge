import os
import requests
import json
import boto3
from datetime import datetime
from dotenv import load_dotenv
import io
import pandas as pd

# Load environment variables
load_dotenv()

def get_new_access_token():
    """
    Fetches a new access token using the refresh token stored in the environment.
    
    Returns:
        str: New access token from Spotify.
    
    Raises:
        Exception: If the request to refresh the token fails.
    """
    tokens = json.loads(os.environ["SPOTIFY_TOKENS_JSON"])
    refresh_token = tokens["refresh_token"]

    try:
        response = requests.post(
            "https://accounts.spotify.com/api/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": os.getenv("SPOTIFY_CLIENT_ID"),
                "client_secret": os.getenv("SPOTIFY_CLIENT_SECRET"),
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()  # Raise HTTPError for bad responses

        new_access_token = response.json()["access_token"]
        print("New access token generated.")
        tokens["access_token"] = new_access_token
        return new_access_token

    except requests.exceptions.RequestException as e:
        print(f"Failed to refresh token: {e}")
        raise


def upload_parquet_to_s3(bucket_name, data, filename_prefix):
    """
    Uploads a DataFrame as a Parquet file to an S3 bucket.
    
    Parameters:
        bucket_name (str): The name of the S3 bucket.
        data (list of dict): Data to be uploaded.
        filename_prefix (str): Prefix for the Parquet file name.
    """
    s3 = boto3.client("s3")

    now = datetime.utcnow()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    key = f"{filename_prefix}/{year}/{month}/{day}/{filename_prefix}_{timestamp}.parquet"

    # Convert data to DataFrame
    df = pd.DataFrame(data)

    # Write DataFrame to in-memory Parquet file
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")

    try:
        # Upload to S3
        buffer.seek(0)
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream"
        )
        print(f"Uploaded to s3://{bucket_name}/{key}")
    
    except boto3.exceptions.S3UploadFailedError as e:
        print(f"Failed to upload to S3: {e}")
        raise


def get_snowflake_connection():
    """
    Establishes a connection to Snowflake using environment variables.
    
    Returns:
        snowflake.connector.connect: A Snowflake connection object.
    """
    import snowflake.connector

    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def copy_into_top_tracks():
    """
    Copies data from staged top tracks in S3 to the raw top_tracks table in Snowflake.
    """
    query = """
        COPY INTO raw.spotify.top_tracks
        FROM (
            SELECT
                $1:track_name::STRING AS track_name,
                $1:track_id::STRING AS track_id,
                $1:artists_name::STRING AS artists_name,
                $1:popularity::NUMBER AS popularity,
                TO_TIMESTAMP_TZ(
                    REGEXP_SUBSTR(METADATA$FILENAME, '[0-9]{8}_[0-9]{6}'),
                    'YYYYMMDD_HH24MISS'
                )::TIMESTAMP_NTZ AS loaded_at
            FROM @raw.spotify.spotify_top_tracks_stage
        )
        FILE_FORMAT = (TYPE = PARQUET)
        PATTERN = '.*\\.parquet';
    """

    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cs:
            cs.execute(query)
            print("COPY INTO for top_tracks succeeded.")
    except Exception as e:
        print(f"COPY INTO for top_tracks failed: {e}")
        raise
    finally:
        conn.close()
