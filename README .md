
# Aptive Technical Challenge

This project is a simplified data pipeline that extracts Spotify top tracks data, enriches it with audio features, and loads it into Snowflake for further analysis using dbt. It includes Docker setup, scheduled orchestration, and transformations through dbt models.

## Architecture

```
Spotify API (Top Tracks)
        ↓
Audio Features CSV (fallback due to API limitations)
        ↓
Python Pipeline (run.py)
        ↓
Parquet File
        ↓
Amazon S3
        ↓
Snowflake (External Stage + COPY INTO)
        ↓
dbt Models (Top Tracks, Audio Features, Intermediate)
```

## Project Structure

```
Aptive Technical Challenge/
├── dags/                     # (Optional) Airflow DAGs folder --- Failed to make it work... 
├── dockerfile                # Docker setup for containerizing the app
├── pipeline/                 # Python logic for ETL process
├── README.md                 # Project documentation
├── requirements.txt          # Python dependencies
├── run.py                    # Entry point to run the pipeline
├── spotify_models/           # dbt models
├── spotify_tokens.json       # Spotify OAuth tokens
└── target/                   # dbt build artifacts
```

## Setup Instructions

1. **Install Dependencies**

   Create a virtual environment (optional but recommended):

   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Environment Configuration**

   Set up any required environment variables for:
   - Spotify API credentials
   - Snowflake account details

   You may store secrets in a `.env` file or use a secret manager if deploying to the cloud.

3. **Run the Pipeline**

   ```bash
   python run.py
   ```

   This will:
   - Fetch top tracks from Spotify.
   - Read fallback audio features from a CSV file.
   - Generate a Parquet file.
   - Upload it to an S3 bucket.
   - Use a Snowflake stage and `COPY INTO` command to ingest data.

4. **Schedule the Pipeline**

   A cron job runs this pipeline daily at midnight:

   ```cron
   0 0 * * * curl -X POST http://localhost:8000/run-top-tracks
   ```

5. **dbt Transformations**

   The following models are defined in the `spotify_models` folder:

   - `top_tracks`
   - `audio_features`
   - `inter_top_weekly_audio_features` (intermediate aggregation model)

   Run them with:

   ```bash
   dbt run --project-dir spotify_models
   ```


## Assumptions & Design Decisions

- **Audio Features Workaround**: Spotify’s `/audio-features` endpoint did not return reliable results, so a publicly available CSV was used to match song features.
- **Partial Coverage**: The CSV may not cover all top tracks, but it contains the majority.
- **Snowflake COPY**: A Snowflake integration and stage were manually configured to enable external S3 ingestion.
- **Midnight Schedule**: Based on the assumption that Spotify’s top tracks update daily.
- **Simple Cron**: Chosen over Airflow for simplicity; however, the `dags/` folder is included in case of future orchestration.

## Potential Improvements

- **Enable Spotify Audio Features API** Find another endpoint? 
- **Cloud Scheduling** with AWS EventBridge or GCP Cloud Scheduler.
- **Monitoring & Logging** for better observability.
- **Unit Tests** and validation for CSV audio feature matching.
- **Environment Management** using `Docker Compose` or Terraform for infrastructure setup.
