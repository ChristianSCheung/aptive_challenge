from flask import Blueprint, jsonify
import os
from dotenv import load_dotenv
import pandas as pd
from pipeline.extract import get_top_tracks, get_recent_top_track_ids, get_audio_features_for_tracks
from pipeline.utils import get_new_access_token, upload_parquet_to_s3, copy_into_top_tracks

# Load environment variables
load_dotenv()

# Spotify API credentials
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI")
os.environ["SPOTIFY_REDIRECT_URI"] = "http://127.0.0.1:8000/callback"

# Initialize Flask Blueprint
main = Blueprint("main", __name__)

@main.route("/run-top-tracks", methods=["POST"])
def run_top_tracks():
    """
    Fetches the top tracks from Spotify and uploads the result to S3 and Snowflake.
    Returns a status message indicating success or failure.
    """
    # Get a new access token
    token = get_new_access_token()

    # Get the top 25 tracks from Spotify - My friend
    result = get_top_tracks(token)

    if not result:
        return jsonify({"error": "No tracks found"}), 404

    # Upload the top tracks data to S3
    upload_parquet_to_s3("spotify-data-bucket", result, "top-tracks")

    # Copy data into Snowflake
    try:
        copy_into_top_tracks()
    except Exception as e:
        return jsonify({"error": f"Uploaded to S3 but failed to copy into Snowflake: {str(e)}"}), 500

    return jsonify({"status": "Uploaded to S3 and Snowflake", "records": len(result)}), 200

@main.route("/run-audio-features", methods=["POST"])
def run_audio_features():
    """
    Fetches audio features for recent top tracks and processes the data.
    Returns an error if no audio features are returned.
    """
    # Get a new access token
    token = get_new_access_token()

    # Step 1: Fetch recent top track IDs from Snowflake
    df = get_recent_top_track_ids()
    if df.empty:
        return jsonify({"error": "No recent top_tracks data found in Snowflake"}), 404

    # Step 2: Get audio features for the tracks
    track_ids = df["track_id"].tolist()
    features = get_audio_features_for_tracks(track_ids, token)

    return jsonify({
        "status": "Audio features retrieved successfully",
    }), 200

# @main.route("/login")
# def login():
#     scopes = "user-top-read"
#     auth_url = (
#         "https://accounts.spotify.com/authorize"
#         f"?client_id={CLIENT_ID}"
#         f"&response_type=code"
#         f"&redirect_uri={REDIRECT_URI}"
#         f"&scope={scopes}"
#     )
#     return redirect(auth_url)


# @main.route("/callback")
# def callback():
#     code = request.args.get("code")
#     if not code:
#         return "Authorization failed.", 400

#     token_url = "https://accounts.spotify.com/api/token"

#     response = requests.post(
#         token_url,
#         data={
#             "grant_type": "authorization_code",
#             "code": code,
#             "redirect_uri": REDIRECT_URI,
#             "client_id": CLIENT_ID,
#             "client_secret": CLIENT_SECRET,
#         },
#         headers={"Content-Type": "application/x-www-form-urlencoded"},
#     )

#     if response.status_code != 200:
#         return f"Failed to get token: {response.text}", 400

#     tokens = response.json()
#     access_token = tokens.get("access_token")
#     refresh_token = tokens.get("refresh_token")

#     print("Access Token:", access_token)
#     print("Refresh Token:", refresh_token)

#     return "Authentication successful! You can close this tab."
