import requests
import pandas as pd
from pipeline.utils import get_snowflake_connection

def get_top_tracks(access_token):
    """
    Fetches the top tracks from Spotify using the provided access token.
    
    Parameters:
        access_token (str): Spotify API access token.
        
    Returns:
        list: A list of dictionaries containing track information.
    """
    url = "https://api.spotify.com/v1/me/top/tracks"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    params = {
        "time_range": "short_term",  # "short_term", "medium_term", "long_term"
        "limit": 25  # Up to 50
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx, 5xx)

        items = response.json().get("items", [])
        result = []

        for item in items:
            artists = ", ".join(artist["name"] for artist in item["artists"])
            track_name = item["name"]
            track_id = item["id"]
            popularity = item["popularity"]

            result.append({
                "artists_name": artists,
                "track_name": track_name,
                "track_id": track_id,
                "popularity": popularity
            })

        return result

    except requests.exceptions.RequestException as e:
        print(f"Error fetching top tracks: {e}")
        return []


def get_recent_top_track_ids():
    """
    Retrieves the most recent top track IDs from Snowflake, checking against the audio_features table.
    
    Returns:
        pd.DataFrame: A DataFrame containing track IDs and their corresponding loaded_at timestamps.
    """
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cs:
            # Get the most recent loaded_at from audio_features
            cs.execute("SELECT MAX(loaded_at) FROM raw.spotify.audio_features")
            audio_features_max = cs.fetchone()[0]

            if audio_features_max is None:
                # If audio_features is empty, return all top_tracks
                cs.execute("""
                    SELECT DISTINCT track_id, loaded_at
                    FROM raw.spotify.top_tracks
                """)
            else:
                cs.execute("""
                    SELECT DISTINCT track_id, loaded_at
                    FROM raw.spotify.top_tracks
                    WHERE loaded_at > %s
                """, (audio_features_max,))
            
            rows = cs.fetchall()
            return pd.DataFrame(rows, columns=["track_id", "loaded_at"])

    except Exception as e:
        print(f"Error fetching recent top track IDs: {e}")
        return pd.DataFrame(columns=["track_id", "loaded_at"])
    finally:
        conn.close()


def get_audio_features_for_tracks(track_ids, token):
    """
    Fetches audio features for a list of track IDs from Spotify.
    
    Parameters:
        track_ids (list): A list of track IDs.
        loaded_at (str): The timestamp when the tracks were loaded.
        token (str): Spotify API access token.
        
    Returns:
        list: A list of audio feature dictionaries.
    """
    audio_features = []

    for track_id in track_ids:
        url = f"https://api.spotify.com/v1/audio-features/{track_id}"
        headers = {"Authorization": f"Bearer {token}"}

        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()  # Raise HTTPError for bad responses

            feature = resp.json()
            if feature:
                audio_features.append(feature)
        except requests.exceptions.RequestException as e:
            print(f" Failed to fetch features for track {track_id}: {e}")

    return audio_features
