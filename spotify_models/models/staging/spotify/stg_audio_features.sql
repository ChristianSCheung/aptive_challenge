with audio_features as (
    select * from {{ source('spotify', 'audio_features') }}
),

stg_audio_features as (
    select
        track_id,
        danceability,
        energy,
        key,
        loudness,
        mode,
        speechiness,
        acousticness,
        instrumentalness,
        liveness,
        valence,
        tempo,
        duration_ms,
        time_signature,
        loaded_at
    from audio_features
)

select * from stg_audio_features
