{{ config(
    materialized = 'table',
    database = 'inter'
) }}

with top_tracks as (
    select * from {{ source('spotify', 'top_tracks') }}
),

audio_features as (
    select * from {{ source('spotify', 'audio_features') }}
),

inter_top_weekly_audio_features as (
    select
        top_tracks.loaded_at,
        avg(top_tracks.popularity) as avg_popularity,
        avg(audio_features.danceability) as avg_danceability,
        avg(audio_features.energy) as avg_energy,
        avg(audio_features.loudness) as avg_loudness,
        avg(audio_features.speechiness) as avg_speechiness,
        avg(audio_features.tempo) as avg_tempo,
        avg(audio_features.liveness) as avg_liveness,
        avg(audio_features.duration_ms) / 1000.0 as avg_duration_seconds
    from top_tracks
    left join audio_features
        on top_tracks.track_id = audio_features.track_id
    group by top_tracks.loaded_at
)

select * from inter_top_weekly_audio_features
