with top_tracks as (
    select * from {{ source('spotify', 'top_tracks') }}
),

    stg_top_tracks as (
    select
        track_id,
        track_name,
        artists_name,
        popularity,
        loaded_at
    from top_tracks
    )

select * from stg_top_tracks


