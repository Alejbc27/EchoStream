"""
Schema definitions for EchoStream's Bronze layer.

WHY SCHEMAS?
In the Raw layer, data is stored as-is from the Spotify API (nested JSON).
The Bronze layer normalizes this into flat, typed columns.

Schemas serve three purposes:
  1. Documentation — they define the "contract" between Raw and Bronze
  2. Validation — Polars rejects data that doesn't match the schema
  3. Optimization — typed columns compress better than untyped JSON

WHY FLATTEN?
Spotify's API returns deeply nested JSON (track → artists → [0] → name).
Flat tables are:
  - Easier to query in SQL/BigQuery
  - More efficient in columnar formats (Parquet)
  - Simpler to test and validate

We extract only the fields we need for analytics. The full JSON
is always available in the Raw layer if we need more fields later.
"""

import polars as pl


# ── Recently-played tracks schema ────────────────────────────────────────────
# Used for data from spotify/recent/ paths.
# Each row represents one track play with a timestamp.
#
# NOTE: This dict is used as documentation AND as the target schema for
# DataFrame construction. Polars enforces these types at creation time.
RECENT_TRACKS_SCHEMA = {
    "track_id": pl.String,  # Spotify track ID (e.g. "4uLU6hMCjMI75M1A2tKUQC")
    "track_name": pl.String,  # Song title
    "artist_id": pl.String,  # Primary artist's Spotify ID
    "artist_name": pl.String,  # Primary artist's display name
    "album_id": pl.String,  # Album Spotify ID
    "album_name": pl.String,  # Album title
    "played_at": pl.String,  # ISO 8601 timestamp (parsed to Datetime after creation)
    "duration_ms": pl.Int64,  # Track length in milliseconds
    "explicit": pl.Boolean,  # Contains explicit content?
    "popularity": pl.Int32,  # 0-100 Spotify popularity score
}

# ── Top tracks schema ────────────────────────────────────────────────────────
# Used for data from spotify/top/<time_range>/ paths.
# No played_at (these are aggregated rankings, not individual plays).
TOP_TRACKS_SCHEMA = {
    "track_id": pl.String,
    "track_name": pl.String,
    "artist_id": pl.String,
    "artist_name": pl.String,
    "album_id": pl.String,
    "album_name": pl.String,
    "duration_ms": pl.Int64,
    "explicit": pl.Boolean,
    "popularity": pl.Int32,
    "time_range": pl.String,  # "short_term", "medium_term", or "long_term"
}


def normalize_recent_track(raw_item: dict) -> dict:
    """
    Flatten a recently-played API item into a Bronze-layer row.

    Input structure (from Spotify API):
        {
            "track": {
                "id": "abc123",
                "name": "Bohemian Rhapsody",
                "artists": [{"id": "art1", "name": "Queen"}],
                "album": {"id": "alb1", "name": "A Night at the Opera"},
                "duration_ms": 354000,
                "explicit": false,
                "popularity": 92
            },
            "played_at": "2026-01-31T12:00:00.000Z"
        }

    Output: flat dict matching RECENT_TRACKS_SCHEMA.

    WHY extract only the primary artist?
    A track can have multiple artists (features/collabs), but for our
    analytics the primary artist (index 0) is the most useful dimension.
    If we need all artists later, we can create a separate artist-track
    mapping table from the Raw data.
    """
    track = raw_item["track"]
    primary_artist = track["artists"][0] if track.get("artists") else {}

    return {
        "track_id": track.get("id"),
        "track_name": track.get("name"),
        "artist_id": primary_artist.get("id"),
        "artist_name": primary_artist.get("name"),
        "album_id": track.get("album", {}).get("id"),
        "album_name": track.get("album", {}).get("name"),
        "played_at": raw_item.get("played_at"),
        "duration_ms": track.get("duration_ms"),
        "explicit": track.get("explicit"),
        "popularity": track.get("popularity"),
    }


def normalize_top_track(raw_item: dict, time_range: str) -> dict:
    """
    Flatten a top-tracks API item into a Bronze-layer row.

    Input structure (from Spotify API — note: NO nesting under "track"):
        {
            "id": "abc123",
            "name": "Shape of You",
            "artists": [{"id": "art1", "name": "Ed Sheeran"}],
            "album": {"id": "alb1", "name": "÷"},
            "duration_ms": 233000,
            "explicit": false,
            "popularity": 90
        }

    time_range: The time window for this ranking ("short_term", etc.).
    We add this as a column because the raw data doesn't include it —
    it comes from the GCS path (spotify/top/<time_range>/...).
    """
    primary_artist = raw_item["artists"][0] if raw_item.get("artists") else {}

    return {
        "track_id": raw_item.get("id"),
        "track_name": raw_item.get("name"),
        "artist_id": primary_artist.get("id"),
        "artist_name": primary_artist.get("name"),
        "album_id": raw_item.get("album", {}).get("id"),
        "album_name": raw_item.get("album", {}).get("name"),
        "duration_ms": raw_item.get("duration_ms"),
        "explicit": raw_item.get("explicit"),
        "popularity": raw_item.get("popularity"),
        "time_range": time_range,
    }
