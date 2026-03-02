"""
Spotify → GCS extractor for EchoStream's Raw layer.

WHY a separate "extractor" module?
-----------------------------------
The SpotifyClient (client.py) is a thin HTTP wrapper: it just calls the API
and returns Python dicts. It knows nothing about storage.

The SpotifyExtractor is the pipeline step that:
  1. Asks the client for data
  2. Decides the storage format (NDJSON) and path layout (partitioned by date)
  3. Writes the file to GCS (Google Cloud Storage)
  4. Returns metadata so the rest of the pipeline knows what was produced

This separation is called the Single Responsibility Principle:
each class does ONE job, making them easier to test and modify independently.

RAW LAYER philosophy (Medallion architecture):
  Raw = exact copy of what the API returned, nothing transformed.
  We keep the original JSON structure so we can always re-process if we
  change our transformation logic later. Think of it as a time capsule.

Storage format — NDJSON (Newline-Delimited JSON):
  Instead of one big JSON array, we write one JSON object per line:
    {"track": "...", "played_at": "..."}
    {"track": "...", "played_at": "..."}
  Why? BigQuery can load NDJSON files directly as external tables.
  Also, if you open the file in a terminal you can grep/jq one line at a time.

GCS path layout (Hive partitioning):
  gs://<bucket>/spotify/<data_type>/YYYY/MM/DD/batch_<unix_ts>.json

  data_type = "recent" or "top/<time_range>"
  This separation lets BigQuery external tables scan only recent OR top data.

  Why partition by date? Tools like BigQuery, Spark, Polars can SKIP entire
  date partitions when querying (e.g. "only load January 2026"). Much faster.
"""

import json
import os
from datetime import UTC, datetime

from google.cloud import storage

from echostream.spotify.client import SpotifyClient


class SpotifyExtractor:
    """
    Extracts recently-played tracks from Spotify and saves them to GCS.

    Usage example (not run in tests — requires real credentials):
        config = SpotifyConfig()
        client = SpotifyClient(config)
        extractor = SpotifyExtractor(client, gcs_bucket="my-project-echostream-raw")
        result = extractor.extract_recent()
        print(result)
        # {"total_tracks": 42, "gcs_path": "gs://...", "extracted_at": "2026-01-31T..."}
    """

    def __init__(self, client: SpotifyClient, gcs_bucket: str) -> None:
        """
        Parameters
        ----------
        client:
            An authenticated SpotifyClient. We inject it here instead of
            creating it internally so that tests can pass a fake client
            (this pattern is called "dependency injection").

        gcs_bucket:
            The name of the GCS bucket (without gs://), e.g.
            "myproject-echostream-raw".  Set via env var GCS_RAW_BUCKET.
        """
        self._client = client

        # storage.Client() automatically picks up credentials from:
        #   1. GOOGLE_APPLICATION_CREDENTIALS env var (path to a service account JSON)
        #   2. gcloud CLI login (gcloud auth application-default login)
        # On Cloud Run / GCE it uses the VM's service account automatically.
        self._gcs = storage.Client()

        # .bucket() creates a Bucket object but does NOT make a network call yet.
        # Network calls happen only when we actually read/write blobs.
        self._bucket = self._gcs.bucket(gcs_bucket)

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    # Spotify supports these time ranges for top tracks/artists.
    # We extract all three to capture different listening perspectives.
    VALID_TIME_RANGES = ("short_term", "medium_term", "long_term")

    def extract_recent(self, limit: int = 50) -> dict:
        """
        Fetch recently-played tracks and save them to GCS.

        Returns a metadata dict so callers (e.g. an Airflow task) know what
        was produced without having to re-read the file.

        GCS path: spotify/recent/YYYY/MM/DD/batch_<unix_ts>.json
        """
        extracted_at = datetime.now(UTC)
        tracks = self._client.get_recently_played(limit=limit)

        if not tracks:
            return {
                "total_tracks": 0,
                "gcs_path": None,
                "extracted_at": extracted_at.isoformat(),
                "partition": _date_partition(extracted_at),
            }

        gcs_path = self._save_to_gcs(tracks, extracted_at, data_type="recent")

        return {
            "total_tracks": len(tracks),
            "gcs_path": gcs_path,
            "extracted_at": extracted_at.isoformat(),
            "partition": _date_partition(extracted_at),
        }

    def extract_top(self, time_range: str = "medium_term", limit: int = 50) -> dict:
        """
        Fetch top tracks for a time range and save them to GCS.

        time_range: "short_term" (~4 weeks), "medium_term" (~6 months),
                    "long_term" (several years).
        GCS path: spotify/top/<time_range>/YYYY/MM/DD/batch_<unix_ts>.json
        """
        if time_range not in self.VALID_TIME_RANGES:
            raise ValueError(
                f"Invalid time_range '{time_range}'. "
                f"Must be one of: {', '.join(self.VALID_TIME_RANGES)}"
            )

        extracted_at = datetime.now(UTC)
        tracks = self._client.get_top_tracks(limit=limit, time_range=time_range)

        if not tracks:
            return {
                "total_tracks": 0,
                "gcs_path": None,
                "extracted_at": extracted_at.isoformat(),
                "partition": _date_partition(extracted_at),
                "time_range": time_range,
            }

        gcs_path = self._save_to_gcs(
            tracks, extracted_at, data_type=f"top/{time_range}"
        )

        return {
            "total_tracks": len(tracks),
            "gcs_path": gcs_path,
            "extracted_at": extracted_at.isoformat(),
            "partition": _date_partition(extracted_at),
            "time_range": time_range,
        }

    def extract_all(self) -> list[dict]:
        """
        Run a full extraction cycle: recent plays + top tracks for all 3 time
        ranges. This is what the Cloud Run job calls every 2 hours.

        Returns a list of 4 result dicts (one per extraction).
        """
        results = []
        results.append(self.extract_recent(limit=50))
        for time_range in self.VALID_TIME_RANGES:
            results.append(self.extract_top(time_range=time_range, limit=50))
        return results

    # ──────────────────────────────────────────────────────────────────────────
    # Private helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _save_to_gcs(
        self, tracks: list[dict], timestamp: datetime, data_type: str = "recent"
    ) -> str:
        """
        Serialize *tracks* as NDJSON and upload to GCS.

        Parameters
        ----------
        tracks:
            List of track dicts from the Spotify API.
        timestamp:
            Extraction moment — used for path partitioning and filename.
        data_type:
            Subdirectory under spotify/. Examples: "recent", "top/short_term".
            This separates different extraction types in GCS so BigQuery
            external tables can target each one independently.

        Returns
        -------
        Full gs:// path, e.g.:
            "gs://my-bucket/spotify/recent/2026/01/31/batch_1738281600.json"
        """
        unix_ts = int(timestamp.timestamp())
        partition = _date_partition(timestamp)
        object_name = f"spotify/{data_type}/{partition}/batch_{unix_ts}.json"

        ndjson_content = "\n".join(json.dumps(track) for track in tracks) + "\n"

        blob = self._bucket.blob(object_name)
        blob.upload_from_string(
            ndjson_content,
            content_type="application/x-ndjson",
        )

        return f"gs://{self._bucket.name}/{object_name}"


# ────────────────────────────────────────────────────────────────────────────
# Module-level helpers (no state — pure functions)
# ────────────────────────────────────────────────────────────────────────────


def _date_partition(dt: datetime) -> str:
    """
    Return a Hive-style date partition string from a datetime.

    Example: datetime(2026, 1, 31) → "2026/01/31"

    Why a separate function?
    - Pure function (no side effects) → trivial to unit test
    - Reusable: any other module can import it without pulling in GCS deps
    - The :02d format ensures single-digit months/days get zero-padded
      (BigQuery partition discovery relies on consistent path formats)
    """
    return f"{dt.year}/{dt.month:02d}/{dt.day:02d}"


def build_extractor_from_env() -> SpotifyExtractor:
    """
    Convenience factory: reads config from environment variables and returns
    a ready-to-use SpotifyExtractor.

    This is the "production" entry point — scripts and Cloud Run jobs call this
    instead of wiring up SpotifyConfig / SpotifyClient / SpotifyExtractor manually.

    Required env vars:
        SPOTIFY_CLIENT_ID
        SPOTIFY_CLIENT_SECRET
        GCS_RAW_BUCKET

    Optional:
        SPOTIFY_REDIRECT_URI  (default: http://127.0.0.1:8888/callback)
    """
    # Import here to avoid circular imports and keep the module testable
    # without needing real Spotify credentials.
    from echostream.spotify.config import SpotifyConfig  # noqa: PLC0415

    gcs_bucket = os.environ.get("GCS_RAW_BUCKET")
    if not gcs_bucket:
        raise ValueError(
            "Missing required environment variable: GCS_RAW_BUCKET\n"
            "Add it to your .env file (see .env.template for guidance)."
        )

    config = SpotifyConfig()
    client = SpotifyClient(config)
    return SpotifyExtractor(client, gcs_bucket)
