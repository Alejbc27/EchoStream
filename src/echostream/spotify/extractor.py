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
  gs://<bucket>/spotify/YYYY/MM/DD/batch_<unix_ts>.json
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

    def extract_recent(self, limit: int = 50) -> dict:
        """
        Fetch recently-played tracks and save them to GCS.

        Returns a metadata dict so callers (e.g. an Airflow task) know what
        was produced without having to re-read the file.

        Returns
        -------
        {
            "total_tracks": int,          # how many tracks were saved
            "gcs_path":     str,          # full gs:// URL of the file
            "extracted_at": str,          # ISO-8601 UTC timestamp
            "partition":    str,          # "YYYY/MM/DD" — useful for downstream steps
        }

        Raises
        ------
        RuntimeError if the Spotify call or GCS upload fails.
        We intentionally let the error bubble up: the caller (Airflow, Cloud
        Scheduler, etc.) should decide whether to retry or alert.
        """
        extracted_at = datetime.now(UTC)

        # ── 1. Fetch from Spotify ────────────────────────────────────────────
        tracks = self._client.get_recently_played(limit=limit)

        if not tracks:
            # Return early rather than uploading an empty file.
            # An empty file would look like a successful extraction and confuse
            # downstream jobs that check file size / row count.
            return {
                "total_tracks": 0,
                "gcs_path": None,
                "extracted_at": extracted_at.isoformat(),
                "partition": _date_partition(extracted_at),
            }

        # ── 2. Save to GCS ───────────────────────────────────────────────────
        gcs_path = self._save_to_gcs(tracks, extracted_at)

        return {
            "total_tracks": len(tracks),
            "gcs_path": gcs_path,
            "extracted_at": extracted_at.isoformat(),
            "partition": _date_partition(extracted_at),
        }

    # ──────────────────────────────────────────────────────────────────────────
    # Private helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _save_to_gcs(self, tracks: list[dict], timestamp: datetime) -> str:
        """
        Serialize *tracks* as NDJSON and upload to GCS.

        Parameters
        ----------
        tracks:
            List of track dicts as returned by SpotifyClient.get_recently_played().
        timestamp:
            The moment of extraction — used to build the date-partitioned path
            and the filename.  Passing it in (instead of calling datetime.now()
            here) ensures the path is consistent with extract_recent()'s metadata.

        Returns
        -------
        The full gs:// path, e.g.:
            "gs://my-bucket/spotify/2026/01/31/batch_1738281600.json"
        """
        # ── Build the object path ────────────────────────────────────────────
        # Object paths in GCS look like file paths but GCS has no real folders —
        # the slash is just part of the object name. However, the GCS UI and
        # BigQuery both treat slashes as folder separators, so the convention works.
        unix_ts = int(timestamp.timestamp())
        partition = _date_partition(timestamp)
        object_name = f"spotify/{partition}/batch_{unix_ts}.json"

        # ── Serialize as NDJSON ──────────────────────────────────────────────
        # json.dumps() converts a Python dict → a JSON string.
        # "\n".join(...) puts each track on its own line.
        # We add a trailing newline for POSIX compliance (some tools expect it).
        ndjson_content = "\n".join(json.dumps(track) for track in tracks) + "\n"

        # ── Upload ───────────────────────────────────────────────────────────
        # blob = a "pointer" to a GCS object (file).  Creating the blob object
        # doesn't make a network call yet — upload_from_string() does.
        blob = self._bucket.blob(object_name)
        blob.upload_from_string(
            ndjson_content,
            content_type="application/x-ndjson",
            # content_type is optional but good practice: GCS stores it as
            # metadata and some tools (like Cloud Functions triggers) use it.
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
