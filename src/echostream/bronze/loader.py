"""
BronzeLoader: converts Raw NDJSON files into typed Parquet (Raw → Bronze).

WHERE THIS FITS IN THE PIPELINE:
─────────────────────────────────────────────────────────────────────────
  Cloud Scheduler → Cloud Run → SpotifyExtractor → GCS Raw (NDJSON)
                                                         ↓
                                              BronzeLoader (THIS FILE)
                                                         ↓
                                                  GCS Bronze (Parquet)
                                                         ↓
                                               BigQuery / dbt (Phase 2)
─────────────────────────────────────────────────────────────────────────

WHY THIS STEP?
Raw NDJSON files are great for preserving the exact API response, but they're
terrible for analytics:
  - No schema enforcement → "duration_ms" could be a string in one file and int in another
  - No compression → JSON is verbose ({"key": "value"} overhead per field)
  - No columnar access → must read entire file even if you only need one column

Parquet solves all three:
  - Typed columns → catch schema mismatches early
  - Snappy compression → typically 5-10x smaller than JSON
  - Columnar format → BigQuery/Polars read only the columns they need

WHAT THIS MODULE DOES:
  1. Lists NDJSON files in the Raw GCS bucket
  2. Downloads and parses each file
  3. Normalizes nested JSON → flat rows using schemas.py
  4. Creates a Polars DataFrame with enforced types
  5. Applies data quality rules (dedup, null checks)
  6. Writes Parquet to the Bronze GCS bucket

GCS PATH MAPPING:
  Raw:    gs://<raw-bucket>/spotify/recent/2026/03/02/batch_1709388000.json
  Bronze: gs://<bronze-bucket>/spotify/recent/2026/03/02/batch_1709388000.parquet

  The path structure is preserved so you can always trace a Bronze file
  back to its Raw source. Only the extension changes (.json → .parquet).
"""

import json
import logging
import os
import re
from datetime import UTC, datetime
from io import BytesIO

import polars as pl
from google.cloud import storage

from echostream.bronze.schemas import (
    RECENT_TRACKS_SCHEMA,
    TOP_TRACKS_SCHEMA,
    normalize_recent_track,
    normalize_top_track,
)

logger = logging.getLogger(__name__)


class BronzeLoader:
    """
    Converts Raw NDJSON files from GCS into typed Parquet in the Bronze layer.

    Usage:
        loader = BronzeLoader(raw_bucket="my-raw", bronze_bucket="my-bronze")
        result = loader.process_raw_file("spotify/recent/2026/03/02/batch_123.json")
        print(result)
        # {"records_in": 50, "records_out": 48, "bronze_path": "gs://...", ...}

    Why dependency injection for buckets?
        Tests can pass mock bucket names without needing real GCS credentials.
        In production, bucket names come from environment variables.
    """

    def __init__(self, raw_bucket: str, bronze_bucket: str) -> None:
        """
        Parameters
        ----------
        raw_bucket:
            Name of the GCS bucket containing Raw NDJSON files.
            Example: "echostream-2026-echostream-raw-dev"

        bronze_bucket:
            Name of the GCS bucket where Parquet files will be written.
            Example: "echostream-2026-echostream-bronze-dev"
        """
        self._gcs = storage.Client()
        self._raw_bucket = self._gcs.bucket(raw_bucket)
        self._bronze_bucket = self._gcs.bucket(bronze_bucket)

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    def process_raw_file(self, raw_blob_name: str) -> dict:
        """
        Convert a single Raw NDJSON file to Bronze Parquet.

        Parameters
        ----------
        raw_blob_name:
            The object name inside the raw bucket (NOT the full gs:// path).
            Example: "spotify/recent/2026/03/02/batch_1709388000.json"

        Returns
        -------
        Metadata dict with: records_in, records_out, bronze_path, processed_at,
        data_type, and dropped_records.

        WHY return metadata?
        Callers (future orchestrator / Airflow DAG) need to know:
        - How many records were processed (for alerting on empty extractions)
        - How many were dropped (for data quality monitoring)
        - Where the output landed (for downstream pipeline steps)
        """
        processed_at = datetime.now(UTC)
        logger.info(f"Processing raw file: {raw_blob_name}")

        # Step 1: Detect data type from the path
        data_type = _detect_data_type(raw_blob_name)
        logger.info(f"Detected data type: {data_type}")

        # Step 2: Download and parse NDJSON
        raw_records = self._download_ndjson(raw_blob_name)
        records_in = len(raw_records)
        logger.info(f"Parsed {records_in} records from NDJSON")

        if records_in == 0:
            return {
                "records_in": 0,
                "records_out": 0,
                "bronze_path": None,
                "processed_at": processed_at.isoformat(),
                "data_type": data_type,
                "dropped_records": 0,
            }

        # Step 3: Normalize to flat schema
        df = self._normalize(raw_records, data_type)

        # Step 4: Apply data quality rules
        df = self._clean(df, data_type)
        records_out = len(df)
        dropped = records_in - records_out

        if dropped > 0:
            logger.warning(f"Dropped {dropped} records during cleaning")

        # Step 5: Upload Parquet to Bronze bucket
        bronze_blob_name = _raw_to_bronze_path(raw_blob_name)
        self._upload_parquet(df, bronze_blob_name)

        bronze_path = f"gs://{self._bronze_bucket.name}/{bronze_blob_name}"
        logger.info(f"Bronze file written: {bronze_path} ({records_out} records)")

        return {
            "records_in": records_in,
            "records_out": records_out,
            "bronze_path": bronze_path,
            "processed_at": processed_at.isoformat(),
            "data_type": data_type,
            "dropped_records": dropped,
        }

    def process_all_recent(self, date_prefix: str | None = None) -> list[dict]:
        """
        Process all Raw NDJSON files for recently-played tracks.

        Parameters
        ----------
        date_prefix:
            Optional date prefix to filter files, e.g. "2026/03/02".
            If None, processes all files under spotify/recent/.

        Returns
        -------
        List of metadata dicts (one per file processed).

        WHY process by prefix?
        In production, this will be called by a scheduler that passes today's
        date. We don't want to re-process all historical files every run.
        """
        prefix = "spotify/recent/"
        if date_prefix:
            prefix += date_prefix

        return self._process_prefix(prefix)

    def process_all_top(self, date_prefix: str | None = None) -> list[dict]:
        """
        Process all Raw NDJSON files for top tracks (all time ranges).

        Same pattern as process_all_recent — filters by spotify/top/ prefix.
        """
        prefix = "spotify/top/"
        if date_prefix:
            prefix += date_prefix

        return self._process_prefix(prefix)

    # ──────────────────────────────────────────────────────────────────────────
    # Private helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _process_prefix(self, prefix: str) -> list[dict]:
        """List all .json blobs under a prefix and process each one."""
        blobs = self._raw_bucket.list_blobs(prefix=prefix)
        results = []

        for blob in blobs:
            if blob.name.endswith(".json"):
                result = self.process_raw_file(blob.name)
                results.append(result)

        return results

    def _download_ndjson(self, blob_name: str) -> list[dict]:
        """
        Download a blob from the Raw bucket and parse it as NDJSON.

        NDJSON = one JSON object per line. We parse line by line instead of
        json.loads(entire_file) because:
          - More memory efficient for large files
          - One bad line doesn't break the entire parse
          - Matches how BigQuery reads NDJSON files
        """
        blob = self._raw_bucket.blob(blob_name)
        content = blob.download_as_text()

        records = []
        for line_number, line in enumerate(content.splitlines(), start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                records.append(json.loads(stripped))
            except json.JSONDecodeError as e:
                # Log the bad line but don't crash — partial data is better than no data
                logger.warning(
                    f"Skipping malformed JSON at line {line_number} in {blob_name}: {e}"
                )

        return records

    def _normalize(self, raw_records: list[dict], data_type: str) -> pl.DataFrame:
        """
        Convert raw API dicts to a typed Polars DataFrame.

        The normalization function and schema depend on the data type:
          - "recent" → normalize_recent_track + RECENT_TRACKS_SCHEMA
          - "top/<time_range>" → normalize_top_track + TOP_TRACKS_SCHEMA

        WHY Polars (not Pandas)?
        - Polars is 10-20x faster (written in Rust, uses all CPU cores)
        - Strict typing: won't silently cast "42" to 42
        - Arrow-native: Parquet write is zero-copy (no conversion overhead)
        """
        if data_type == "recent":
            normalized = [normalize_recent_track(r) for r in raw_records]
            return pl.DataFrame(normalized, schema=RECENT_TRACKS_SCHEMA)
        else:
            # data_type is "top/<time_range>" — extract the time_range
            time_range = data_type.split("/", 1)[1] if "/" in data_type else "unknown"
            normalized = [normalize_top_track(r, time_range) for r in raw_records]
            return pl.DataFrame(normalized, schema=TOP_TRACKS_SCHEMA)

    def _clean(self, df: pl.DataFrame, data_type: str) -> pl.DataFrame:
        """
        Apply data quality rules to the DataFrame.

        Rules:
        1. Remove rows where track_id is null (can't identify the track)
        2. Remove rows where duration_ms <= 0 (invalid tracks)
        3. For recent tracks: deduplicate by (track_id, played_at)
           → same track at same timestamp = API returning duplicates
        4. For recent tracks: sort by played_at (helps Parquet compression
           and partition pruning in downstream queries)

        WHY these specific rules?
        - Null track_ids happen when Spotify removes a track from its catalog
        - duration_ms <= 0 means the track metadata is corrupt
        - Duplicates happen when the extraction job runs twice close together
          and fetches overlapping windows of recently-played tracks
        """
        # Rule 1: track_id must exist
        df = df.filter(pl.col("track_id").is_not_null())

        # Rule 2: valid duration
        df = df.filter(pl.col("duration_ms") > 0)

        if data_type == "recent":
            # Rule 3: deduplicate by track + timestamp
            df = df.unique(subset=["track_id", "played_at"])

            # Rule 4: sort chronologically
            df = df.sort("played_at")

        return df

    def _upload_parquet(self, df: pl.DataFrame, bronze_blob_name: str) -> None:
        """
        Write a Polars DataFrame to GCS as Parquet.

        WHY Snappy compression?
        - Fast to compress AND decompress (important for queries)
        - Good compression ratio (~5x for this data)
        - Industry standard for analytics workloads
        - Alternative: zstd (better ratio, slightly slower) or gzip (best ratio, slowest)

        WHY write to bytes buffer instead of temp file?
        - No filesystem side effects (cleaner for testing)
        - No risk of orphaned temp files if the process crashes
        - Slightly faster (no disk I/O roundtrip)
        """
        # Polars write_parquet requires a file path or buffer as the first arg.
        # We use BytesIO to avoid temp files on disk — cleaner and crash-safe.
        buffer = BytesIO()
        df.write_parquet(buffer, compression="snappy")
        parquet_bytes = buffer.getvalue()

        blob = self._bronze_bucket.blob(bronze_blob_name)
        blob.upload_from_string(
            parquet_bytes,
            content_type="application/octet-stream",
        )


# ────────────────────────────────────────────────────────────────────────────
# Module-level helpers (pure functions — no state, easy to test)
# ────────────────────────────────────────────────────────────────────────────


def _detect_data_type(blob_name: str) -> str:
    """
    Infer the data type from a Raw GCS blob path.

    Examples:
        "spotify/recent/2026/03/02/batch_123.json"       → "recent"
        "spotify/top/short_term/2026/03/02/batch_123.json" → "top/short_term"
        "spotify/top/long_term/2026/03/02/batch_123.json"  → "top/long_term"

    The data type determines which schema and normalization function to use.
    """
    # Match the path pattern: spotify/<data_type>/YYYY/MM/DD/...
    match = re.match(r"spotify/(recent|top/\w+)/\d{4}/", blob_name)
    if match:
        return match.group(1)

    raise ValueError(
        f"Cannot detect data type from path: {blob_name}\n"
        "Expected pattern: spotify/recent/YYYY/... or spotify/top/<range>/YYYY/..."
    )


def _raw_to_bronze_path(raw_blob_name: str) -> str:
    """
    Convert a Raw blob path to its corresponding Bronze blob path.

    Simply changes the file extension from .json to .parquet.
    The rest of the path structure is preserved for traceability.

    Examples:
        "spotify/recent/2026/03/02/batch_123.json"
        → "spotify/recent/2026/03/02/batch_123.parquet"
    """
    if raw_blob_name.endswith(".json"):
        return raw_blob_name[:-5] + ".parquet"
    return raw_blob_name + ".parquet"


def build_bronze_loader_from_env() -> BronzeLoader:
    """
    Factory: reads bucket names from environment variables and returns
    a ready-to-use BronzeLoader.

    Required env vars:
        GCS_RAW_BUCKET    — Raw layer bucket name
        GCS_BRONZE_BUCKET — Bronze layer bucket name

    This follows the same pattern as build_extractor_from_env() in the
    Spotify module — environment-based configuration for production use.
    """
    raw_bucket = os.environ.get("GCS_RAW_BUCKET")
    if not raw_bucket:
        raise ValueError(
            "Missing required environment variable: GCS_RAW_BUCKET\n"
            "Add it to your .env file (see .env.template for guidance)."
        )

    bronze_bucket = os.environ.get("GCS_BRONZE_BUCKET")
    if not bronze_bucket:
        raise ValueError(
            "Missing required environment variable: GCS_BRONZE_BUCKET\n"
            "Add it to your .env file (see .env.template for guidance)."
        )

    return BronzeLoader(raw_bucket=raw_bucket, bronze_bucket=bronze_bucket)
