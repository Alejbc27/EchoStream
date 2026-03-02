"""
Tests for the Bronze layer: schemas + BronzeLoader.

What we're testing:
  SCHEMAS:
  - normalize_recent_track() → flattens nested Spotify recent-play JSON
  - normalize_top_track() → flattens top-track JSON + adds time_range
  - Edge cases: missing fields, empty artists list

  LOADER (BronzeLoader):
  - process_raw_file() with recent tracks → correct Parquet + metadata
  - process_raw_file() with top tracks → uses correct schema
  - process_raw_file() with empty file → returns zero, no upload
  - Data quality: deduplication, null track_id, invalid duration
  - NDJSON parsing: handles malformed lines gracefully
  - Parquet output: correct compression, column types

  PURE FUNCTIONS:
  - _detect_data_type() → extracts "recent" or "top/<range>" from path
  - _raw_to_bronze_path() → changes .json → .parquet
  - build_bronze_loader_from_env() → validates env vars

WHY mock GCS?
  Same reason as test_extractor.py: tests must run without credentials,
  without network, fast and reproducible. We mock the storage.Client and
  bucket objects, then verify our code calls them correctly.
"""

import json
from datetime import UTC, datetime
from io import BytesIO
from unittest.mock import MagicMock, call, patch

import polars as pl
import pytest

from echostream.bronze.loader import (
    BronzeLoader,
    _detect_data_type,
    _raw_to_bronze_path,
    build_bronze_loader_from_env,
)
from echostream.bronze.schemas import (
    RECENT_TRACKS_SCHEMA,
    TOP_TRACKS_SCHEMA,
    normalize_recent_track,
    normalize_top_track,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures — reusable test data
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def raw_recent_item() -> dict:
    """A single recently-played item as returned by the Spotify API."""
    return {
        "track": {
            "id": "4uLU6hMCjMI75M1A2tKUQC",
            "name": "Bohemian Rhapsody",
            "artists": [
                {"id": "1dfeR4HaWDbWqFHLkxsg1d", "name": "Queen"},
                {"id": "fake-artist-2", "name": "David Bowie"},
            ],
            "album": {
                "id": "6i6folBtxKV28WX3msQ4FE",
                "name": "A Night at the Opera",
            },
            "duration_ms": 354947,
            "explicit": False,
            "popularity": 92,
        },
        "played_at": "2026-03-02T14:30:00.000Z",
        "context": None,
    }


@pytest.fixture
def raw_recent_item_2() -> dict:
    """A second recent-play item for dedup/multi-record tests."""
    return {
        "track": {
            "id": "7tFiyTwD0nx5a1eklYtX2J",
            "name": "Blinding Lights",
            "artists": [{"id": "1Xyo4u8uXC1ZmMpatF05PJ", "name": "The Weeknd"}],
            "album": {"id": "4yP0hdKOZPNshxUOjY0cZj", "name": "After Hours"},
            "duration_ms": 200040,
            "explicit": False,
            "popularity": 88,
        },
        "played_at": "2026-03-02T14:25:00.000Z",
        "context": None,
    }


@pytest.fixture
def raw_top_item() -> dict:
    """A top-track item (note: NOT nested under 'track', different from recent)."""
    return {
        "id": "0VjIjW4GlUZAMYd2vXMi3b",
        "name": "Shape of You",
        "artists": [{"id": "6eUKZXaKkcviH0Ku9w2n3V", "name": "Ed Sheeran"}],
        "album": {"id": "3T4tUhGYeRNVUGevb0wThu", "name": "÷ (Divide)"},
        "duration_ms": 233713,
        "explicit": False,
        "popularity": 90,
    }


@pytest.fixture
def fake_ndjson_recent(raw_recent_item, raw_recent_item_2) -> str:
    """NDJSON content with two recent-play records."""
    return json.dumps(raw_recent_item) + "\n" + json.dumps(raw_recent_item_2) + "\n"


@pytest.fixture
def fake_ndjson_top(raw_top_item) -> str:
    """NDJSON content with one top-track record."""
    return json.dumps(raw_top_item) + "\n"


@pytest.fixture
def mock_raw_bucket() -> MagicMock:
    """Mock GCS bucket for the Raw layer."""
    bucket = MagicMock()
    bucket.name = "test-echostream-raw"
    return bucket


@pytest.fixture
def mock_bronze_bucket() -> MagicMock:
    """Mock GCS bucket for the Bronze layer."""
    bucket = MagicMock()
    bucket.name = "test-echostream-bronze"
    return bucket


@pytest.fixture
def loader(mock_raw_bucket, mock_bronze_bucket) -> BronzeLoader:
    """BronzeLoader with mocked GCS clients."""
    with patch("echostream.bronze.loader.storage.Client") as mock_client_class:
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # storage.Client().bucket(name) should return our mock buckets
        def bucket_selector(name: str) -> MagicMock:
            if "raw" in name:
                return mock_raw_bucket
            return mock_bronze_bucket

        mock_client.bucket.side_effect = bucket_selector

        ldr = BronzeLoader(
            raw_bucket="test-echostream-raw",
            bronze_bucket="test-echostream-bronze",
        )

    return ldr


# ─────────────────────────────────────────────────────────────────────────────
# Tests for normalize_recent_track()
# ─────────────────────────────────────────────────────────────────────────────


class TestNormalizeRecentTrack:
    def test_extracts_track_fields(self, raw_recent_item):
        result = normalize_recent_track(raw_recent_item)

        assert result["track_id"] == "4uLU6hMCjMI75M1A2tKUQC"
        assert result["track_name"] == "Bohemian Rhapsody"
        assert result["duration_ms"] == 354947
        assert result["explicit"] is False
        assert result["popularity"] == 92

    def test_extracts_primary_artist_only(self, raw_recent_item):
        """Even when track has multiple artists, we take only the first."""
        result = normalize_recent_track(raw_recent_item)

        assert result["artist_id"] == "1dfeR4HaWDbWqFHLkxsg1d"
        assert result["artist_name"] == "Queen"

    def test_extracts_album_fields(self, raw_recent_item):
        result = normalize_recent_track(raw_recent_item)

        assert result["album_id"] == "6i6folBtxKV28WX3msQ4FE"
        assert result["album_name"] == "A Night at the Opera"

    def test_preserves_played_at_timestamp(self, raw_recent_item):
        result = normalize_recent_track(raw_recent_item)

        assert result["played_at"] == "2026-03-02T14:30:00.000Z"

    def test_handles_missing_album(self):
        """Tracks without album metadata should get None for album fields."""
        item = {
            "track": {
                "id": "abc",
                "name": "Test",
                "artists": [{"id": "art1", "name": "Artist"}],
                "duration_ms": 180000,
                "explicit": False,
                "popularity": 50,
            },
            "played_at": "2026-01-01T00:00:00Z",
        }

        result = normalize_recent_track(item)

        assert result["album_id"] is None
        assert result["album_name"] is None

    def test_handles_empty_artists_list(self):
        """Tracks with empty artists list should get None for artist fields."""
        item = {
            "track": {
                "id": "abc",
                "name": "Test",
                "artists": [],
                "album": {"id": "alb1", "name": "Album"},
                "duration_ms": 180000,
                "explicit": False,
                "popularity": 50,
            },
            "played_at": "2026-01-01T00:00:00Z",
        }

        result = normalize_recent_track(item)

        assert result["artist_id"] is None
        assert result["artist_name"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Tests for normalize_top_track()
# ─────────────────────────────────────────────────────────────────────────────


class TestNormalizeTopTrack:
    def test_extracts_track_fields(self, raw_top_item):
        result = normalize_top_track(raw_top_item, "medium_term")

        assert result["track_id"] == "0VjIjW4GlUZAMYd2vXMi3b"
        assert result["track_name"] == "Shape of You"
        assert result["duration_ms"] == 233713
        assert result["popularity"] == 90

    def test_adds_time_range_column(self, raw_top_item):
        result = normalize_top_track(raw_top_item, "short_term")
        assert result["time_range"] == "short_term"

    def test_does_not_have_played_at(self, raw_top_item):
        """Top tracks are rankings, not individual plays — no timestamp."""
        result = normalize_top_track(raw_top_item, "long_term")
        assert "played_at" not in result

    def test_extracts_primary_artist(self, raw_top_item):
        result = normalize_top_track(raw_top_item, "medium_term")

        assert result["artist_id"] == "6eUKZXaKkcviH0Ku9w2n3V"
        assert result["artist_name"] == "Ed Sheeran"


# ─────────────────────────────────────────────────────────────────────────────
# Tests for _detect_data_type()
# ─────────────────────────────────────────────────────────────────────────────


class TestDetectDataType:
    def test_detects_recent(self):
        path = "spotify/recent/2026/03/02/batch_123.json"
        assert _detect_data_type(path) == "recent"

    def test_detects_top_short_term(self):
        path = "spotify/top/short_term/2026/03/02/batch_123.json"
        assert _detect_data_type(path) == "top/short_term"

    def test_detects_top_medium_term(self):
        path = "spotify/top/medium_term/2026/03/02/batch_123.json"
        assert _detect_data_type(path) == "top/medium_term"

    def test_detects_top_long_term(self):
        path = "spotify/top/long_term/2026/03/02/batch_123.json"
        assert _detect_data_type(path) == "top/long_term"

    def test_raises_on_unknown_path(self):
        with pytest.raises(ValueError, match="Cannot detect data type"):
            _detect_data_type("unknown/path/file.json")

    def test_raises_on_empty_string(self):
        with pytest.raises(ValueError, match="Cannot detect data type"):
            _detect_data_type("")


# ─────────────────────────────────────────────────────────────────────────────
# Tests for _raw_to_bronze_path()
# ─────────────────────────────────────────────────────────────────────────────


class TestRawToBronzePath:
    def test_changes_json_to_parquet(self):
        raw = "spotify/recent/2026/03/02/batch_123.json"
        assert _raw_to_bronze_path(raw) == "spotify/recent/2026/03/02/batch_123.parquet"

    def test_preserves_directory_structure(self):
        raw = "spotify/top/short_term/2026/03/02/batch_456.json"
        expected = "spotify/top/short_term/2026/03/02/batch_456.parquet"
        assert _raw_to_bronze_path(raw) == expected

    def test_handles_path_without_json_extension(self):
        raw = "spotify/recent/2026/03/02/batch_123"
        assert _raw_to_bronze_path(raw) == "spotify/recent/2026/03/02/batch_123.parquet"


# ─────────────────────────────────────────────────────────────────────────────
# Tests for BronzeLoader.process_raw_file() — recent tracks
# ─────────────────────────────────────────────────────────────────────────────


class TestProcessRawFileRecent:
    def test_returns_correct_metadata(
        self, loader, mock_raw_bucket, fake_ndjson_recent
    ):
        blob = MagicMock()
        blob.download_as_text.return_value = fake_ndjson_recent
        mock_raw_bucket.blob.return_value = blob

        result = loader.process_raw_file("spotify/recent/2026/03/02/batch_123.json")

        assert result["records_in"] == 2
        assert result["records_out"] == 2
        assert result["data_type"] == "recent"
        assert result["dropped_records"] == 0
        assert result["bronze_path"] is not None
        assert result["bronze_path"].endswith(".parquet")
        assert result["processed_at"] is not None

    def test_bronze_path_uses_bronze_bucket(
        self, loader, mock_raw_bucket, mock_bronze_bucket, fake_ndjson_recent
    ):
        blob = MagicMock()
        blob.download_as_text.return_value = fake_ndjson_recent
        mock_raw_bucket.blob.return_value = blob

        result = loader.process_raw_file("spotify/recent/2026/03/02/batch_123.json")

        assert result["bronze_path"].startswith("gs://test-echostream-bronze/")

    def test_uploads_parquet_to_bronze_bucket(
        self, loader, mock_raw_bucket, mock_bronze_bucket, fake_ndjson_recent
    ):
        blob = MagicMock()
        blob.download_as_text.return_value = fake_ndjson_recent
        mock_raw_bucket.blob.return_value = blob

        loader.process_raw_file("spotify/recent/2026/03/02/batch_123.json")

        # Verify a blob was created in the bronze bucket
        mock_bronze_bucket.blob.assert_called_once_with(
            "spotify/recent/2026/03/02/batch_123.parquet"
        )
        # Verify upload was called
        bronze_blob = mock_bronze_bucket.blob.return_value
        bronze_blob.upload_from_string.assert_called_once()

    def test_uploaded_content_is_valid_parquet(
        self, loader, mock_raw_bucket, mock_bronze_bucket, fake_ndjson_recent
    ):
        """Verify the uploaded bytes are actually a valid Parquet file."""
        blob = MagicMock()
        blob.download_as_text.return_value = fake_ndjson_recent
        mock_raw_bucket.blob.return_value = blob

        loader.process_raw_file("spotify/recent/2026/03/02/batch_123.json")

        bronze_blob = mock_bronze_bucket.blob.return_value
        uploaded_bytes = bronze_blob.upload_from_string.call_args[0][0]

        # Read the Parquet bytes back into a DataFrame
        df = pl.read_parquet(BytesIO(uploaded_bytes))

        assert len(df) == 2
        assert "track_id" in df.columns
        assert "track_name" in df.columns
        assert "played_at" in df.columns
        assert "artist_name" in df.columns

    def test_parquet_has_correct_schema(
        self, loader, mock_raw_bucket, mock_bronze_bucket, fake_ndjson_recent
    ):
        """Verify that the Parquet file columns match RECENT_TRACKS_SCHEMA."""
        blob = MagicMock()
        blob.download_as_text.return_value = fake_ndjson_recent
        mock_raw_bucket.blob.return_value = blob

        loader.process_raw_file("spotify/recent/2026/03/02/batch_123.json")

        bronze_blob = mock_bronze_bucket.blob.return_value
        uploaded_bytes = bronze_blob.upload_from_string.call_args[0][0]
        df = pl.read_parquet(BytesIO(uploaded_bytes))

        expected_columns = set(RECENT_TRACKS_SCHEMA.keys())
        assert set(df.columns) == expected_columns

    def test_sorts_by_played_at(
        self, loader, mock_raw_bucket, mock_bronze_bucket, fake_ndjson_recent
    ):
        """Recent tracks should be sorted chronologically."""
        blob = MagicMock()
        blob.download_as_text.return_value = fake_ndjson_recent
        mock_raw_bucket.blob.return_value = blob

        loader.process_raw_file("spotify/recent/2026/03/02/batch_123.json")

        bronze_blob = mock_bronze_bucket.blob.return_value
        uploaded_bytes = bronze_blob.upload_from_string.call_args[0][0]
        df = pl.read_parquet(BytesIO(uploaded_bytes))

        played_at_values = df["played_at"].to_list()
        assert played_at_values == sorted(played_at_values)


# ─────────────────────────────────────────────────────────────────────────────
# Tests for BronzeLoader.process_raw_file() — top tracks
# ─────────────────────────────────────────────────────────────────────────────


class TestProcessRawFileTop:
    def test_returns_correct_metadata(self, loader, mock_raw_bucket, fake_ndjson_top):
        blob = MagicMock()
        blob.download_as_text.return_value = fake_ndjson_top
        mock_raw_bucket.blob.return_value = blob

        result = loader.process_raw_file(
            "spotify/top/short_term/2026/03/02/batch_456.json"
        )

        assert result["records_in"] == 1
        assert result["records_out"] == 1
        assert result["data_type"] == "top/short_term"

    def test_parquet_has_time_range_column(
        self, loader, mock_raw_bucket, mock_bronze_bucket, fake_ndjson_top
    ):
        blob = MagicMock()
        blob.download_as_text.return_value = fake_ndjson_top
        mock_raw_bucket.blob.return_value = blob

        loader.process_raw_file("spotify/top/short_term/2026/03/02/batch_456.json")

        bronze_blob = mock_bronze_bucket.blob.return_value
        uploaded_bytes = bronze_blob.upload_from_string.call_args[0][0]
        df = pl.read_parquet(BytesIO(uploaded_bytes))

        assert "time_range" in df.columns
        assert df["time_range"][0] == "short_term"

    def test_parquet_does_not_have_played_at(
        self, loader, mock_raw_bucket, mock_bronze_bucket, fake_ndjson_top
    ):
        blob = MagicMock()
        blob.download_as_text.return_value = fake_ndjson_top
        mock_raw_bucket.blob.return_value = blob

        loader.process_raw_file("spotify/top/medium_term/2026/03/02/batch_456.json")

        bronze_blob = mock_bronze_bucket.blob.return_value
        uploaded_bytes = bronze_blob.upload_from_string.call_args[0][0]
        df = pl.read_parquet(BytesIO(uploaded_bytes))

        assert "played_at" not in df.columns


# ─────────────────────────────────────────────────────────────────────────────
# Tests for BronzeLoader.process_raw_file() — empty files
# ─────────────────────────────────────────────────────────────────────────────


class TestProcessRawFileEmpty:
    def test_returns_zero_for_empty_file(self, loader, mock_raw_bucket):
        blob = MagicMock()
        blob.download_as_text.return_value = ""
        mock_raw_bucket.blob.return_value = blob

        result = loader.process_raw_file("spotify/recent/2026/03/02/batch_123.json")

        assert result["records_in"] == 0
        assert result["records_out"] == 0
        assert result["bronze_path"] is None
        assert result["dropped_records"] == 0

    def test_does_not_upload_for_empty_file(
        self, loader, mock_raw_bucket, mock_bronze_bucket
    ):
        blob = MagicMock()
        blob.download_as_text.return_value = ""
        mock_raw_bucket.blob.return_value = blob

        loader.process_raw_file("spotify/recent/2026/03/02/batch_123.json")

        mock_bronze_bucket.blob.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Tests for data quality rules (_clean)
# ─────────────────────────────────────────────────────────────────────────────


class TestDataQuality:
    def test_deduplicates_recent_tracks_by_track_and_timestamp(
        self, loader, mock_raw_bucket, mock_bronze_bucket, raw_recent_item
    ):
        """Same track + same played_at = duplicate → keep only one."""
        ndjson = json.dumps(raw_recent_item) + "\n" + json.dumps(raw_recent_item) + "\n"

        blob = MagicMock()
        blob.download_as_text.return_value = ndjson
        mock_raw_bucket.blob.return_value = blob

        result = loader.process_raw_file("spotify/recent/2026/03/02/batch_123.json")

        assert result["records_in"] == 2
        assert result["records_out"] == 1
        assert result["dropped_records"] == 1

    def test_drops_records_with_null_track_id(
        self, loader, mock_raw_bucket, mock_bronze_bucket
    ):
        """Tracks with null IDs (removed from Spotify catalog) should be filtered."""
        item_no_id = {
            "track": {
                "id": None,
                "name": "Deleted Track",
                "artists": [{"id": "art1", "name": "Artist"}],
                "album": {"id": "alb1", "name": "Album"},
                "duration_ms": 180000,
                "explicit": False,
                "popularity": 0,
            },
            "played_at": "2026-03-02T12:00:00Z",
        }
        item_valid = {
            "track": {
                "id": "valid123",
                "name": "Valid Track",
                "artists": [{"id": "art2", "name": "Artist 2"}],
                "album": {"id": "alb2", "name": "Album 2"},
                "duration_ms": 200000,
                "explicit": False,
                "popularity": 75,
            },
            "played_at": "2026-03-02T13:00:00Z",
        }
        ndjson = json.dumps(item_no_id) + "\n" + json.dumps(item_valid) + "\n"

        blob = MagicMock()
        blob.download_as_text.return_value = ndjson
        mock_raw_bucket.blob.return_value = blob

        result = loader.process_raw_file("spotify/recent/2026/03/02/batch_123.json")

        assert result["records_out"] == 1
        assert result["dropped_records"] == 1

    def test_drops_records_with_zero_duration(
        self, loader, mock_raw_bucket, mock_bronze_bucket
    ):
        """Tracks with 0 or negative duration are invalid."""
        item_zero = {
            "track": {
                "id": "track_zero",
                "name": "Zero Duration",
                "artists": [{"id": "art1", "name": "Artist"}],
                "album": {"id": "alb1", "name": "Album"},
                "duration_ms": 0,
                "explicit": False,
                "popularity": 50,
            },
            "played_at": "2026-03-02T12:00:00Z",
        }
        item_valid = {
            "track": {
                "id": "track_valid",
                "name": "Valid Track",
                "artists": [{"id": "art2", "name": "Artist 2"}],
                "album": {"id": "alb2", "name": "Album 2"},
                "duration_ms": 200000,
                "explicit": False,
                "popularity": 75,
            },
            "played_at": "2026-03-02T13:00:00Z",
        }
        ndjson = json.dumps(item_zero) + "\n" + json.dumps(item_valid) + "\n"

        blob = MagicMock()
        blob.download_as_text.return_value = ndjson
        mock_raw_bucket.blob.return_value = blob

        result = loader.process_raw_file("spotify/recent/2026/03/02/batch_123.json")

        assert result["records_out"] == 1
        assert result["dropped_records"] == 1

    def test_handles_malformed_ndjson_lines(
        self, loader, mock_raw_bucket, mock_bronze_bucket, raw_recent_item
    ):
        """Malformed JSON lines should be skipped, not crash the loader."""
        ndjson = (
            json.dumps(raw_recent_item)
            + "\n"
            + "THIS IS NOT VALID JSON\n"
            + json.dumps(raw_recent_item).replace(
                raw_recent_item["played_at"],
                "2026-03-02T15:00:00.000Z",
            )
            + "\n"
        )

        blob = MagicMock()
        blob.download_as_text.return_value = ndjson
        mock_raw_bucket.blob.return_value = blob

        # Should not raise — just skip the bad line
        result = loader.process_raw_file("spotify/recent/2026/03/02/batch_123.json")

        # 2 valid records parsed (bad line skipped), then dedup may apply
        assert result["records_in"] == 2


# ─────────────────────────────────────────────────────────────────────────────
# Tests for build_bronze_loader_from_env()
# ─────────────────────────────────────────────────────────────────────────────


class TestBuildBronzeLoaderFromEnv:
    def test_raises_if_raw_bucket_missing(self, monkeypatch):
        monkeypatch.delenv("GCS_RAW_BUCKET", raising=False)
        monkeypatch.delenv("GCS_BRONZE_BUCKET", raising=False)

        with pytest.raises(ValueError, match="GCS_RAW_BUCKET"):
            build_bronze_loader_from_env()

    def test_raises_if_bronze_bucket_missing(self, monkeypatch):
        monkeypatch.setenv("GCS_RAW_BUCKET", "my-raw-bucket")
        monkeypatch.delenv("GCS_BRONZE_BUCKET", raising=False)

        with pytest.raises(ValueError, match="GCS_BRONZE_BUCKET"):
            build_bronze_loader_from_env()

    def test_builds_loader_with_env_vars(self, monkeypatch):
        monkeypatch.setenv("GCS_RAW_BUCKET", "my-raw-bucket")
        monkeypatch.setenv("GCS_BRONZE_BUCKET", "my-bronze-bucket")

        with patch("echostream.bronze.loader.storage.Client"):
            result = build_bronze_loader_from_env()

        assert isinstance(result, BronzeLoader)
