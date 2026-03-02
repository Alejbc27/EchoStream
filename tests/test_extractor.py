"""
Tests for SpotifyExtractor.

WHY mock GCS and Spotify instead of calling them for real?
-----------------------------------------------------------
Unit tests must be:
  1. Fast   — a real GCS upload takes ~500ms; we want tests in <1s total
  2. Isolated — no network dependency means tests pass in CI, on a plane, anywhere
  3. Deterministic — a real Spotify call returns DIFFERENT data each time

By "mocking" we mean: replacing the real object with a fake one that we control.
pytest-mock gives us a `mocker` fixture that makes this easy.

What we're testing:
  - extract_recent() with real tracks → saves to GCS, returns correct metadata
  - extract_recent() with empty list  → returns early, does NOT upload
  - _save_to_gcs()                    → builds the correct GCS path
  - _date_partition()                 → utility function, pure logic
  - build_extractor_from_env()        → raises if GCS_RAW_BUCKET missing
"""

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from echostream.spotify.extractor import (
    SpotifyExtractor,
    _date_partition,
    build_extractor_from_env,
)

# ─────────────────────────────────────────────────────────────────────────────
# Fixtures — reusable test setup (pytest injects them by name)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def fake_tracks() -> list[dict]:
    """
    A minimal list of track dicts that look like what Spotify returns.
    We only include the fields the extractor cares about (it saves everything
    as-is, so this is intentionally small).
    """
    return [
        {
            "track": {"name": "Bohemian Rhapsody", "id": "spotify:track:001"},
            "played_at": "2026-01-31T12:00:00.000Z",
            "context": None,
        },
        {
            "track": {"name": "Hotel California", "id": "spotify:track:002"},
            "played_at": "2026-01-31T11:45:00.000Z",
            "context": None,
        },
    ]


@pytest.fixture
def mock_spotify_client(fake_tracks) -> MagicMock:
    """
    A fake SpotifyClient that returns our fake_tracks.

    MagicMock creates an object that:
    - Has any attribute you access (returns another MagicMock)
    - Records every call made to it (so we can assert later)
    - Can be configured to return specific values

    We configure get_recently_played to return fake_tracks.
    """
    client = MagicMock()
    client.get_recently_played.return_value = fake_tracks
    return client


@pytest.fixture
def mock_gcs_bucket() -> MagicMock:
    """
    A fake GCS Bucket with a fake blob that records upload calls.
    We can check what content was uploaded and what path was used.
    """
    blob = MagicMock()
    bucket = MagicMock()
    bucket.name = "test-echostream-raw"
    bucket.blob.return_value = blob
    return bucket


@pytest.fixture
def extractor(mock_spotify_client, mock_gcs_bucket) -> SpotifyExtractor:
    """
    A SpotifyExtractor wired with fake dependencies.

    We bypass __init__'s storage.Client() call by:
    1. Creating the extractor with a fake bucket name
    2. Patching storage.Client so it doesn't try to contact GCP
    3. Replacing ._bucket with our mock directly after construction

    This pattern (create → patch → replace) is common when the constructor
    does side-effectful work (network calls, file I/O) we don't want in tests.
    """
    with patch("echostream.spotify.extractor.storage.Client"):
        ex = SpotifyExtractor(mock_spotify_client, "test-echostream-raw")

    # Replace the internal bucket with our controllable mock
    ex._bucket = mock_gcs_bucket
    return ex


# ─────────────────────────────────────────────────────────────────────────────
# Tests for _date_partition() — pure function, simplest to test first
# ─────────────────────────────────────────────────────────────────────────────


class TestDatePartition:
    """Pure-function tests: no mocks needed, just inputs and expected outputs."""

    def test_formats_single_digit_month_and_day(self):
        """Month 1, day 5 → '2026/01/05' (not '2026/1/5')."""
        dt = datetime(2026, 1, 5, tzinfo=UTC)
        assert _date_partition(dt) == "2026/01/05"

    def test_formats_double_digit_month_and_day(self):
        """Month 12, day 31 → '2026/12/31'."""
        dt = datetime(2026, 12, 31, tzinfo=UTC)
        assert _date_partition(dt) == "2026/12/31"

    def test_year_is_preserved(self):
        """Year comes first, verbatim."""
        dt = datetime(2025, 6, 15, tzinfo=UTC)
        assert _date_partition(dt).startswith("2025/")


# ─────────────────────────────────────────────────────────────────────────────
# Tests for extract_recent()
# ─────────────────────────────────────────────────────────────────────────────


class TestExtractRecent:
    def test_returns_correct_metadata(self, extractor, fake_tracks):
        """
        The happy path: Spotify returns 2 tracks → we get back a dict
        with the right keys and values.
        """
        result = extractor.extract_recent()

        assert result["total_tracks"] == len(fake_tracks)
        assert result["gcs_path"] is not None
        assert result["gcs_path"].startswith("gs://test-echostream-raw/spotify/")
        assert result["extracted_at"] is not None  # ISO-8601 string
        assert "/" in result["partition"]  # "YYYY/MM/DD"

    def test_calls_spotify_client_with_default_limit(self, extractor):
        """
        extract_recent() should ask for 50 tracks by default (Spotify's max).
        We verify the client was called with the right argument.
        """
        extractor.extract_recent()
        extractor._client.get_recently_played.assert_called_once_with(limit=50)

    def test_calls_spotify_client_with_custom_limit(self, extractor):
        """Passing limit=10 should forward that to the client."""
        extractor.extract_recent(limit=10)
        extractor._client.get_recently_played.assert_called_once_with(limit=10)

    def test_uploads_to_gcs(self, extractor):
        """After a successful fetch, upload_from_string should be called once."""
        extractor.extract_recent()
        # bucket.blob() was called once to get the blob handle
        extractor._bucket.blob.assert_called_once()
        # blob.upload_from_string() was called once with the NDJSON content
        extractor._bucket.blob.return_value.upload_from_string.assert_called_once()

    def test_uploaded_content_is_valid_ndjson(self, extractor, fake_tracks):
        """
        Each line in the uploaded content should be a valid JSON object.
        This ensures BigQuery can load the file correctly.
        """
        extractor.extract_recent()

        # Retrieve what was actually uploaded
        blob = extractor._bucket.blob.return_value
        upload_call_args = blob.upload_from_string.call_args
        uploaded_content: str = upload_call_args[0][0]  # first positional arg

        lines = [line for line in uploaded_content.strip().splitlines() if line]
        assert len(lines) == len(fake_tracks)

        for line in lines:
            parsed = json.loads(line)  # raises json.JSONDecodeError if invalid
            assert "track" in parsed
            assert "played_at" in parsed

    def test_gcs_path_contains_date_partition(self, extractor):
        """
        The GCS object path must contain a date partition so BigQuery can
        use partition pruning (skipping old data when querying).
        Pattern: gs://<bucket>/spotify/YYYY/MM/DD/batch_<ts>.json
        """
        result = extractor.extract_recent()
        # Path should have 3 date segments after "spotify/"
        # e.g. "gs://bucket/spotify/2026/01/31/batch_123.json"
        path_parts = result["gcs_path"].split("/")
        # Index: gs: // bucket spotify YYYY MM DD batch_ts.json
        #          0   1   2       3     4    5   6   7
        assert len(path_parts) >= 8
        assert path_parts[3] == "spotify"

    def test_content_type_is_ndjson(self, extractor):
        """
        content_type should be 'application/x-ndjson' so GCS metadata
        is accurate and Cloud Function triggers can filter by content type.
        """
        extractor.extract_recent()
        blob = extractor._bucket.blob.return_value
        _, kwargs = blob.upload_from_string.call_args
        assert kwargs.get("content_type") == "application/x-ndjson"


# ─────────────────────────────────────────────────────────────────────────────
# Tests for empty-track edge case
# ─────────────────────────────────────────────────────────────────────────────


class TestExtractRecentEmpty:
    def test_returns_zero_total_when_no_tracks(self, extractor):
        """
        If Spotify returns nothing (rare, but can happen if the account
        has literally never played anything), we should NOT crash.
        """
        extractor._client.get_recently_played.return_value = []

        result = extractor.extract_recent()

        assert result["total_tracks"] == 0
        assert result["gcs_path"] is None

    def test_does_not_upload_when_no_tracks(self, extractor):
        """
        Uploading an empty file would confuse downstream jobs (they'd think
        an extraction ran and produced no data, vs. an extraction that wasn't run).
        So we skip the upload entirely.
        """
        extractor._client.get_recently_played.return_value = []

        extractor.extract_recent()

        extractor._bucket.blob.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Tests for build_extractor_from_env()
# ─────────────────────────────────────────────────────────────────────────────


class TestBuildExtractorFromEnv:
    def test_raises_if_gcs_bucket_missing(self, monkeypatch):
        """
        If GCS_RAW_BUCKET is not set, the factory should fail immediately
        with a clear error message (not a cryptic AttributeError later).

        monkeypatch is a pytest built-in that safely sets/unsets env vars
        during a test and restores them automatically afterward.
        """
        monkeypatch.delenv("GCS_RAW_BUCKET", raising=False)

        with pytest.raises(ValueError, match="GCS_RAW_BUCKET"):
            build_extractor_from_env()

    def test_builds_extractor_with_env_vars(self, monkeypatch):
        """
        When all required env vars are present, factory returns a SpotifyExtractor.
        We mock out SpotifyConfig, SpotifyClient, and storage.Client to avoid
        real network calls.
        """
        monkeypatch.setenv("GCS_RAW_BUCKET", "my-test-bucket")
        monkeypatch.setenv("SPOTIFY_CLIENT_ID", "fake-id")
        monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "fake-secret")

        with (
            patch("echostream.spotify.extractor.storage.Client"),
            patch("echostream.spotify.config.SpotifyConfig.validate"),
            patch("spotipy.Spotify"),
            patch("spotipy.oauth2.SpotifyOAuth"),
        ):
            result = build_extractor_from_env()

        assert isinstance(result, SpotifyExtractor)
