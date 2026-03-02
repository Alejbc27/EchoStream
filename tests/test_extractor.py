"""
Tests for SpotifyExtractor.

What we're testing:
  - extract_recent() → saves to GCS under spotify/recent/, returns metadata
  - extract_top()    → saves to GCS under spotify/top/<range>/, returns metadata
  - extract_all()    → calls extract_recent + extract_top for all 3 time ranges
  - extract_top() with invalid time_range → raises ValueError
  - Empty track edge cases → no upload, returns zero
  - _save_to_gcs() with data_type → correct GCS path
  - _date_partition() → pure function, formats dates correctly
  - build_extractor_from_env() → raises if GCS_RAW_BUCKET missing
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
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def fake_tracks() -> list[dict]:
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
def fake_top_tracks() -> list[dict]:
    return [
        {"name": "Shape of You", "id": "spotify:track:010", "popularity": 90},
        {"name": "Blinding Lights", "id": "spotify:track:011", "popularity": 88},
    ]


@pytest.fixture
def mock_spotify_client(fake_tracks, fake_top_tracks) -> MagicMock:
    client = MagicMock()
    client.get_recently_played.return_value = fake_tracks
    client.get_top_tracks.return_value = fake_top_tracks
    return client


@pytest.fixture
def mock_gcs_bucket() -> MagicMock:
    blob = MagicMock()
    bucket = MagicMock()
    bucket.name = "test-echostream-raw"
    bucket.blob.return_value = blob
    return bucket


@pytest.fixture
def extractor(mock_spotify_client, mock_gcs_bucket) -> SpotifyExtractor:
    with patch("echostream.spotify.extractor.storage.Client"):
        ex = SpotifyExtractor(mock_spotify_client, "test-echostream-raw")
    ex._bucket = mock_gcs_bucket
    return ex


# ─────────────────────────────────────────────────────────────────────────────
# Tests for _date_partition()
# ─────────────────────────────────────────────────────────────────────────────


class TestDatePartition:
    def test_formats_single_digit_month_and_day(self):
        dt = datetime(2026, 1, 5, tzinfo=UTC)
        assert _date_partition(dt) == "2026/01/05"

    def test_formats_double_digit_month_and_day(self):
        dt = datetime(2026, 12, 31, tzinfo=UTC)
        assert _date_partition(dt) == "2026/12/31"

    def test_year_is_preserved(self):
        dt = datetime(2025, 6, 15, tzinfo=UTC)
        assert _date_partition(dt).startswith("2025/")


# ─────────────────────────────────────────────────────────────────────────────
# Tests for extract_recent()
# ─────────────────────────────────────────────────────────────────────────────


class TestExtractRecent:
    def test_returns_correct_metadata(self, extractor, fake_tracks):
        result = extractor.extract_recent()

        assert result["total_tracks"] == len(fake_tracks)
        assert result["gcs_path"] is not None
        assert result["gcs_path"].startswith("gs://test-echostream-raw/spotify/recent/")
        assert result["extracted_at"] is not None
        assert "/" in result["partition"]

    def test_calls_spotify_client_with_default_limit(self, extractor):
        extractor.extract_recent()
        extractor._client.get_recently_played.assert_called_once_with(limit=50)

    def test_calls_spotify_client_with_custom_limit(self, extractor):
        extractor.extract_recent(limit=10)
        extractor._client.get_recently_played.assert_called_once_with(limit=10)

    def test_uploads_to_gcs(self, extractor):
        extractor.extract_recent()
        extractor._bucket.blob.assert_called_once()
        extractor._bucket.blob.return_value.upload_from_string.assert_called_once()

    def test_uploaded_content_is_valid_ndjson(self, extractor, fake_tracks):
        extractor.extract_recent()

        blob = extractor._bucket.blob.return_value
        upload_call_args = blob.upload_from_string.call_args
        uploaded_content: str = upload_call_args[0][0]

        lines = [line for line in uploaded_content.strip().splitlines() if line]
        assert len(lines) == len(fake_tracks)

        for line in lines:
            parsed = json.loads(line)
            assert "track" in parsed
            assert "played_at" in parsed

    def test_gcs_path_contains_recent_and_date_partition(self, extractor):
        result = extractor.extract_recent()
        # Path: gs://bucket/spotify/recent/YYYY/MM/DD/batch_ts.json
        path_parts = result["gcs_path"].split("/")
        assert "recent" in path_parts

    def test_content_type_is_ndjson(self, extractor):
        extractor.extract_recent()
        blob = extractor._bucket.blob.return_value
        _, kwargs = blob.upload_from_string.call_args
        assert kwargs.get("content_type") == "application/x-ndjson"


# ─────────────────────────────────────────────────────────────────────────────
# Tests for extract_recent() — empty tracks
# ─────────────────────────────────────────────────────────────────────────────


class TestExtractRecentEmpty:
    def test_returns_zero_total_when_no_tracks(self, extractor):
        extractor._client.get_recently_played.return_value = []
        result = extractor.extract_recent()
        assert result["total_tracks"] == 0
        assert result["gcs_path"] is None

    def test_does_not_upload_when_no_tracks(self, extractor):
        extractor._client.get_recently_played.return_value = []
        extractor.extract_recent()
        extractor._bucket.blob.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Tests for extract_top()
# ─────────────────────────────────────────────────────────────────────────────


class TestExtractTop:
    def test_returns_correct_metadata_for_short_term(self, extractor, fake_top_tracks):
        result = extractor.extract_top(time_range="short_term")

        assert result["total_tracks"] == len(fake_top_tracks)
        assert result["gcs_path"].startswith(
            "gs://test-echostream-raw/spotify/top/short_term/"
        )
        assert result["time_range"] == "short_term"
        assert result["extracted_at"] is not None

    def test_calls_client_with_correct_params(self, extractor):
        extractor.extract_top(time_range="long_term", limit=25)
        extractor._client.get_top_tracks.assert_called_once_with(
            limit=25, time_range="long_term"
        )

    def test_gcs_path_includes_time_range(self, extractor):
        result = extractor.extract_top(time_range="medium_term")
        assert "top/medium_term" in result["gcs_path"]

    def test_uploaded_content_is_valid_ndjson(self, extractor, fake_top_tracks):
        extractor.extract_top(time_range="short_term")

        blob = extractor._bucket.blob.return_value
        uploaded_content: str = blob.upload_from_string.call_args[0][0]
        lines = [line for line in uploaded_content.strip().splitlines() if line]
        assert len(lines) == len(fake_top_tracks)

        for line in lines:
            parsed = json.loads(line)
            assert "name" in parsed

    def test_raises_on_invalid_time_range(self, extractor):
        with pytest.raises(ValueError, match="Invalid time_range"):
            extractor.extract_top(time_range="invalid_range")

    def test_returns_zero_when_no_top_tracks(self, extractor):
        extractor._client.get_top_tracks.return_value = []
        result = extractor.extract_top(time_range="short_term")
        assert result["total_tracks"] == 0
        assert result["gcs_path"] is None
        assert result["time_range"] == "short_term"

    def test_does_not_upload_when_no_top_tracks(self, extractor):
        extractor._client.get_top_tracks.return_value = []
        extractor.extract_top(time_range="short_term")
        extractor._bucket.blob.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Tests for extract_all()
# ─────────────────────────────────────────────────────────────────────────────


class TestExtractAll:
    def test_returns_four_results(self, extractor):
        results = extractor.extract_all()
        assert len(results) == 4

    def test_first_result_is_recent(self, extractor):
        results = extractor.extract_all()
        assert "recent" in results[0]["gcs_path"]

    def test_remaining_results_are_top_with_all_time_ranges(self, extractor):
        results = extractor.extract_all()
        time_ranges = [r["time_range"] for r in results[1:]]
        assert time_ranges == ["short_term", "medium_term", "long_term"]

    def test_calls_spotify_api_four_times(self, extractor):
        extractor.extract_all()
        assert extractor._client.get_recently_played.call_count == 1
        assert extractor._client.get_top_tracks.call_count == 3


# ─────────────────────────────────────────────────────────────────────────────
# Tests for build_extractor_from_env()
# ─────────────────────────────────────────────────────────────────────────────


class TestBuildExtractorFromEnv:
    def test_raises_if_gcs_bucket_missing(self, monkeypatch):
        monkeypatch.delenv("GCS_RAW_BUCKET", raising=False)
        with pytest.raises(ValueError, match="GCS_RAW_BUCKET"):
            build_extractor_from_env()

    def test_builds_extractor_with_env_vars(self, monkeypatch):
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
