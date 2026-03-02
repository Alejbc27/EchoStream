"""
Unit tests for SpotifyConfig and SpotifyClient.

Why mock spotipy instead of calling the real API?
- Tests must run without internet, without credentials, in CI
- We're testing OUR code (config loading, method delegation), not spotipy itself
- Fast: no network = no latency, no rate limits, no token expiry
"""

from unittest.mock import MagicMock, patch

import pytest

from echostream.spotify.client import SpotifyClient
from echostream.spotify.config import SpotifyConfig

# ---------------------------------------------------------------------------
# SpotifyConfig tests
# ---------------------------------------------------------------------------


class TestSpotifyConfig:
    def test_reads_env_vars(self, monkeypatch):
        monkeypatch.setenv("SPOTIFY_CLIENT_ID", "fake_id")
        monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "fake_secret")
        monkeypatch.setenv("SPOTIFY_REDIRECT_URI", "http://localhost:9999/cb")

        config = SpotifyConfig()

        assert config.client_id == "fake_id"
        assert config.client_secret == "fake_secret"
        assert config.redirect_uri == "http://localhost:9999/cb"

    def test_redirect_uri_has_default(self, monkeypatch):
        monkeypatch.setenv("SPOTIFY_CLIENT_ID", "fake_id")
        monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "fake_secret")
        monkeypatch.delenv("SPOTIFY_REDIRECT_URI", raising=False)

        config = SpotifyConfig()

        assert config.redirect_uri == "http://localhost:8888/callback"

    def test_validate_raises_when_client_id_missing(self, monkeypatch):
        monkeypatch.delenv("SPOTIFY_CLIENT_ID", raising=False)
        monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "fake_secret")

        config = SpotifyConfig()

        with pytest.raises(ValueError, match="SPOTIFY_CLIENT_ID"):
            config.validate()

    def test_validate_raises_when_client_secret_missing(self, monkeypatch):
        monkeypatch.setenv("SPOTIFY_CLIENT_ID", "fake_id")
        monkeypatch.delenv("SPOTIFY_CLIENT_SECRET", raising=False)

        config = SpotifyConfig()

        with pytest.raises(ValueError, match="SPOTIFY_CLIENT_SECRET"):
            config.validate()

    def test_validate_lists_all_missing_vars_at_once(self, monkeypatch):
        monkeypatch.delenv("SPOTIFY_CLIENT_ID", raising=False)
        monkeypatch.delenv("SPOTIFY_CLIENT_SECRET", raising=False)

        config = SpotifyConfig()

        with pytest.raises(ValueError) as exc_info:
            config.validate()

        error_message = str(exc_info.value)
        assert "SPOTIFY_CLIENT_ID" in error_message
        assert "SPOTIFY_CLIENT_SECRET" in error_message

    def test_validate_passes_when_all_vars_present(self, monkeypatch):
        monkeypatch.setenv("SPOTIFY_CLIENT_ID", "fake_id")
        monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "fake_secret")

        config = SpotifyConfig()
        config.validate()  # should not raise


# ---------------------------------------------------------------------------
# SpotifyClient tests
# ---------------------------------------------------------------------------

# We patch SpotifyOAuth and spotipy.Spotify at the module level where they're
# imported (echostream.spotify.client), NOT at spotipy's source.
# This is the standard Python mocking rule: patch where the name is USED.


@pytest.fixture
def mock_spotify_client(monkeypatch):
    """
    Returns a SpotifyClient with the underlying spotipy.Spotify mocked out.
    The fixture yields (client, mock_sp) so tests can configure return values.
    """
    monkeypatch.setenv("SPOTIFY_CLIENT_ID", "fake_id")
    monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "fake_secret")

    with (
        patch("echostream.spotify.client.SpotifyOAuth"),
        patch("echostream.spotify.client.spotipy.Spotify") as mock_spotify_class,
    ):
        mock_sp_instance = MagicMock()
        mock_spotify_class.return_value = mock_sp_instance

        config = SpotifyConfig()
        client = SpotifyClient(config)

        yield client, mock_sp_instance


class TestSpotifyClient:
    def test_get_current_user_delegates_to_spotipy(self, mock_spotify_client):
        client, mock_sp = mock_spotify_client
        mock_sp.current_user.return_value = {
            "id": "alejandro",
            "display_name": "Alejandro",
        }

        result = client.get_current_user()

        mock_sp.current_user.assert_called_once()
        assert result["id"] == "alejandro"

    def test_get_recently_played_returns_items(self, mock_spotify_client):
        client, mock_sp = mock_spotify_client
        mock_sp.current_user_recently_played.return_value = {
            "items": [
                {
                    "track": {"name": "Blinding Lights"},
                    "played_at": "2025-01-01T10:00:00Z",
                },
                {"track": {"name": "Levitating"}, "played_at": "2025-01-01T09:00:00Z"},
            ]
        }

        result = client.get_recently_played(limit=2)

        mock_sp.current_user_recently_played.assert_called_once_with(limit=2)
        assert len(result) == 2
        assert result[0]["track"]["name"] == "Blinding Lights"

    def test_get_recently_played_returns_empty_list_when_no_items(
        self, mock_spotify_client
    ):
        client, mock_sp = mock_spotify_client
        mock_sp.current_user_recently_played.return_value = {}

        result = client.get_recently_played()

        assert result == []

    def test_get_top_tracks_uses_default_time_range(self, mock_spotify_client):
        client, mock_sp = mock_spotify_client
        mock_sp.current_user_top_tracks.return_value = {
            "items": [{"name": "Shape of You"}]
        }

        result = client.get_top_tracks()

        mock_sp.current_user_top_tracks.assert_called_once_with(
            limit=20, time_range="medium_term"
        )
        assert len(result) == 1

    def test_get_top_tracks_accepts_long_term(self, mock_spotify_client):
        client, mock_sp = mock_spotify_client
        mock_sp.current_user_top_tracks.return_value = {"items": []}

        client.get_top_tracks(limit=50, time_range="long_term")

        mock_sp.current_user_top_tracks.assert_called_once_with(
            limit=50, time_range="long_term"
        )

    def test_client_raises_if_config_invalid(self, monkeypatch):
        monkeypatch.delenv("SPOTIFY_CLIENT_ID", raising=False)
        monkeypatch.delenv("SPOTIFY_CLIENT_SECRET", raising=False)

        config = SpotifyConfig()

        with pytest.raises(ValueError, match="SPOTIFY_CLIENT_ID"):
            SpotifyClient(config)
