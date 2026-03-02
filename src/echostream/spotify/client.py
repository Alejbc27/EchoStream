"""
Spotify API client for EchoStream.

Why wrap spotipy instead of using it directly?
- Hides spotipy's API surface: the rest of EchoStream never imports spotipy
- One place to handle token refresh, errors, and retries
- Swap to a different library later without touching pipeline code
"""

import spotipy
from spotipy.oauth2 import SpotifyOAuth

from echostream.spotify.config import SpotifyConfig


class SpotifyClient:
    """
    High-level Spotify client used by EchoStream pipelines.

    Authentication flow (Authorization Code — "SpotifyOAuth"):
    1. First run: opens a browser so you can approve access
    2. spotipy saves access_token + refresh_token to .spotify_cache
    3. Subsequent runs: reads the cache and auto-refreshes the token when it expires

    This is the right flow for a backend app that runs as YOU (one user).
    PKCE would be used for mobile/web apps where the secret can't be stored safely.
    """

    # Scopes = permissions we ask the user to grant.
    # Spotify won't return data outside the granted scopes.
    SCOPES = " ".join(
        [
            "user-read-recently-played",  # last 50 played tracks (with timestamps)
            "user-top-read",  # your top tracks/artists over time
            "user-read-playback-state",  # what's playing right now
        ]
    )

    def __init__(self, config: SpotifyConfig) -> None:
        config.validate()

        # SpotifyOAuth manages the full token lifecycle:
        # - first-time browser redirect → you approve → saves token
        # - auto-refreshes the token (valid 1 hour) using the refresh_token
        auth_manager = SpotifyOAuth(
            client_id=config.client_id,
            client_secret=config.client_secret,
            redirect_uri=config.redirect_uri,
            scope=self.SCOPES,
            cache_path=config.cache_path,
            open_browser=config.open_browser,
        )

        self._sp = spotipy.Spotify(auth_manager=auth_manager)

    def get_current_user(self) -> dict:
        """
        Returns your Spotify profile (id, display_name, email, country, etc.).
        Useful as a health-check: if this works, auth is set up correctly.
        """
        return self._sp.current_user()

    def get_recently_played(self, limit: int = 50) -> list[dict]:
        """
        Returns the last N tracks you played, with timestamps.

        limit: max 50 (Spotify API hard limit per call).
        Each item contains: track name, artists, album, played_at (UTC ISO-8601).

        Why only 50? Spotify only keeps ~50 recent plays in this endpoint.
        For full history (2020-2026) we'll use a scheduled job that runs
        every few hours and stores each batch to GCS (the Bronze layer).
        """
        result = self._sp.current_user_recently_played(limit=limit)
        return result.get("items", [])

    def get_top_tracks(
        self, limit: int = 20, time_range: str = "medium_term"
    ) -> list[dict]:
        """
        Returns your most-played tracks over a time window.

        time_range options:
        - "short_term"  → last ~4 weeks
        - "medium_term" → last ~6 months  (default)
        - "long_term"   → several years   ← great for your 2020-2026 data

        limit: max 50.
        """
        result = self._sp.current_user_top_tracks(limit=limit, time_range=time_range)
        return result.get("items", [])
