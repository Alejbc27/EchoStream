"""
Spotify configuration loader.

Why a dedicated config module?
- Centralizes all env-var reading in one place → only one file to change later
- Validates early (fail fast): if credentials are missing, we know BEFORE
  making any network call
- Makes testing easy: tests can construct SpotifyConfig with fake values
  without touching environment variables
"""

import os
from pathlib import Path


class SpotifyConfig:
    """
    Holds all Spotify OAuth2 settings needed to build a spotipy client.

    Reads from environment variables (set in .env at project root).
    Call validate() after construction to surface missing variables all at once.
    """

    def __init__(self) -> None:
        # OAuth2 credentials — you get these from developer.spotify.com/dashboard
        self.client_id: str | None = os.getenv("SPOTIFY_CLIENT_ID")
        self.client_secret: str | None = os.getenv("SPOTIFY_CLIENT_SECRET")

        # Where Spotify redirects after the user approves access.
        # For local dev, http://localhost:8888/callback is the convention.
        # You must add this EXACT URL in your Spotify app settings.
        self.redirect_uri: str = os.getenv(
            "SPOTIFY_REDIRECT_URI", "http://localhost:8888/callback"
        )

        # Where spotipy saves the access + refresh token locally.
        # SPOTIFY_CACHE_PATH lets you override for Cloud Run (where the token
        # comes from Secret Manager, copied to /tmp for write-back on refresh).
        # Default: .spotify_cache at the project root (gitignored).
        self.cache_path: str = os.getenv(
            "SPOTIFY_CACHE_PATH",
            str(Path(__file__).parent.parent.parent.parent / ".spotify_cache"),
        )

        # Whether to open the browser for OAuth approval.
        # Set to "false" in headless environments (Cloud Run, CI).
        # When false, spotipy relies on a cached token — it won't prompt.
        self.open_browser: bool = (
            os.getenv("SPOTIFY_OPEN_BROWSER", "true").lower() == "true"
        )

    def validate(self) -> None:
        """
        Check all required variables are present.

        Raises ValueError listing EVERY missing variable (not just the first),
        so you can fix them all in one go instead of discovering them one by one.
        """
        missing: list[str] = []

        if not self.client_id:
            missing.append("SPOTIFY_CLIENT_ID")
        if not self.client_secret:
            missing.append("SPOTIFY_CLIENT_SECRET")

        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}\n"
                "Copy .env.template to .env and fill in your Spotify credentials.\n"
                "Get them at: https://developer.spotify.com/dashboard"
            )
