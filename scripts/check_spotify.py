"""
Manual smoke-test script: verifies Spotify auth works with real credentials.

Run this AFTER:
  1. Copying .env.template → .env and filling in your credentials
  2. Running: uv sync

Usage:
  uv run python scripts/check_spotify.py

What happens on first run:
  - A browser tab opens asking you to approve EchoStream's access
  - After you click "Agree", Spotify redirects to http://localhost:8888/callback
  - spotipy captures the code from the redirect URL and exchanges it for tokens
  - Tokens are saved to .spotify_cache (gitignored)

Subsequent runs:
  - No browser needed — spotipy uses the saved refresh token automatically
"""

import sys
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root before importing anything that reads env vars
load_dotenv(Path(__file__).parent.parent / ".env")

from echostream.spotify.client import SpotifyClient
from echostream.spotify.config import SpotifyConfig


def main() -> None:
    print("=== EchoStream — Spotify Auth Check ===\n")

    config = SpotifyConfig()

    try:
        config.validate()
    except ValueError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    print("Credentials found in .env. Connecting to Spotify...\n")
    client = SpotifyClient(config)

    user = client.get_current_user()
    print(f"Connected as: {user.get('display_name')} (@{user.get('id')})")
    print(f"Country: {user.get('country')}\n")

    print("Fetching last 5 played tracks...")
    recent = client.get_recently_played(limit=5)
    for item in recent:
        track = item["track"]
        artists = ", ".join(a["name"] for a in track["artists"])
        played_at = item["played_at"]
        print(f"  {played_at}  |  {track['name']} — {artists}")

    print("\nFetching top 5 tracks (long_term = multi-year data)...")
    top = client.get_top_tracks(limit=5, time_range="long_term")
    for i, track in enumerate(top, 1):
        artists = ", ".join(a["name"] for a in track["artists"])
        print(f"  #{i}  {track['name']} — {artists}")

    print("\n[OK] Spotify auth is working correctly.")


if __name__ == "__main__":
    main()
