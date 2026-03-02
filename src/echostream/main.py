"""
Cloud Run Job entrypoint for EchoStream.

This module is the production entry point that Cloud Run executes every 2 hours
(triggered by Cloud Scheduler). It runs a full extraction cycle:
  1. Last 50 recently-played tracks
  2. Top tracks for short_term (~4 weeks)
  3. Top tracks for medium_term (~6 months)
  4. Top tracks for long_term (several years)

All data lands in GCS Raw bucket as NDJSON, partitioned by date.

How it runs:
  Cloud Scheduler (cron: every 2h)
      → triggers Cloud Run Job
          → this script calls extract_all()
              → 4 GCS uploads (recent + 3 top ranges)

Environment variables (set via Terraform + Secret Manager):
  SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET  — from Secret Manager
  GCS_RAW_BUCKET                            — from Terraform output
  SPOTIFY_CACHE_PATH                        — /tmp/.spotify_cache (Cloud Run)
  SPOTIFY_OPEN_BROWSER                      — "false" (headless environment)
"""

import json
import os
import sys

from echostream.spotify.extractor import build_extractor_from_env


def _load_spotify_cache_from_secret() -> None:
    """
    In Cloud Run, the Spotify token lives in Secret Manager (not on disk).
    We read the secret value and write it to SPOTIFY_CACHE_PATH so that
    Spotipy finds it on startup and doesn't try to open a browser.

    WHY /tmp?
    Cloud Run containers have a read-only filesystem EXCEPT /tmp.
    Spotipy needs write access to the cache file (it updates it on token refresh).

    WHY only in Cloud Run?
    We detect Cloud Run by checking K_SERVICE (Cloud Run Services) or
    CLOUD_RUN_JOB (Cloud Run Jobs) — both set automatically by Google.
    Locally, the .spotify_cache file already exists on disk.
    """
    # K_SERVICE  → set by Cloud Run Services (web servers)
    # CLOUD_RUN_JOB → set by Cloud Run Jobs (batch tasks like ours)
    # If neither is set, we're running locally → skip Secret Manager entirely
    if not os.getenv("K_SERVICE") and not os.getenv("CLOUD_RUN_JOB"):
        return

    from google.cloud import secretmanager

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCLOUD_PROJECT")
    cache_path = os.getenv("SPOTIFY_CACHE_PATH", "/tmp/.spotify_cache")

    if not project_id:
        raise RuntimeError(
            "GOOGLE_CLOUD_PROJECT is not set — cannot load Spotify token from Secret Manager"
        )

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/spotify-cache-token/versions/latest"
    response = client.access_secret_version(request={"name": name})
    token_data = response.payload.data.decode("utf-8")

    with open(cache_path, "w") as f:
        f.write(token_data)

    print(f"✅ Spotify token loaded from Secret Manager → {cache_path}")


def main() -> None:
    print("🎵 EchoStream Cloud Run Job — Starting extraction cycle")
    print("=" * 55)

    _load_spotify_cache_from_secret()

    try:
        extractor = build_extractor_from_env()
    except ValueError as e:
        print(f"❌ Configuration error: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        results = extractor.extract_all()
    except Exception as e:
        print(f"❌ Extraction failed: {e}", file=sys.stderr)
        sys.exit(1)

    total_tracks = sum(r["total_tracks"] for r in results)
    successful = [r for r in results if r["gcs_path"] is not None]

    print("\n✅ Extraction cycle complete")
    print(f"   Extractions run : {len(results)}")
    print(f"   Files uploaded  : {len(successful)}")
    print(f"   Total tracks    : {total_tracks}")

    for r in results:
        label = r.get("time_range", "recent")
        status = f"→ {r['gcs_path']}" if r["gcs_path"] else "→ (no tracks)"
        print(f"   [{label}] {r['total_tracks']} tracks {status}")

    # Print JSON summary for Cloud Logging structured logs
    print(
        f"\n📊 {json.dumps({'total_tracks': total_tracks, 'files': len(successful)})}"
    )


if __name__ == "__main__":
    main()
