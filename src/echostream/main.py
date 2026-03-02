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
import sys

from echostream.spotify.extractor import build_extractor_from_env


def main() -> None:
    print("🎵 EchoStream Cloud Run Job — Starting extraction cycle")
    print("=" * 55)

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
