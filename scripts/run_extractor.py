"""
Script manual para ejecutar el SpotifyExtractor y ver tus canciones en GCS.

Los tests usan mocks (datos falsos) para ser rápidos y no necesitar internet.
Este script es diferente: hace llamadas REALES a Spotify y GCS.
Lo usamos para verificar que todo funciona de verdad antes de automatizarlo.

Cómo correrlo:
    uv run python scripts/run_extractor.py          # solo recent (por defecto)
    uv run python scripts/run_extractor.py --all     # recent + top (3 rangos)

Qué necesitas tener configurado antes:
    - .env con SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, GCS_RAW_BUCKET
    - gcloud auth application-default login (para que Python pueda hablar con GCS)
    - Los buckets de GCS creados (terraform apply)
"""

import argparse
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")

from echostream.spotify.client import SpotifyClient  # noqa: E402
from echostream.spotify.config import SpotifyConfig  # noqa: E402
from echostream.spotify.extractor import SpotifyExtractor  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="EchoStream Spotify Extractor")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Extract recent + top tracks (all 3 time ranges)",
    )
    args = parser.parse_args()

    print("🎵 EchoStream — Spotify Extractor")
    print("=" * 45)

    # ── 1. Configuración ────────────────────────────────────────────────────
    print("\n1️⃣  Cargando configuración desde .env...")
    config = SpotifyConfig()

    import os  # noqa: PLC0415

    gcs_bucket = os.environ.get("GCS_RAW_BUCKET")
    if not gcs_bucket:
        print("❌ Falta GCS_RAW_BUCKET en tu .env")
        print("   Añade: GCS_RAW_BUCKET=echostream-2026-echostream-raw-dev")
        sys.exit(1)

    print(f"   ✅ Bucket Raw: gs://{gcs_bucket}")

    # ── 2. Autenticación Spotify ─────────────────────────────────────────────
    print("\n2️⃣  Conectando con Spotify...")
    print("   (Si es la primera vez, se abrirá el navegador para aprobar acceso)")
    client = SpotifyClient(config)

    user = client.get_current_user()
    print(f"   ✅ Autenticado como: {user.get('display_name', user.get('id'))}")

    # ── 3. Extracción ────────────────────────────────────────────────────────
    extractor = SpotifyExtractor(client, gcs_bucket)

    if args.all:
        print("\n3️⃣  Extrayendo recent + top tracks (4 extracciones)...")
        results = extractor.extract_all()
    else:
        print("\n3️⃣  Extrayendo canciones recientes de Spotify...")
        results = [extractor.extract_recent(limit=50)]

    # ── 4. Resultado ─────────────────────────────────────────────────────────
    print("\n4️⃣  Resultado:")
    total = sum(r["total_tracks"] for r in results)

    if total == 0:
        print("   ⚠️  Spotify no devolvió canciones.")
        return

    for r in results:
        label = r.get("time_range", "recent")
        if r["gcs_path"]:
            print(f"   ✅ [{label}] {r['total_tracks']} tracks → {r['gcs_path']}")
        else:
            print(f"   ⚠️  [{label}] sin tracks")

    print(f"\n   📊 Total: {total} tracks guardados")
    print("\n" + "=" * 45)
    print("✅ ¡Listo! Tus canciones están en GCS.")
    print("\nPuedes verlas en la consola de GCP:")
    print(f"https://console.cloud.google.com/storage/browser/{gcs_bucket}")


if __name__ == "__main__":
    main()
