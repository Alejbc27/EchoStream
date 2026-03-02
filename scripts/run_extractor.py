"""
Script manual para ejecutar el SpotifyExtractor y ver tus canciones en GCS.

¿Por qué un script separado y no un test?
------------------------------------------
Los tests usan mocks (datos falsos) para ser rápidos y no necesitar internet.
Este script es diferente: hace llamadas REALES a Spotify y GCS.
Lo usamos para verificar que todo funciona de verdad antes de automatizarlo.

Cómo correrlo:
    uv run python scripts/run_extractor.py

Qué hace:
    1. Lee tus credenciales del archivo .env
    2. Se autentica con Spotify (abre el navegador la primera vez)
    3. Pide las últimas 50 canciones que escuchaste
    4. Las guarda en GCS como NDJSON
    5. Imprime en pantalla qué guardó y dónde

Qué necesitas tener configurado antes:
    - .env con SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, GCS_RAW_BUCKET
    - gcloud auth application-default login (para que Python pueda hablar con GCS)
    - Los buckets de GCS creados (terraform apply ✅)
"""

import sys
from pathlib import Path

# Añadimos el directorio raíz al path para que Python encuentre el .env
# __file__ = este archivo (scripts/run_extractor.py)
# .parent   = carpeta scripts/
# .parent   = carpeta raíz del proyecto
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

# python-dotenv lee el archivo .env y mete las variables en os.environ
# Tiene que hacerse ANTES de importar nuestro código (que lee os.environ en __init__)
from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")

from echostream.spotify.client import SpotifyClient  # noqa: E402
from echostream.spotify.config import SpotifyConfig  # noqa: E402
from echostream.spotify.extractor import SpotifyExtractor  # noqa: E402


def main() -> None:
    print("🎵 EchoStream — Spotify Extractor")
    print("=" * 45)

    # ── 1. Configuración ────────────────────────────────────────────────────
    print("\n1️⃣  Cargando configuración desde .env...")
    config = SpotifyConfig()

    # GCS_RAW_BUCKET viene del .env, lo leemos directamente
    import os  # noqa: PLC0415

    gcs_bucket = os.environ.get("GCS_RAW_BUCKET")
    if not gcs_bucket:
        print("❌ Falta GCS_RAW_BUCKET en tu .env")
        print("   Añade: GCS_RAW_BUCKET=echostream-2026-echostream-raw-dev")
        sys.exit(1)

    print(f"   ✅ Bucket Raw: gs://{gcs_bucket}")

    # ── 2. Autenticación Spotify ─────────────────────────────────────────────
    # La primera vez abre el navegador para que apruebes el acceso.
    # spotipy guarda el token en .spotify_cache en la raíz del proyecto.
    # Las siguientes veces reutiliza el token (sin abrir el navegador).
    print("\n2️⃣  Conectando con Spotify...")
    print("   (Si es la primera vez, se abrirá el navegador para aprobar acceso)")
    client = SpotifyClient(config)

    # Verificamos que la auth funciona pidiendo tu perfil
    user = client.get_current_user()
    print(f"   ✅ Autenticado como: {user.get('display_name', user.get('id'))}")

    # ── 3. Extracción ────────────────────────────────────────────────────────
    print("\n3️⃣  Extrayendo canciones recientes de Spotify...")
    extractor = SpotifyExtractor(client, gcs_bucket)
    result = extractor.extract_recent(limit=50)

    # ── 4. Resultado ─────────────────────────────────────────────────────────
    print("\n4️⃣  Resultado:")
    if result["total_tracks"] == 0:
        print("   ⚠️  Spotify no devolvió canciones.")
        print("   ¿Has escuchado música recientemente con esta cuenta?")
        return

    print(f"   ✅ Canciones guardadas : {result['total_tracks']}")
    print(f"   📁 Ruta en GCS         : {result['gcs_path']}")
    print(f"   📅 Partición de fecha  : {result['partition']}")
    print(f"   🕐 Extraído a las      : {result['extracted_at']}")

    print("\n" + "=" * 45)
    print("✅ ¡Listo! Tus canciones están en GCS.")
    print(f"\nPuedes verlas en la consola de GCP:")
    print(f"https://console.cloud.google.com/storage/browser/{gcs_bucket}")


if __name__ == "__main__":
    main()
