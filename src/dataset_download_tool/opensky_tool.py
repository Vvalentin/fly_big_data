"""
Skript zum PARALLELEN Download und Entpacken von OpenSky Network Datensätzen.
Features:
- Multithreading für maximalen Speed.
- Shared S3-Client (vermeidet Connection-Pool Warnungen).
- Zählt exakt den Speicherplatz der ENTPACKTEN CSV-Dateien.
- Konfigurierbar über .env Datei.
"""

import os
import tarfile
import gzip
import shutil
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Bucket Zugriff
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from dotenv import load_dotenv

# --- 1. Konfiguration laden ---
load_dotenv()  # WICHTIG: Lädt die Variablen aus der .env Datei

# Standard-Pfad (falls nichts in .env steht): data/raw im Projektordner
DEFAULT_PATH = os.path.join(os.getcwd(), 'data', 'raw')

# Variablen aus .env holen
LOKALER_SPEICHERORT = os.getenv('OPENSKY_DOWNLOAD_PATH', DEFAULT_PATH)
MAX_FILES = int(os.getenv('MAX_FILES', '5'))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '5'))

# Feste Konfig
BUCKET_NAME = 'data-samples'
START_ORDNER = 'states/'
ENDPOINT_URL = 'https://s3.opensky-network.org'
MIN_DATEIGROESSE_BYTES = 1024 * 1024  # 1 MB (Download-Filter)
POOL_SIZE = MAX_WORKERS + 5           # Puffer für HTTP-Verbindungen

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(message)s')
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("botocore").setLevel(logging.ERROR)
logger = logging.getLogger()


def _worker_prozess(s3_client, bucket, s3_key, lokaler_pfad):
    """
    Lädt Datei, entpackt sie komplett, löscht Müll und gibt die finale Größe aus.
    """
    ordner_pfad = os.path.dirname(lokaler_pfad)
    datei_name = os.path.basename(lokaler_pfad)

    try:
        # 1. Tar Downloaden
        s3_client.download_file(bucket, s3_key, lokaler_pfad)

        # 2. Tar Entpacken
        with tarfile.open(lokaler_pfad, 'r') as tar:
            tar.extractall(path=ordner_pfad)

        # 3. Aufräumen (Readme/License) und GZ finden
        gz_pfad = None
        for datei in os.listdir(ordner_pfad):
            full_path = os.path.join(ordner_pfad, datei)
            if datei.endswith('.csv.gz'):
                gz_pfad = full_path
            elif 'README' in datei or 'LICENSE' in datei:
                try:
                    os.remove(full_path)
                except OSError:
                    pass

        # 4. Tar löschen
        if os.path.exists(lokaler_pfad):
            os.remove(lokaler_pfad)

        # 5. Gz Entpacken
        finale_csv_groesse = 0
        if gz_pfad:
            ziel_csv = gz_pfad[:-3]
            with gzip.open(gz_pfad, 'rb') as f_in:
                with open(ziel_csv, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Dateigröße ermitteln
            if os.path.exists(ziel_csv):
                finale_csv_groesse = os.path.getsize(ziel_csv)

            # Gz löschen
            os.remove(gz_pfad)

        return finale_csv_groesse, datei_name

    except (boto3.exceptions.S3UploadFailedError, tarfile.TarError, OSError, gzip.BadGzipFile) as fehler:
        logger.error("!!! FEHLER bei %s: %s", datei_name, fehler)
        return 0, datei_name


def main():
    """Hauptfunktion für Initialisierung und Thread-Steuerung."""

    # Ordner erstellen, falls nicht existent (nutzt jetzt den Pfad aus .env)
    if not os.path.exists(LOKALER_SPEICHERORT):
        os.makedirs(LOKALER_SPEICHERORT)
        print(f"Zielordner erstellt: {LOKALER_SPEICHERORT}")
    else:
        print(f"Speichere Daten in: {LOKALER_SPEICHERORT}")

    # 1. Shared Client erstellen
    config = Config(
        signature_version=UNSIGNED,
        max_pool_connections=POOL_SIZE
    )
    shared_client = boto3.client(
        's3', endpoint_url=ENDPOINT_URL, config=config)

    # 2. Listing der Dateien im Bucket
    logger.info("Initialisiere %d Worker. Scanne Bucket '%s' (Limit: %d)...",
                MAX_WORKERS, BUCKET_NAME, MAX_FILES)

    paginator = shared_client.get_paginator('list_objects_v2')
    seiten = paginator.paginate(Bucket=BUCKET_NAME, Prefix=START_ORDNER)

    aufgaben_liste = []
    stop_search = False

    for seite in seiten:
        if stop_search:
            break
        if 'Contents' not in seite:
            continue

        for obj in seite['Contents']:
            # Stoppen sobald Limit erreicht
            if len(aufgaben_liste) >= MAX_FILES:
                stop_search = True
                break

            key = obj['Key']
            size = obj['Size']

            # Filter: Nur .csv.tar und keine Junk-Dateien
            if not key.endswith('.csv.tar') or size < MIN_DATEIGROESSE_BYTES:
                continue

            # Pfade berechnen
            rel_path = os.path.relpath(key, START_ORDNER)
            lokal_tar = os.path.join(LOKALER_SPEICHERORT, rel_path)
            lokal_dir = os.path.dirname(lokal_tar)

            # Ordner vorbereiten (Thread-Safe durch exist_ok=True)
            if not os.path.exists(lokal_dir):
                os.makedirs(lokal_dir, exist_ok=True)

            # Überspringen wenn CSV schon da
            csv_check = lokal_tar.replace('.csv.tar', '.csv')
            if os.path.exists(csv_check):
                continue

            aufgaben_liste.append((key, lokal_tar))

    total_files = len(aufgaben_liste)
    logger.info(
        "%d neue Dateien gefunden. Starte Parallel-Verarbeitung...\n", total_files)

    # 3. Parallel-Verarbeitung
    gesamt_csv_bytes = 0
    erledigt_counter = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Jobs einreichen
        future_map = {
            executor.submit(_worker_prozess, shared_client, BUCKET_NAME, k, p): k
            for k, p in aufgaben_liste
        }

        # Ergebnisse einsammeln
        for future in as_completed(future_map):
            erledigt_counter += 1
            groesse_csv, name = future.result()

            if groesse_csv > 0:
                gesamt_csv_bytes += groesse_csv

                # Umrechnung für Anzeige
                csv_mb = groesse_csv / (1024**2)
                total_gb = gesamt_csv_bytes / (1024**3)

                logger.info(
                    "[%d/%d] Fertig: %s (+%.0f MB) -> Gesamt (CSV): %.2f GB",
                    erledigt_counter, total_files, name, csv_mb, total_gb
                )
            else:
                logger.warning("[%d/%d] Übersprungen/Fehler: %s",
                               erledigt_counter, total_files, name)

    logger.info("-" * 40)
    logger.info("ENDE. Belegter Speicherplatz (Unkomprimiert): %.2f GB",
                gesamt_csv_bytes / (1024**3))
    logger.info("Daten liegen in: %s", LOKALER_SPEICHERORT)


if __name__ == "__main__":
    main()
