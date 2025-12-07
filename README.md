# OpenSky Flight Data Analysis

Dieses Projekt implementiert eine skalierbare Data Pipeline zur Analyse von OpenSky Network Flugdaten (ADS-B). Es nutzt einen **Data Lake** Ansatz mit **Apache Spark** für ETL-Prozesse und Analysen.

## Systemarchitektur

Das Projekt folgt einer **Multi-Layer Data Lake Architektur**:

1.  **Ingestion Layer:** Python-Skript lädt `.tar`-Archive vom S3-Bucket (parallelisiert) und extrahiert CSV-Rohdaten.
2.  **Storage Layer (Bronze/Raw):** Speicherung der unveränderten CSV-Dateien ("Schema-on-Read").
3.  **Storage Layer (Silver/Processed):** Spark transformiert Daten in **Apache Parquet** (spaltenbasiert, komprimiert) und partitioniert nach Datum für performante Analysen (OLAP).
4.  **Analytics Layer (Gold):** Jupyter Notebooks nutzen Spark SQL für Aggregationen und Folium für geografische Visualisierungen.

---

## Installation & Setup

Befolge diese Schritte, um das Projekt lokal einzurichten (z.B. nach dem Klonen).

### 1. Voraussetzungen
* **Setup aus ADE erfolgreich befolgt**

### 2. Virtual Environment einrichten
Öffne ein Terminal im Hauptordner des Projekts:

```bash
# 1. Umgebung erstellen
python -m venv .venv

# 2. Umgebung aktivieren
# Windows:
.venv\Scripts\activate
# Mac/Linux:
# source .venv/bin/activate

# 3. Abhängigkeiten installieren
pip install -r requirements.txt
```

### 3. Konfiguration (.env)
Das Projekt benötigt lokale Pfadeinstellungen.
1.  Kopiere die Datei `.env.example` und benenne sie um in `.env`.
2.  Passe die Werte in der `.env` Datei an (falls nötig)

## Nutzung

### Schritt 1: Daten laden (Ingestion)
Führe das Download-Tool aus, um echte Daten von OpenSky zu holen:

*(Stelle sicher, dass dein `.venv` aktiv ist)*
```bash
python src/dataset_download_tool/opensky_tool.py
```
*Die Daten landen automatisch im konfigurierten Ordner (Default: `data/raw`).*

### Schritt 2: Daten verarbeiten & analysieren
Starten der Jupyter Notebook Umgebung:

```bash
jupyter notebook
```

Öffne das Notebook `notebooks/prototype.ipynb` im Browser oder direkt in VS Code.

1.  Wähle oben rechts den Kernel **`.venv (Python...)`** aus. (oder installiere die empfohlene Python und Jupyter Option)
2.  Führe die Zellen nacheinander aus ("Run All").
    * **Zelle 1-2:** Liest die CSV-Rohdaten.
    * **Zelle 3 (ETL):** Konvertiert zu Parquet (`data/processed`).
    * **Zelle 4-5:** Analysiert die Daten und zeichnet eine interaktive Karte.

---

## Projektstruktur

```text
FLY_BIG_DATA/
├── data/                   # Lokaler Data Lake
│   ├── raw/                # Bronze Layer (CSV)
│   └── processed/          # Silver Layer (Parquet)
├── notebooks/              # Jupyter Notebooks
├── src/                    # Quellcode
│   └── dataset_download_tool/
│       └── opensky_tool.py # Ingestion Logik
├── .env.example            # Template
├── .gitignore              # Git Regeln
├── requirements.txt        # Python Abhängigkeiten
└── README.md               # Diese Datei