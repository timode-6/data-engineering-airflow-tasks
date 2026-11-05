"""Paths and the shared Asset definition"""

from __future__ import annotations

from pathlib import Path

from airflow.sdk import Asset

# Inside the containers; ./data on the host is bind-mounted here by docker-compose
DATA_ROOT = Path("/opt/airflow/data")

INCOMING_DIR = DATA_ROOT / "incoming"
STAGING_DIR = DATA_ROOT / "staging"
PROCESSED_DIR = DATA_ROOT / "processed"

# The file the sensor waits for
SOURCE_FILE = INCOMING_DIR / "tiktok_google_play_reviews.csv"

STAGE_NULLS = STAGING_DIR / "01_nulls_replaced.csv"
STAGE_SORTED = STAGING_DIR / "02_sorted_by_created_date.csv"
FINAL_FILE = PROCESSED_DIR / "reviews_processed.csv"

REVIEWS_PROCESSED = Asset(f"file://{FINAL_FILE}")

MONGO_CONN_ID = "mongo_default"
MONGO_DATABASE = "reviews"
MONGO_COLLECTION = "tiktok_reviews"
