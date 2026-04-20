"""
etl/load.py
SAP BDC Capstone — Data Load Layer
Saves processed data to BDC-ready CSV / simulated Datasphere upload
"""

import pandas as pd
import json
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def load_to_datasphere(df: pd.DataFrame, table_name: str, path: str = "./data/processed"):
    """
    Simulates loading a DataFrame into SAP Datasphere (BDC).
    In production: uses Datasphere REST API or JDBC connector.
    """
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, f"{table_name}.csv")
    df.to_csv(filepath, index=False)
    logger.info(f"[BDC LOAD] {table_name}: {len(df):,} rows → {filepath}")
    return filepath


def load_kpis(kpis: dict, path: str = "./data/processed"):
    """Saves computed KPIs as JSON for dashboard consumption."""
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, "kpis.json")

    # Ensure all values are JSON serializable
    def make_serializable(obj):
        if isinstance(obj, dict):
            return {k: make_serializable(v) for k, v in obj.items()}
        elif hasattr(obj, "item"):
            return obj.item()
        return obj

    kpis_serializable = make_serializable(kpis)
    kpis_serializable["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(filepath, "w") as f:
        json.dump(kpis_serializable, f, indent=2)
    logger.info(f"[BDC LOAD] KPIs saved → {filepath}")
    return filepath


def load_pipeline_log(stats: dict, path: str = "./data/processed"):
    """Writes a pipeline execution log."""
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, "pipeline_log.json")
    stats["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stats["status"] = "SUCCESS"
    with open(filepath, "w") as f:
        json.dump(stats, f, indent=2)
    logger.info(f"[PIPELINE LOG] Saved → {filepath}")
    return filepath


if __name__ == "__main__":
    logger.info("Load module loaded. Use pipeline.py to run end-to-end.")
