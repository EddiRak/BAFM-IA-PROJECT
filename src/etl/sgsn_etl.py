import os
import glob
import logging
import time
from datetime import datetime
from typing import List

import pandas as pd
from dask.distributed import Client
from modules.headers import load_headers

# ------------------ Logging ------------------
logging.basicConfig(level=logging.INFO, format='%(levelname)-8s - %(message)s')
logger = logging.getLogger(__name__)

# ------------------ Configuration ------------------
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
RAW_DATA_PATH = os.environ.get("RAW_DATA_SGSN", os.path.join(BASE_PATH, "raw_data/SGSN"))
PROCESSED_DATA_PATH = os.environ.get("PROCESSED_DATA_SGSN", os.path.join(BASE_PATH, "processed_data/ETL/SGSN"))

PATTERN = "MGANPGW*"
SEP_SGSN = "|"

# ------------------ Lecture fichier ------------------
def _read_sgsn_file(file_path: str, columns: List[str]) -> pd.DataFrame:
    try:
        df = pd.read_csv(
            file_path,
            sep=SEP_SGSN,
            names=columns,
            header=None,
            dtype="string",
            engine="python",
            on_bad_lines="skip",
        )
        if "FileSourceName" not in df.columns:
            df["FileSourceName"] = os.path.basename(file_path)
        else:
            df["FileSourceName"] = os.path.basename(file_path)
        return df
    except Exception as e:
        logger.error(f"[SGSN] Lecture échouée: {os.path.basename(file_path)} -> {e}")
        return pd.DataFrame(columns=columns)

# ------------------ Pipeline ------------------
def main():
    """Point d'entrée principal du processus ETL SGSN (PGW)."""
    start_time = time.time()
    logger.info("-" * 50)
    logger.info("ETL SGSN - Démarrage")
    logger.info(f"• Date   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"• Source : {os.path.basename(RAW_DATA_PATH)}")
    logger.info(f"• Cible  : {os.path.basename(PROCESSED_DATA_PATH)}")

    files = sorted(glob.glob(os.path.join(RAW_DATA_PATH, PATTERN)))
    if not files:
        logger.error(f"[SGSN] Aucun fichier trouvé dans {RAW_DATA_PATH} ({PATTERN})")
        return

    headers = load_headers()
    sgsn_cols = headers.get("SGSN", [])

    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
    n_workers = int(os.environ.get("DASK_WORKERS", "4"))

    files_ok = 0
    files_ko = 0
    total_rows = 0
    results: List[pd.DataFrame] = []

    logger.info(f"• À traiter : {len(files)} fichiers")

    with Client(n_workers=n_workers) as client:

        # Traitement des fichiers
        logger.info("\nTraitement des fichiers...")

        futures = [client.submit(_read_sgsn_file, fp, sgsn_cols) for fp in files]
        for fut in futures:
            df = fut.result()
            if isinstance(df, pd.DataFrame) and not df.empty:
                results.append(df)
                files_ok += 1
                total_rows += len(df)
            else:
                files_ko += 1

    logger.info("\nTraitement des fichiers terminé.")

    # Sauvegarde
    logger.info("\nSauvegarde Parquet...")

    if results:
        combined = pd.concat(results, ignore_index=True)
        out_path = os.path.join(PROCESSED_DATA_PATH, "sgsn.parquet")
        combined.to_parquet(out_path, index=False)
        logger.info(f"[SGSN] • {len(combined):,} lignes -> {out_path}")
    else:
        logger.warning("[SGSN] Aucune donnée consolidée")

    exec_time = time.time() - start_time
    logger.info("="*70)
    logger.info("Résultats du traitement:")
    logger.info(f"• Fichiers avec données : {files_ok}")
    logger.info(f"• Fichiers sans données : {files_ko}")
    logger.info(f"• Total enregistrements: {total_rows:,}")
    logger.info(f"• Temps d'exécution   : {exec_time:.2f} s")
    logger.info("=" * 70 + "\n")
