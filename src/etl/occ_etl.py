import os
import glob
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
from dask.distributed import Client

# En-têtes inlinés (import commun)
from modules.headers import load_headers

# ------------------ Logging ------------------
logging.basicConfig(level=logging.INFO, format='%(levelname)-8s - %(message)s')
logger = logging.getLogger(__name__)

# ------------------ Configuration ------------------
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

RAW_DATA_PATH = os.environ.get("RAW_DATA_OCC", os.path.join(BASE_PATH, "raw_data/OCC"))

# Sorties (2 flux)
PROCESSED_DATA_PATH_DATA = os.environ.get("PROCESSED_DATA_OCC_DATA", os.path.join(BASE_PATH, "processed_data/ETL/OCC/OCC-DATA"))
PROCESSED_DATA_PATH_SMS = os.environ.get("PROCESSED_DATA_OCC_SMS", os.path.join(BASE_PATH, "processed_data/ETL/OCC/OCC-SMS"))

# Suffixes (split demandé)
DATA_SUFFIXES = (".ber", ".ber_NP")
SMS_SUFFIXES = (".ber-SMS",)

# Séparateur OCC
SEP_OCC = ","

# ------------------ Lecture fichier ------------------
def _read_occ_file(file_path: str, columns: List[str]) -> pd.DataFrame:
    """
    Lit un fichier OCC (CSV sans en-tête) et retourne un DataFrame typé string.
    """
    try:
        df = pd.read_csv(
            file_path,
            sep=SEP_OCC,
            names=columns,
            header=None,
            dtype="string",
            engine="python",
            on_bad_lines="skip",
        )
        # Ajoute FileSourceName si pas dans les colonnes
        if "FileSourceName" not in df.columns:
            df["FileSourceName"] = os.path.basename(file_path)
        else:
            df["FileSourceName"] = os.path.basename(file_path)
        return df
    except Exception as e:
        logger.error(f"[OCC] Lecture échouée: {os.path.basename(file_path)} -> {e}")
        return pd.DataFrame(columns=columns)

def _list_files_by_suffixes(root: str, suffixes: tuple) -> List[str]:
    files: List[str] = []
    for s in suffixes:
        files.extend(glob.glob(os.path.join(root, f"*{s}")))
    return sorted(files)

# ------------------ Pipeline ------------------
def _process_group(files: List[str], columns: List[str], out_dir: str, out_name: str) -> Dict[str, int]:
    """
    Traite un groupe de fichiers (DATA ou SMS) en parallèle et écrit un parquet unique.
    """
    if not files:
        logger.warning(f"[OCC] Aucun fichier à traiter pour {out_dir}")
        return {"files_ok": 0, "files_ko": 0, "rows": 0}

    os.makedirs(out_dir, exist_ok=True)
    n_workers = int(os.environ.get("DASK_WORKERS", "4"))

    files_ok = 0
    files_ko = 0
    total_rows = 0

    logger.info(f"• À traiter : {len(files)} fichiers")

    with Client(n_workers=n_workers) as client:

        # Traitement des fichiers
        logger.info("\nTraitement des fichiers...")

        futures = [client.submit(_read_occ_file, fp, columns) for fp in files]
        results: List[pd.DataFrame] = []
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
        out_path = os.path.join(out_dir, out_name)
        combined.to_parquet(out_path, index=False)
        logger.info(f"• Nombre d'enregistrement : {len(combined):,} enregistrements")
    else:
        logger.warning(f"[OCC] • {out_dir} : Aucune donnée consolidée")

    return {"files_ok": files_ok, "files_ko": files_ko, "rows": total_rows}

def main():
    """Point d'entrée principal du processus ETL OCC (split DATA vs SMS)."""
    start_time = time.time()
    logger.info("-" * 50)
    logger.info("ETL OCC - Démarrage")
    logger.info(f"• Date   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"• Source : {os.path.basename(RAW_DATA_PATH)}")
    logger.info(f"• Cible (OCC-DATA) : {os.path.basename(PROCESSED_DATA_PATH_DATA)}")
    logger.info(f"• Cible (OCC-SMS)  : {os.path.basename(PROCESSED_DATA_PATH_SMS)}")


    # En-têtes OCC
    headers = load_headers()
    occ_cols = headers.get("OCC", [])

    # Groupes de fichiers
    data_files = _list_files_by_suffixes(RAW_DATA_PATH, DATA_SUFFIXES)
    sms_files = _list_files_by_suffixes(RAW_DATA_PATH, SMS_SUFFIXES)

    print("Lancement de traitement pour OCC DATA.")
    # DATA
    stats_data = _process_group(
        files=data_files,
        columns=occ_cols,
        out_dir=PROCESSED_DATA_PATH_DATA,
        out_name="occ_data.parquet",
    )

    logger.info("-" * 50)
    print("Lancement de traitement pour OCC DATA.")

    # SMS
    stats_sms = _process_group(
        files=sms_files,
        columns=occ_cols,
        out_dir=PROCESSED_DATA_PATH_SMS,
        out_name="occ_sms.parquet",
    )

    # Résumé
    exec_time = time.time() - start_time
    logger.info("="*70)
    logger.info("Résultats du traitement:")
    logger.info(f"• Fichiers OCC-DATA avec données : {stats_data['files_ok']}")
    logger.info(f"• Fichiers OCC-DATA sans données : {stats_data['files_ko']}")
    logger.info(f"• Total enregistrements OCC-DATA : {stats_data['rows']:,}")
    logger.info("-" * 50)
    logger.info(f"• Fichiers OCC-SMS avec données  : {stats_sms['files_ok']}")
    logger.info(f"• Fichiers OCC-SMS sans données  : {stats_sms['files_ko']}")
    logger.info(f"• Total enregistrements OCC-SMS: {stats_sms['rows']:,}")
    logger.info("-" * 50)
    logger.info(f"• Total enregistrements OCC-DATA + OCC-SMS : {stats_data['rows'] + stats_sms['rows']:,}")
    logger.info(f"• Temps d'exécution   : {exec_time:.2f} s")
    logger.info("=" * 70 + "\n")

