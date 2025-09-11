import os, glob, time, logging
from datetime import datetime
from typing import List, Dict

import dask
import dask.dataframe as dd

from modules.headers import load_headers
from modules.io_one_parquet import write_one_parquet
from modules.dask_runtime import _start_client, quiet_close

logging.basicConfig(level=logging.INFO, format='%(levelname)-8s - %(message)s')
logger = logging.getLogger(__name__)

BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
SOURCE_DATA_PATH_DATA = r"D:\Utilisateurs\Public\IA_BAFM\PROJECT\DATA_SAMPLE\CDR_EDR_zipped\OCC-DATA"
SOURCE_DATA_PATH_SMS  = r"D:\Utilisateurs\Public\IA_BAFM\PROJECT\DATA_SAMPLE\CDR_EDR_zipped\OCC-SMS"

RAW_DATA_PATH_DATA = os.environ.get("RAW_DATA_OCC_DATA", os.path.join(BASE_PATH, "data/raw_data/OCC/OCC-DATA"))
RAW_DATA_PATH_SMS  = os.environ.get("RAW_DATA_OCC_SMS",  os.path.join(BASE_PATH, "data/raw_data/OCC/OCC-SMS"))

DATA_SUFFIXES = (".ber", ".ber_NP")
SMS_SUFFIXES  = (".ber-SMS",)
SEP_OCC = ","

def _list_files_by_suffixes(root: str, suffixes: tuple) -> List[str]:
    files: List[str] = []
    for s in suffixes:
        files.extend(glob.glob(os.path.join(root, f"*{s}")))
        files.extend(glob.glob(os.path.join(root, f"*{s}.gz")))
    return sorted(files)

def _process_group(files: List[str], columns: List[str], out_dir: str, out_name: str) -> Dict[str, int]:
    if not files:
        logger.warning(f"[OCC] Aucun fichier à traiter pour {out_dir}")
        return {"files_ok": 0, "files_ko": 0, "rows": 0}

    os.makedirs(out_dir, exist_ok=True)
    logger.info(f"• À traiter : {len(files)} fichiers")

    has_gz = any(fp.lower().endswith(".gz") for fp in files)
    blocksize = None if has_gz else "64MB"

    ddf = dd.read_csv(
        files,
        sep=SEP_OCC,
        names= columns,
        header=None,
        dtype="string",
        blocksize=blocksize,
        include_path_column="__path",
        engine="python",
        on_bad_lines="skip",
    )
    ddf["FileSourceName"] = ddf["__path"].map(os.path.basename, meta=("FileSourceName","object"))
    ddf = ddf.drop(columns="__path")

    logger.info("\nSauvegarde Parquet...")
    client = _start_client()
    try:
        with dask.config.set(optimizations=[], optimize_graph=False):
            ddf_p = ddf.persist()
        out_path = os.path.join(out_dir, out_name)
        write_one_parquet(ddf_p, out_path)
        total_rows = int(ddf_p.map_partitions(len, meta=("rows","i8")).sum().compute(optimize_graph=False))
    finally:
        quiet_close(client)

    return {"files_ok": len(files), "files_ko": 0, "rows": total_rows}

def main():
    start_time = time.time()
    logger.info("-" * 50)
    logger.info("ETL OCC - Démarrage")
    logger.info(f"• Date   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"• Source (OCC-DATA) : {os.path.basename(SOURCE_DATA_PATH_DATA)}")
    logger.info(f"• Source (OCC-SMS)  : {os.path.basename(SOURCE_DATA_PATH_SMS)}")
    logger.info(f"• Cible (OCC-DATA)  : {os.path.basename(RAW_DATA_PATH_DATA)}")
    logger.info(f"• Cible (OCC-SMS)   : {os.path.basename(RAW_DATA_PATH_SMS)}")

    headers = load_headers()
    occ_cols = headers.get("OCC", [])

    data_files = _list_files_by_suffixes(SOURCE_DATA_PATH_DATA, DATA_SUFFIXES)
    sms_files  = _list_files_by_suffixes(SOURCE_DATA_PATH_SMS,  SMS_SUFFIXES)

    logger.info("Lancement de traitement pour OCC DATA.")
    stats_data = _process_group(data_files, occ_cols, RAW_DATA_PATH_DATA, "occ_data.parquet")

    logger.info("-" * 50)
    logger.info("Lancement de traitement pour OCC SMS.")
    stats_sms = _process_group(sms_files,  occ_cols, RAW_DATA_PATH_SMS,  "occ_sms.parquet")

    exec_time = time.time() - start_time
    logger.info("="*70)
    logger.info("Résultats du traitement:")
    logger.info(f"• Fichiers OCC-DATA avec données : {stats_data['files_ok']}")
    logger.info(f"• Fichiers OCC-DATA sans données : {stats_data['files_ko']}")
    logger.info(f"• Total enregistrements OCC-DATA : {stats_data['rows']:,}")
    logger.info("-" * 50)
    logger.info(f"• Fichiers OCC-SMS avec données  : {stats_sms['files_ok']}")
    logger.info(f"• Fichiers OCC-SMS sans données  : {stats_sms['files_ko']}")
    logger.info(f"• Total enregistrements OCC-SMS  : {stats_sms['rows']:,}")
    logger.info("-" * 50)
    logger.info(f"• Total DATA + SMS : {stats_data['rows'] + stats_sms['rows']:,}")
    logger.info(f"• Temps d'exécution : {exec_time:.2f} s")
    logger.info("=" * 70 + "\n")

if __name__ == "__main__":
    main()
