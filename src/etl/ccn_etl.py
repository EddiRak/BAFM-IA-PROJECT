import os, glob, time, logging
from datetime import datetime

import dask
import dask.dataframe as dd

from modules.headers import load_headers
from modules.io_one_parquet import write_one_parquet
from modules.dask_runtime import _start_client, quiet_close

logging.basicConfig(level=logging.INFO, format='%(levelname)-8s - %(message)s')
logger = logging.getLogger(__name__)

BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
SOURCE_DATA_PATH = r"D:\Utilisateurs\Public\IA_BAFM\PROJECT\DATA_SAMPLE\CDR_EDR_zipped\CCN-VOICE"
RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", os.path.join(BASE_PATH, "data/raw_data/CCN/CCN-VOICE"))

PATTERN = "CCNCDR*"
SEP_CCN = ","

def main():
    start_time = time.time()
    logger.info("-" * 50)
    logger.info("ETL CCN - Démarrage")
    logger.info(f"• Date   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"• Source : {os.path.basename(SOURCE_DATA_PATH)}")
    logger.info(f"• Cible  : {os.path.basename(RAW_DATA_PATH)}")

    files = sorted(glob.glob(os.path.join(SOURCE_DATA_PATH, PATTERN)))
    if not files:
        logger.error(f"[CCN] Aucun fichier trouvé dans {SOURCE_DATA_PATH} ({PATTERN})")
        return

    headers = load_headers()
    ccn_cols = headers.get("CCN", [])
    os.makedirs(RAW_DATA_PATH, exist_ok=True)

    logger.info(f"• À traiter : {len(files)} fichiers")
    has_gz = any(fp.lower().endswith(".gz") for fp in files)
    blocksize = None if has_gz else "64MB"

    ddf = dd.read_csv(
        files,
        sep=SEP_CCN,
        names=ccn_cols,
        header=None,
        dtype="string",
        blocksize=blocksize,
        include_path_column="__path",
        engine="python",
        on_bad_lines="skip",
    )
    ddf["FileSourceName"] = ddf["__path"].map(os.path.basename, meta=("FileSourceName", "object"))
    ddf = ddf.drop(columns="__path")

    client = _start_client()
    try:
        logger.info("\nSauvegarde Parquet...")
        with dask.config.set(optimizations=[], optimize_graph=False):
            ddf_p = ddf.persist()
        out_path = os.path.join(RAW_DATA_PATH, "ccn.parquet")
        write_one_parquet(ddf_p, out_path)
        total_rows = int(ddf_p.map_partitions(len, meta=("rows", "i8")).sum().compute(optimize_graph=False))
        exec_time = time.time() - start_time
        logger.info("="*70)
        logger.info("Résultats du traitement:")
        logger.info(f"• Fichiers avec données : {len(files)}")
        logger.info(f"• Fichiers sans données : 0")
        logger.info(f"• Total enregistrements : {total_rows:,}")
        logger.info(f"• Temps d'exécution     : {exec_time:.2f} s")
        logger.info("=" * 70 + "\n")
    finally:
        quiet_close(client)

if __name__ == "__main__":
    main()
