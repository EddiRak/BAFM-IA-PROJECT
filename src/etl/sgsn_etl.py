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
SOURCE_DATA_PATH = r"/home/eddi/Desktop/CDR_EDR_unzipped/raw_data/SGSN"
RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", os.path.join(BASE_PATH, "data/raw_data/GGSN"))

PATTERN = "MGANPGW*"
SEP_GGSN = "|"

def main():
    start_time = time.time()
    logger.info("-" * 50)
    logger.info("ETL GGSN - Démarrage")
    logger.info(f"• Date   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"• Source : {os.path.basename(SOURCE_DATA_PATH)}")
    logger.info(f"• Cible  : {os.path.basename(RAW_DATA_PATH)}")

    files = sorted(glob.glob(os.path.join(SOURCE_DATA_PATH, PATTERN)))
    if not files:
        logger.error(f"[GGSN] Aucun fichier trouvé dans {SOURCE_DATA_PATH} ({PATTERN})")
        return

    headers = load_headers()
    ggsn_cols = headers.get("GGSN", [])
    os.makedirs(RAW_DATA_PATH, exist_ok=True)

    logger.info(f"• À traiter : {len(files)} fichiers")
    has_gz = any(fp.lower().endswith(".gz") for fp in files)
    blocksize = None if has_gz else "64MB"

    ddf = dd.read_csv(
        files,
        sep=SEP_GGSN,
        names= ggsn_cols,
        header=None,
        dtype="string",
        blocksize=blocksize,
        include_path_column="__path",
        engine="python",
        on_bad_lines="skip",
    )
    ddf["FileSourceName"] = ddf["__path"].map(os.path.basename, meta=("FileSourceName", "object"))
    ddf = ddf.drop(columns="__path")

    logger.info("\nSauvegarde Parquet...")

    # Client Dask silencieux + fermeture propre
    from modules.dask_runtime import _start_client, quiet_close
    client = _start_client()
    try:
        out_path = os.path.join(RAW_DATA_PATH)
        #write_one_parquet(ddf, out_path)
        ddf.to_parquet(
            out_path,                # <-- un répertoire (ex: ".../ccn_dataset/")
            write_index=False,
            compression="snappy",        # zstd si CPU OK
            engine="pyarrow",
            #partition_on=["callStartDate"]   # ou toute colonne de partition logique
        )

        # >>> FIX ICI : comptage robuste (évite l’erreur .sum sur int)
        total_rows = int(ddf.shape[0].compute())
        # Variante fallback si jamais shape[0] n’est pas supporté dans un environnement :
        # if not isinstance(total_rows, int):
        #     total_rows = int(ddf.map_partitions(lambda pdf: len(pdf)).compute())  # pas de .sum() dask ici

    finally:
        quiet_close(client)


    exec_time = time.time() - start_time
    logger.info("="*70)
    logger.info("Résultats du traitement:")
    logger.info(f"• Fichiers avec données : {len(files)}")
    logger.info(f"• Fichiers sans données : 0")
    logger.info(f"• Total enregistrements : {total_rows:,}")
    logger.info(f"• Temps d'exécution     : {exec_time:.2f} s")
    logger.info("=" * 70 + "\n")

if __name__ == "__main__":
    main()
