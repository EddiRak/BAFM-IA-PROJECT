import os, glob, time, gzip, logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import pandas as pd
import dask
import dask.delayed as delayed
import dask.dataframe as dd

from modules.io_one_parquet import write_one_parquet
from modules.dask_runtime import _start_client, quiet_close

logging.basicConfig(level=logging.INFO, format='%(levelname)-8s - %(message)s')
logger = logging.getLogger(__name__)

BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
SOURCE_DATA_PATH = r"D:\Utilisateurs\Public\IA_BAFM\PROJECT\DATA_SAMPLE\CDR_EDR_zipped\SDP"
RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", os.path.join(BASE_PATH, "data/raw_data/SDP"))

RECORD_TYPES = {
    'ACCOUNT_ADJUSTMENT': "SdpoutputCdr2_255_CS6.SDPCallDataRecord.accountAdjustment",
    'PERIODIC_ACCOUNT_MGMT': "SdpoutputCdr2_255_CS6.SDPCallDataRecord.periodicAccountMgmt",
    'LIFE_CYCLE_CHANGE': "SdpoutputCdr2_255_CS6.SDPCallDataRecord.lifeCycleChange"
}

def parse_record(lines: List[str]) -> Tuple[Optional[str], Dict]:
    if not lines: return None, {}
    record_type = lines[0].strip(); data = {}
    for line in lines[2:-1]:
        line = line.strip()
        if not line or line.startswith('{') or line.endswith('}'): continue
        parts = line.split(':', 1)
        if len(parts) != 2: continue
        key, value = parts; key = key.strip(); value = value.strip().strip('"\'')
        if value.endswith('D'): value = value[:-2]
        elif value == "": value = None
        data[key] = value
    return record_type, data

def _read_text(path: str) -> str:
    if path.endswith('.gz'):
        with gzip.open(path, 'rt') as f: return f.read()
    with open(path, 'r') as f: return f.read()

@delayed
def process_file_delayed(file_path: str) -> Dict[str, pd.DataFrame]:
    try:
        content = _read_text(file_path)
        records, current_record = [], []
        for line in content.split('\n'):
            line = line.strip()
            if not line: continue
            if line in [RECORD_TYPES['ACCOUNT_ADJUSTMENT'], RECORD_TYPES['PERIODIC_ACCOUNT_MGMT'], RECORD_TYPES['LIFE_CYCLE_CHANGE']]:
                if current_record: records.append(parse_record(current_record))
                current_record = [line]
            else:
                current_record.append(line)
        if current_record: records.append(parse_record(current_record))
        adj = [rec[1] for rec in records if rec[0] == RECORD_TYPES['ACCOUNT_ADJUSTMENT']]
        per = [rec[1] for rec in records if rec[0] == RECORD_TYPES['PERIODIC_ACCOUNT_MGMT']]
        lcc = [rec[1] for rec in records if rec[0] == RECORD_TYPES['LIFE_CYCLE_CHANGE']]
        adj_df = pd.DataFrame(adj) if adj else pd.DataFrame()
        per_df = pd.DataFrame(per) if per else pd.DataFrame()
        lcc_df = pd.DataFrame(lcc) if lcc else pd.DataFrame()
        for df in (adj_df, per_df, lcc_df):
            if not df.empty:
                if 'adjustmentDate' in df.columns and 'adjustmentTime' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['adjustmentDate'] + df['adjustmentTime'], format='%Y%m%d%H%M%S', errors='ignore')
                elif 'timeStamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timeStamp'].str[:14], format='%Y%m%d%H%M%S', errors='ignore')
        return {'adjustments': adj_df, 'periodic': per_df, 'life_cycle': lcc_df}
    except Exception as e:
        logger.error(f"Erreur {file_path}: {str(e)}")
        return {'adjustments': pd.DataFrame(), 'periodic': pd.DataFrame(), 'life_cycle': pd.DataFrame()}

def _meta(cols: List[str]) -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})

def _family_ddf(tasks: List[delayed], key: str) -> dd.DataFrame:
    delayed_frames = [t.map(lambda d, k=key: d.get(k, pd.DataFrame())) for t in tasks]
    # déduire colonnes
    head = []
    for df_del in delayed_frames[:5]:
        try:
            df0 = df_del.compute()
            if not df0.empty:
                head.append(df0.iloc[:0])
        except Exception:
            pass
    cols = list(pd.concat(head, ignore_index=True).columns) if head else ["__dummy__"]
    return dd.from_delayed(delayed_frames, meta=_meta(cols))

def main():
    start_time = time.time()
    logger.info("-" * 50)
    logger.info("ETL SDP - Démarrage")
    logger.info(f"• Date      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"• Source    : {os.path.basename(SOURCE_DATA_PATH)}")
    logger.info(f"• Cible     : {os.path.basename(RAW_DATA_PATH)}")

    for d in ["ACCOUNT_ADJUSTMENT", "PERIODIC_ACCOUNT_MGMT", "LIFE_CYCLE_CHANGE"]:
        os.makedirs(os.path.join(RAW_DATA_PATH, d), exist_ok=True)

    asn_files = glob.glob(os.path.join(SOURCE_DATA_PATH, "*.ASN")) + glob.glob(os.path.join(SOURCE_DATA_PATH, "*.ASN.gz"))
    if not asn_files:
        logger.error(f"Aucun fichier ASN ou ASN.gz trouvé dans {SOURCE_DATA_PATH}")
        return
    logger.info(f"• À traiter : {len(asn_files)} fichiers")

    tasks = [process_file_delayed(fp) for fp in asn_files]

    ddfs = {
        'adjustments': _family_ddf(tasks, 'adjustments'),
        'periodic':    _family_ddf(tasks, 'periodic'),
        'life_cycle':  _family_ddf(tasks, 'life_cycle'),
    }

    client = _start_client()
    try:
        logger.info("\nSauvegarde des fichiers parquet...")
        with dask.config.set(optimizations=[], optimize_graph=False):
            persisted = {k: v.persist() for k, v in ddfs.items()}

        if persisted['adjustments'].npartitions > 0:
            write_one_parquet(persisted['adjustments'], os.path.join(RAW_DATA_PATH, "ACCOUNT_ADJUSTMENT/account_adjustments.parquet"))
        if persisted['periodic'].npartitions > 0:
            write_one_parquet(persisted['periodic'], os.path.join(RAW_DATA_PATH, "PERIODIC_ACCOUNT_MGMT/periodic_account_mgmt.parquet"))
        if persisted['life_cycle'].npartitions > 0:
            write_one_parquet(persisted['life_cycle'], os.path.join(RAW_DATA_PATH, "LIFE_CYCLE_CHANGE/life_cycle_changes.parquet"))

        total = 0
        for name, ddf in persisted.items():
            n = int(ddf.map_partitions(len, meta=("rows", "i8")).sum().compute(optimize_graph=False))
            logger.info(f"• {name:<20}: {n:,} enregistrements")
            total += n

        execution_time = time.time() - start_time
        logger.info("="*70)
        logger.info("\nRésultats du traitement:")
        logger.info(f"• Fichiers traités : {len(asn_files):,}")
        logger.info(f"• Total des enregistrements   : {total:,}")
        logger.info(f"• Temps d'exécution           : {execution_time:.2f} secondes")
        logger.info("="*70 + "\n")
    finally:
        quiet_close(client)

if __name__ == "__main__":
    main()
