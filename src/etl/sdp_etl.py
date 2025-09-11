# -*- coding: utf-8 -*-
import os
import pandas as pd
import glob
from datetime import datetime
from typing import Dict, List, Tuple
import logging
import gzip
import time
import dask
import dask.delayed as delayed

logging.basicConfig(level=logging.INFO, format='%(levelname)-8s - %(message)s')
logger = logging.getLogger(__name__)

BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
SOURCE_DATA_PATH = r"/home/eddi/Desktop/CDR_EDR_unzipped/raw_data/SDP"
RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", os.path.join(BASE_PATH, "data/raw_data/SDP"))

RECORD_TYPES = {
    'ACCOUNT_ADJUSTMENT': "SdpoutputCdr2_255_CS6.SDPCallDataRecord.accountAdjustment",
    'PERIODIC_ACCOUNT_MGMT': "SdpoutputCdr2_255_CS6.SDPCallDataRecord.periodicAccountMgmt",
    'LIFE_CYCLE_CHANGE': "SdpoutputCdr2_255_CS6.SDPCallDataRecord.lifeCycleChange"
}

def parse_record(lines: List[str]) -> Tuple[str, Dict]:
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

def _read_text(fp: str) -> str:
    if fp.endswith('.gz'):
        with gzip.open(fp, 'rt', encoding='utf-8', errors='ignore') as f: return f.read()
    with open(fp, 'rt', encoding='utf-8', errors='ignore') as f: return f.read()

def _safe_parquet_name(src_path: str) -> str:
    base = os.path.basename(src_path)
    if base.endswith(".gz"): base = base[:-3]
    return os.path.splitext(base)[0] + ".parquet"

def _process_and_write_one(file_path: str, out_root: str) -> Dict[str, int]:
    """Parse le fichier SDP et écrit jusqu'à 3 parquets (1 par famille)."""
    content = _read_text(file_path)
    records, current = [], []
    for line in content.split('\n'):
        line = line.strip()
        if not line: continue
        if line in RECORD_TYPES.values():
            if current: records.append(parse_record(current))
            current = [line]
        else:
            current.append(line)
    if current: records.append(parse_record(current))

    groups = {'ACCOUNT_ADJUSTMENT': [], 'PERIODIC_ACCOUNT_MGMT': [], 'LIFE_CYCLE_CHANGE': []}
    for rec_type, row in records:
        if rec_type == RECORD_TYPES['ACCOUNT_ADJUSTMENT']: groups['ACCOUNT_ADJUSTMENT'].append(row)
        elif rec_type == RECORD_TYPES['PERIODIC_ACCOUNT_MGMT']: groups['PERIODIC_ACCOUNT_MGMT'].append(row)
        elif rec_type == RECORD_TYPES['LIFE_CYCLE_CHANGE']: groups['LIFE_CYCLE_CHANGE'].append(row)

    stats = {k: 0 for k in groups.keys()}
    for k, rows in groups.items():
        if not rows: continue
        df = pd.DataFrame(rows)
        # petites normalisations de temps si dispo
        if 'adjustmentDate' in df.columns and 'adjustmentTime' in df.columns:
            try: df['timestamp'] = pd.to_datetime(df['adjustmentDate'] + df['adjustmentTime'], format='%Y%m%d%H%M%S')
            except Exception: pass
        elif 'timeStamp' in df.columns:
            try: df['timestamp'] = pd.to_datetime(df['timeStamp'].str[:14], format='%Y%m%d%H%M%S')
            except Exception: pass

        out_dir = os.path.join(out_root, k)
        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(out_dir, _safe_parquet_name(file_path))
        df.to_parquet(out_file, index=False, engine="pyarrow")
        stats[k] = len(df)

    return stats

def main():
    t0 = time.time()
    logger.info("-" * 50)
    logger.info("ETL SDP - Démarrage")
    logger.info(f"• Date      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"• Source    : {os.path.basename(SOURCE_DATA_PATH)}")
    logger.info(f"• Cible     : {os.path.basename(RAW_DATA_PATH)}")

    for sub in ["ACCOUNT_ADJUSTMENT", "PERIODIC_ACCOUNT_MGMT", "LIFE_CYCLE_CHANGE"]:
        os.makedirs(os.path.join(RAW_DATA_PATH, sub), exist_ok=True)

    files = glob.glob(os.path.join(SOURCE_DATA_PATH, "*.ASN")) + \
            glob.glob(os.path.join(SOURCE_DATA_PATH, "*.ASN.gz"))
    if not files:
        logger.error(f"Aucun fichier ASN ou ASN.gz trouvé dans {SOURCE_DATA_PATH}")
        return
    logger.info(f"• À traiter : {len(files)} fichiers")

    tasks = [delayed(_process_and_write_one)(fp, RAW_DATA_PATH) for fp in files]

    from modules.dask_runtime import _start_client, quiet_close
    client = _start_client()
    try:
        results: List[Dict[str, int]] = dask.compute(*tasks)
    finally:
        quiet_close(client)

    totals = {"ACCOUNT_ADJUSTMENT": 0, "PERIODIC_ACCOUNT_MGMT": 0, "LIFE_CYCLE_CHANGE": 0}
    ok = ko = 0
    for st in results:
        rows = sum(st.values())
        ok += 1 if rows > 0 else 0
        ko += 0 if rows > 0 else 1
        for k in totals: totals[k] += st.get(k, 0)

    dt = time.time() - t0
    logger.info("="*70)
    logger.info("Résultats du traitement:")
    logger.info(f"• ACCOUNT_ADJUSTMENT    : {totals['ACCOUNT_ADJUSTMENT']:,}")
    logger.info(f"• PERIODIC_ACCOUNT_MGMT : {totals['PERIODIC_ACCOUNT_MGMT']:,}")
    logger.info(f"• LIFE_CYCLE_CHANGE     : {totals['LIFE_CYCLE_CHANGE']:,}")
    logger.info(f"• Fichiers avec données : {ok:,}")
    logger.info(f"• Fichiers sans données : {ko:,}")
    logger.info(f"• Total enregistrements : {sum(totals.values()):,}")
    logger.info(f"• Temps d'exécution     : {dt:.2f} s")
    logger.info("=" * 70 + "\n")

if __name__ == "__main__":
    main()
