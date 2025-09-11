import os, glob, time, gzip, logging
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional

import numpy as np
import pandas as pd
import dask
import dask.delayed as delayed
import dask.dataframe as dd

from modules.io_one_parquet import write_one_parquet
from modules.dask_runtime import _start_client, quiet_close

logging.basicConfig(level=logging.INFO, format='%(levelname)-8s - %(message)s')
logger = logging.getLogger(__name__)

BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
SOURCE_DATA_PATH = r"D:\Utilisateurs\Public\IA_BAFM\PROJECT\DATA_SAMPLE\CDR_EDR_zipped\MSC"
RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", os.path.join(BASE_PATH, "data/raw_data/MSC"))

RECORD_TYPES = {
    'UMTS_GSM': "CME20MSC2013Aber.CallDataRecord.uMTSGSMPLMNCallDataRecord",
    'COMPOSITE': "CME20MSC2013Aber.CallDataRecord.compositeCallDataRecord"
}
SERVICE_BLOCKS = [
    "mSOriginatingSMSinMSC","mSTerminatingSMSinMSC","mSTerminating","sSProcedure",
    "transit","mSOriginating","transitINOutgoingCall","callForwarding",
]
OUTPUT_SERVICES = [
    "mSOriginatingSMSinMSC","mSTerminatingSMSinMSC","mSTerminating","sSProcedure",
    "transit","mSOriginating","callForwarding",
]

def _clean_value(v: str) -> Optional[str]:
    v = v.strip()
    if v == "": return None
    if len(v) > 3 and v.startswith("'") and v.endswith("'H"): return v[1:-2]
    if len(v) >= 2 and v[0] == '"' and v[-1] == '"': return v[1:-1]
    if len(v) >= 2 and v[0] == "'" and v[-1] == "'": return v[1:-1]
    return v

def _flatten(d: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    out = {}
    for k, val in d.items():
        key = f"{prefix}{k}" if prefix == "" else f"{prefix}.{k}"
        if isinstance(val, dict):
            out.update(_flatten(val, key))
        else:
            out[key] = val
    return out

def _find_blocks_by_key(node: Any, target_key: str):
    if isinstance(node, dict):
        for k, v in node.items():
            if k == target_key and isinstance(v, dict):
                yield v
            if isinstance(v, dict):
                yield from _find_blocks_by_key(v, target_key)

def _first_call_forwarding_block(rec_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for blk in _find_blocks_by_key(rec_dict, "callForwarding"):
        return blk
    return None

def _top_level_ordered_keys(rec_dict: Dict[str, Any]) -> List[str]:
    return [k for k in rec_dict.keys() if not str(k).startswith("__")]

def _next_following_block(rec_dict: Dict[str, Any], key: str, follower_key: str) -> Optional[Dict[str, Any]]:
    keys = _top_level_ordered_keys(rec_dict)
    seen = False
    for k in keys:
        if not seen:
            if k == key:
                seen = True
            continue
        if k == follower_key:
            v = rec_dict.get(follower_key)
            return v if isinstance(v, dict) else None
    return None

def _parse_block(lines: List[str], start_idx: int) -> Tuple[Dict[str, Any], int]:
    node: Dict[str, Any] = {}
    i = start_idx
    while i < len(lines):
        line = lines[i].strip()
        if not line: i += 1; continue
        if line == "}": return node, i + 1
        if ":" not in line and not line.endswith("{") and (i + 1) < len(lines) and lines[i+1].strip() == "{":
            key = line; sub, nxt = _parse_block(lines, i + 2); node[key] = sub; i = nxt; continue
        if line.endswith("{"):
            key = line.split(":", 1)[0].strip() if ":" in line else line[:-1].strip()
            sub, nxt = _parse_block(lines, i + 1); node[key] = sub; i = nxt; continue
        if ":" in line:
            key, value = line.split(":", 1); node[key.strip()] = _clean_value(value); i += 1; continue
        i += 1
    return node, i

def parse_record(lines: List[str]) -> Tuple[Optional[str], Dict[str, Any]]:
    if not lines: return None, {}
    record_type = lines[0].strip()
    try:
        open_idx = next(idx for idx, l in enumerate(lines) if l.strip() == "{")
    except StopIteration:
        return record_type, {"__record_type__": record_type}
    node, _ = _parse_block(lines, open_idx + 1)
    node["__record_type__"] = record_type
    return record_type, node

def read_msc_file(content: str) -> List[Tuple[str, Dict]]:
    raw_lines = content.split('\n')
    lines = [ln.strip() for ln in raw_lines if ln.strip()]
    records: List[Tuple[str, Dict]] = []
    i = 0; found_any_type = False
    while i < len(lines):
        line = lines[i].strip()
        if line in (RECORD_TYPES['UMTS_GSM'], RECORD_TYPES['COMPOSITE']):
            found_any_type = True
            chunk = [line]; i += 1
            if i < len(lines) and lines[i].strip() == "{":
                chunk.append("{"); i += 1; brace = 1
                while i < len(lines) and brace > 0:
                    current_line = lines[i].strip(); chunk.append(current_line)
                    if current_line == "{": brace += 1
                    elif current_line == "}": brace -= 1
                    i += 1
            records.append(parse_record(chunk))
        else:
            i += 1
    if not found_any_type:
        logger.warning("Aucun type d'enregistrement MSC reconnu dans ce fichier")
    return records

def _split_record_by_service(rec_dict: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    top = {k: v for k, v in rec_dict.items() if k not in SERVICE_BLOCKS and not str(k).startswith("__")}
    rec_type = rec_dict.get("__record_type__")
    if rec_type == RECORD_TYPES['COMPOSITE']:
        mso = rec_dict.get("mSOriginating")
        cfw = _first_call_forwarding_block(rec_dict)
        tio_after_mso = _next_following_block(rec_dict, "mSOriginating", "transitINOutgoingCall")
        tio_after_cfw = _next_following_block(rec_dict, "callForwarding", "transitINOutgoingCall") if "callForwarding" in rec_dict else rec_dict.get("transitINOutgoingCall")
        merged_mso: Dict[str, Any] = {}
        merged_mso.update(_flatten(top))
        if isinstance(mso, dict): merged_mso.update(_flatten(mso, "mSOriginating"))
        if isinstance(tio_after_mso, dict): merged_mso.update(_flatten(tio_after_mso, "transitINOutgoingCall"))
        if isinstance(cfw, dict): merged_mso.update(_flatten(cfw, "callForwarding"))
        if merged_mso: result["mSOriginating"] = merged_mso
        if isinstance(cfw, dict):
            merged_cfw: Dict[str, Any] = {}
            merged_cfw.update(_flatten(top))
            merged_cfw.update(_flatten(cfw, "callForwarding"))
            if isinstance(tio_after_cfw, dict): merged_cfw.update(_flatten(tio_after_cfw, "transitINOutgoingCall"))
            result["callForwarding"] = merged_cfw
        for svc in SERVICE_BLOCKS:
            if svc in ("mSOriginating", "callForwarding", "transitINOutgoingCall"): continue
            blk = rec_dict.get(svc)
            if isinstance(blk, dict):
                row: Dict[str, Any] = {}
                row.update(_flatten(top)); row.update(_flatten(blk, svc))
                result[svc] = row
        return result
    for svc in SERVICE_BLOCKS:
        blk = rec_dict.get(svc)
        if isinstance(blk, dict):
            row: Dict[str, Any] = {}
            row.update(_flatten(top)); row.update(_flatten(blk, svc))
            if svc == "mSOriginating":
                cfw = _first_call_forwarding_block(rec_dict)
                if isinstance(cfw, dict):
                    row.update(_flatten(cfw, "callForwarding"))
            result[svc] = row
    return result

def transform_to_dataframe(records: List[Tuple[str, Dict]], service_name: str) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for _, d in records:
        if not isinstance(d, dict): continue
        split = _split_record_by_service(d)
        if service_name in split: rows.append(split[service_name])
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)
    time_columns = ['startTime', 'eventTime', 'callStartTime', 'answerTime', 'releaseTime']
    for col in time_columns:
        if col in df.columns:
            try: df[col] = pd.to_datetime(df[col], format='%Y%m%d%H%M%S')
            except Exception: pass
    numeric_columns = ['duration', 'charge', 'serviceIdentifier', 'causeForTerm']
    for col in numeric_columns:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors='ignore')
    return df

@delayed
def process_file_delayed(file_path: str) -> Dict[str, pd.DataFrame]:
    try:
        if file_path.endswith('.gz'):
            with gzip.open(file_path, 'rb') as gz_file:
                content = gz_file.read().decode('utf-8', errors='ignore')
        else:
            with open(file_path, 'rt', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        records = read_msc_file(content)
        if not records:
            return {svc: pd.DataFrame() for svc in OUTPUT_SERVICES}
        result = {}
        for service in OUTPUT_SERVICES:
            df = transform_to_dataframe(records, service)
            result[service] = df
        return result
    except Exception as e:
        logger.error(f"Erreur {file_path}: {str(e)}")
        return {svc: pd.DataFrame() for svc in OUTPUT_SERVICES}

def _normalize_concat(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Ajoute toutes les colonnes manquantes d'un coup (anti-fragmentation) + ordre + dtypes object."""
    if df is None or df.empty:
        return pd.DataFrame({c: pd.Series([], dtype="object") for c in cols})
    missing = [c for c in cols if c not in df.columns]
    if missing:
        add = pd.DataFrame(
            {c: pd.Series(np.full(len(df), None, dtype=object), dtype="object") for c in missing},
            index=df.index,
        )
        df = pd.concat([df, add], axis=1, copy=False)
    df = df.reindex(columns=cols, copy=False)
    to_obj = {c: "object" for c in df.columns if df[c].dtype != "object"}
    if to_obj:
        df = df.astype(to_obj, copy=False)
    return df

def _meta(cols: List[str]) -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})

def main():
    start_time = time.time()
    logger.info("-" * 50)
    logger.info("ETL MSC - Démarrage")
    logger.info(f"• Date      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"• Source    : {os.path.basename(SOURCE_DATA_PATH)}")
    logger.info(f"• Cible     : {os.path.basename(RAW_DATA_PATH)}")

    for service in OUTPUT_SERVICES:
        os.makedirs(os.path.join(RAW_DATA_PATH, service), exist_ok=True)

    input_files = sorted(glob.glob(os.path.join(SOURCE_DATA_PATH, "TTFILE*")))
    if not input_files:
        logger.error(f"Aucun fichier TTFILE trouvé dans {SOURCE_DATA_PATH}")
        return
    logger.info(f"• À traiter : {len(input_files)} fichiers")

    tasks = [process_file_delayed(fp) for fp in input_files]

    # Construire les DDF par service
    ddfs: Dict[str, dd.DataFrame] = {}
    for svc in OUTPUT_SERVICES:
        delayed_frames = [t.map(lambda d, s=svc: d.get(s, pd.DataFrame())) for t in tasks]

        # Échantillonnage léger pour colonnes
        head = []
        for df_del in delayed_frames[:5]:
            try:
                df0 = df_del.compute()
                if not df0.empty:
                    head.append(df0.iloc[:0])
            except Exception:
                pass
        cols = list(pd.concat(head, ignore_index=True).columns) if head else []

        # Normalisation via map_partitions pour garantir meta homogène
        if cols:
            normalized = [df_del.map(lambda df, c=cols: _normalize_concat(df, c)) for df_del in delayed_frames]
            ddfs[svc] = dd.from_delayed(normalized, meta=_meta(cols))
        else:
            # aucune donnée observée: DataFrame vide convenable
            ddfs[svc] = dd.from_pandas(pd.DataFrame(), npartitions=1)

    client = _start_client()
    try:
        logger.info("\nSauvegarde parquet....")
        with dask.config.set(optimizations=[], optimize_graph=False):
            persisted = {svc: ddf.persist() for svc, ddf in ddfs.items()}
        for svc, ddf in persisted.items():
            out_path = os.path.join(RAW_DATA_PATH, svc, f"{svc}.parquet")
            write_one_parquet(ddf, out_path)

        total_rows = 0
        for svc, ddf in persisted.items():
            n = int(ddf.map_partitions(len, meta=("rows", "i8")).sum().compute(optimize_graph=False))
            total_rows += n
            logger.info(f"• {svc}: {n:,} enregistrements")

        execution_time = time.time() - start_time
        logger.info("="*70)
        logger.info("\nRésultats du traitement:")
        logger.info(f"• Fichiers traités : {len(input_files):,}")
        logger.info(f"• Total des enregistrements   : {total_rows:,}")
        logger.info(f"• Temps d'exécution           : {execution_time:.2f} secondes")
        logger.info("="*70 + "\n")
    finally:
        quiet_close(client)

if __name__ == "__main__":
    main()
