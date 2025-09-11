# -*- coding: utf-8 -*-
import os
import glob
from typing import Dict, List, Tuple, Any, Optional
import pandas as pd
import logging
import time
from datetime import datetime
import gzip
import dask
import dask.delayed as delayed

logging.basicConfig(level=logging.INFO, format='%(levelname)-8s - %(message)s')
logger = logging.getLogger(__name__)

BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
SOURCE_DATA_PATH = r"/home/eddi/Desktop/CDR_EDR_unzipped/raw_data/MSC"
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

# ---------- parseurs (inchangés de votre logique) ----------
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
    return

def _first_call_forwarding_block(rec_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for blk in _find_blocks_by_key(rec_dict, "callForwarding"): return blk
    return None

def _top_level_ordered_keys(rec_dict: Dict[str, Any]) -> List[str]:
    return [k for k in rec_dict.keys() if not str(k).startswith("__")]

def _next_following_block(rec_dict: Dict[str, Any], key: str, follower_key: str) -> Optional[Dict[str, Any]]:
    keys = _top_level_ordered_keys(rec_dict)
    seen = False
    for k in keys:
        if not seen:
            if k == key: seen = True
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
        if not line:
            i += 1; continue
        if line == "}":
            return node, i + 1
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
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line in (RECORD_TYPES['UMTS_GSM'], RECORD_TYPES['COMPOSITE']):
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
    return records

def _split_record_by_service(rec_dict: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    top = {k: v for k, v in rec_dict.items() if k not in SERVICE_BLOCKS and not str(k).startswith("__")}
    rec_type = rec_dict.get("__record_type__")
    if rec_type == RECORD_TYPES['COMPOSITE']:
        mso = rec_dict.get("mSOriginating")
        cfw = _first_call_forwarding_block(rec_dict)
        tio_after_mso = _next_following_block(rec_dict, "mSOriginating", "transitINOutgoingCall")
        tio_after_cfw = _next_following_block(rec_dict, "callForwarding", "transitINOutgoingCall") or rec_dict.get("transitINOutgoingCall")
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
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def _read_text(fp: str) -> str:
    if fp.endswith('.gz'):
        with gzip.open(fp, 'rt', encoding='utf-8', errors='ignore') as f: return f.read()
    with open(fp, 'rt', encoding='utf-8', errors='ignore') as f: return f.read()

def _safe_parquet_name(src_path: str) -> str:
    base = os.path.basename(src_path)
    if base.endswith(".gz"): base = base[:-3]
    return os.path.splitext(base)[0] + ".parquet"

def _process_and_write_one(file_path: str, out_root: str) -> Dict[str, int]:
    """Parse le fichier et écrit jusqu'à 7 parquets (1 par service). Retourne le nb de lignes par service."""
    text = _read_text(file_path)
    records = read_msc_file(text)
    stats: Dict[str, int] = {svc: 0 for svc in OUTPUT_SERVICES}
    for svc in OUTPUT_SERVICES:
        df = transform_to_dataframe(records, svc)
        if not df.empty:
            out_dir = os.path.join(out_root, svc)
            os.makedirs(out_dir, exist_ok=True)
            out_file = os.path.join(out_dir, _safe_parquet_name(file_path))
            df.to_parquet(out_file, index=False, engine="pyarrow")
            stats[svc] = len(df)
    return stats

def main():
    t0 = time.time()
    logger.info("-" * 50)
    logger.info("ETL MSC - Démarrage")
    logger.info(f"• Date      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"• Source    : {os.path.basename(SOURCE_DATA_PATH)}")
    logger.info(f"• Cible     : {os.path.basename(RAW_DATA_PATH)}")

    for svc in OUTPUT_SERVICES:
        os.makedirs(os.path.join(RAW_DATA_PATH, svc), exist_ok=True)

    input_files = sorted(glob.glob(os.path.join(SOURCE_DATA_PATH, "*.gz")))
    
    if not input_files:
        logger.error(f"Aucun fichier TTFILE trouvé dans {SOURCE_DATA_PATH}")
        return
    logger.info(f"• À traiter : {len(input_files)} fichiers")

    tasks = [delayed(_process_and_write_one)(fp, RAW_DATA_PATH) for fp in input_files]

    from modules.dask_runtime import _start_client, quiet_close
    client = _start_client()
    try:
        results: List[Dict[str, int]] = dask.compute(*tasks)
    finally:
        quiet_close(client)

    totals = {svc: 0 for svc in OUTPUT_SERVICES}
    ok = ko = 0
    for st in results:
        rows = sum(st.values())
        ok += 1 if rows > 0 else 0
        ko += 0 if rows > 0 else 1
        for k in totals: totals[k] += st.get(k, 0)

    dt = time.time() - t0
    logger.info("="*70)
    logger.info("Résultats du traitement:")
    for svc in OUTPUT_SERVICES:
        logger.info(f"• {svc}: {totals[svc]:,} enregistrements")
    logger.info(f"• Fichiers avec données : {ok:,}")
    logger.info(f"• Fichiers sans données : {ko:,}")
    logger.info(f"• Total enregistrements : {sum(totals.values()):,}")
    logger.info(f"• Temps d'exécution     : {dt:.2f} s")
    logger.info("=" * 70 + "\n")

if __name__ == "__main__":
    main()
