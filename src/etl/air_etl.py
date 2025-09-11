# -*- coding: utf-8 -*-
import os
import glob
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import gzip
import dask
import dask.delayed as delayed

logging.basicConfig(level=logging.INFO, format="%(levelname)-8s - %(message)s")
logger = logging.getLogger(__name__)

BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
SOURCE_DATA_PATH = r"/home/eddi/Desktop/CDR_EDR_unzipped/raw_data/AIR"
RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", os.path.join(BASE_PATH, "data/raw_data/AIR"))

RECORD_TYPES = {
    "ADJUSTMENT": "AIROUTPUTCDR_413_EC22.DetailOutputRecord.adjustmentRecordV2",
    "REFILL":     "AIROUTPUTCDR_413_EC22.DetailOutputRecord.refillRecordV2",
}
OUTPUT_DIRS = {"ADJUSTMENT": "adjustmentRecordV2", "REFILL": "refillRecordV2"}

# --------- utils / parser (inchangés) ---------
def _clean_value(v: str) -> Optional[str]:
    v = v.strip()
    if v == "": return None
    if len(v) >= 3 and v[0] == "'" and v.endswith("'D"): return v[1:-2]
    if len(v) > 3 and v.startswith("'") and v.endswith("'H"): return v[1:-2]
    if (len(v) >= 2 and v[0] in "'\"" and v[-1] == v[0]): return v[1:-1]
    return v

def _flatten(d: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, val in d.items():
        key = f"{prefix}{k}" if prefix == "" else f"{prefix}.{k}"
        if isinstance(val, dict):
            out.update(_flatten(val, key))
        else:
            out[key] = val
    return out

def _parse_block(lines: List[str], start_idx: int) -> Tuple[Dict[str, Any], int]:
    node: Dict[str, Any] = {}
    i = start_idx; n = len(lines)
    def is_open_brace_line(idx: int) -> bool:
        return 0 <= idx < n and lines[idx].strip() == "{"
    while i < n:
        line = lines[i].strip()
        if not line: i += 1; continue
        if line == "}": return node, i + 1
        if (line.startswith("[") and line.endswith("]")) and is_open_brace_line(i + 1):
            key = line; sub, nxt = _parse_block(lines, i + 2); node[key] = sub; i = nxt; continue
        if ":" not in line and not line.endswith("{") and is_open_brace_line(i + 1):
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

def read_air_file(content: str) -> List[Tuple[str, Dict[str, Any]]]:
    SUP = (RECORD_TYPES["ADJUSTMENT"], RECORD_TYPES["REFILL"])
    lines = [ln.rstrip() for ln in content.split("\n") if ln.strip()]
    records: List[Tuple[str, Dict[str, Any]]] = []; i = 0; n = len(lines)
    while i < n:
        line = lines[i].strip()
        if line in SUP:
            chunk = [line]; i += 1
            if i < n and lines[i].strip() == "{":
                chunk.append("{"); i += 1; brace = 1
                while i < n and brace > 0:
                    current = lines[i].strip(); chunk.append(current)
                    if current == "{": brace += 1
                    elif current == "}": brace -= 1
                    i += 1
            records.append(parse_record(chunk))
        else: i += 1
    return records

def to_dataframe(records: List[Tuple[str, Dict[str, Any]]], wanted_type: str) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for rtype, data in records:
        if rtype != wanted_type or not isinstance(data, dict): continue
        top = {k: v for k, v in data.items() if not str(k).startswith("__")}
        rows.append(_flatten(top))
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def _read_file_text(file_path: str) -> str:
    if file_path.endswith(".gz"):
        with gzip.open(file_path, "rt", encoding="utf-8", errors="ignore") as f: return f.read()
    else:
        with open(file_path, "rt", encoding="utf-8", errors="ignore") as f: return f.read()

def _safe_parquet_name(src_path: str) -> str:
    base = os.path.basename(src_path)
    if base.endswith(".gz"): base = base[:-3]
    return os.path.splitext(base)[0] + ".parquet"

def _process_and_write_one(file_path: str, out_root: str) -> Dict[str, int]:
    """Delayed-friendly: parse le fichier et écrit 0..2 parquets (ADJUSTMENT/REFILL). Retourne le nb de lignes par type."""
    content = _read_file_text(file_path)
    recs = read_air_file(content)
    stats = {"ADJUSTMENT": 0, "REFILL": 0}
    for label, rec_type in RECORD_TYPES.items():
        df = to_dataframe(recs, rec_type)
        if not df.empty:
            out_dir = os.path.join(out_root, OUTPUT_DIRS[label])
            os.makedirs(out_dir, exist_ok=True)
            out_file = os.path.join(out_dir, _safe_parquet_name(file_path))
            # un fichier parquet par fichier source (pas d’append, pas d’agrégation)
            df.to_parquet(out_file, index=False, engine="pyarrow")
            stats[label] = len(df)
    return stats

def main():
    start = time.time()
    logger.info("-" * 50)
    logger.info("ETL AIR - Démarrage")
    logger.info(f"• Date   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"• Source : {os.path.basename(SOURCE_DATA_PATH)}")
    logger.info(f"• Cible  : {os.path.basename(RAW_DATA_PATH)}")

    for k in OUTPUT_DIRS.values():
        os.makedirs(os.path.join(RAW_DATA_PATH, k), exist_ok=True)

    air_files = sorted(glob.glob(os.path.join(SOURCE_DATA_PATH, "*.AIR"))) + \
                sorted(glob.glob(os.path.join(SOURCE_DATA_PATH, "*.AIR.gz")))
    if not air_files:
        logger.error(f"Aucun fichier AIR trouvé dans {SOURCE_DATA_PATH}")
        return
    logger.info(f"• À traiter : {len(air_files)} fichiers")

    # Planifier en parallèle : une tâche Dask par fichier
    tasks = [delayed(_process_and_write_one)(fp, RAW_DATA_PATH) for fp in air_files]

    from modules.dask_runtime import _start_client, quiet_close
    client = _start_client()
    try:
        results: List[Dict[str, int]] = dask.compute(*tasks)
    finally:
        quiet_close(client)

    total = {"ADJUSTMENT": 0, "REFILL": 0}
    ok = ko = 0
    for st in results:
        rows = sum(st.values())
        ok += 1 if rows > 0 else 0
        ko += 0 if rows > 0 else 1
        for k in total: total[k] += st.get(k, 0)

    elapsed = time.time() - start
    logger.info("="*70)
    logger.info("Résultats du traitement:")
    logger.info(f"• Fichiers avec données : {ok:,}")
    logger.info(f"• Fichiers sans données : {ko:,}")
    logger.info(f"• ADJUSTMENT            : {total['ADJUSTMENT']:,}")
    logger.info(f"• REFILL                : {total['REFILL']:,}")
    logger.info(f"• Total enregistrements : {(total['ADJUSTMENT']+total['REFILL']):,}")
    logger.info(f"• Temps d'exécution     : {elapsed:.2f} s")
    logger.info("=" * 70 + "\n")

if __name__ == "__main__":
    main()
