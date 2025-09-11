import os, glob, time, gzip, logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import dask
import dask.delayed as delayed
import dask.dataframe as dd

from modules.io_one_parquet import write_one_parquet
from modules.dask_runtime import _start_client, quiet_close

logging.basicConfig(level=logging.INFO, format="%(levelname)-8s - %(message)s")
logger = logging.getLogger(__name__)

BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
SOURCE_DATA_PATH = r"D:\Utilisateurs\Public\IA_BAFM\PROJECT\DATA_SAMPLE\CDR_EDR_zipped\AIR"
RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", os.path.join(BASE_PATH, "data/raw_data/AIR"))

RECORD_TYPES = {
    "ADJUSTMENT": "AIROUTPUTCDR_413_EC22.DetailOutputRecord.adjustmentRecordV2",
    "REFILL":     "AIROUTPUTCDR_413_EC22.DetailOutputRecord.refillRecordV2",
}
OUTPUT_DIRS = {"ADJUSTMENT": "adjustmentRecordV2", "REFILL": "refillRecordV2"}

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
    def is_open_brace_line(idx: int) -> bool: return 0 <= idx < n and lines[idx].strip() == "{"
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

@delayed
def process_file_delayed(file_path: str) -> Dict[str, pd.DataFrame]:
    try:
        content = _read_file_text(file_path)
        records = read_air_file(content)
        return {
            "ADJUSTMENT": to_dataframe(records, RECORD_TYPES["ADJUSTMENT"]),
            "REFILL":     to_dataframe(records, RECORD_TYPES["REFILL"]),
        }
    except Exception as e:
        logger.error(f"Erreur fichier {file_path}: {e}")
        return {"ADJUSTMENT": pd.DataFrame(), "REFILL": pd.DataFrame()}

def _empty_meta(cols: List[str]) -> pd.DataFrame:
    # meta homogène (object)
    return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})

def main():
    start = time.time()
    logger.info("-" * 50)
    logger.info("ETL AIR - Démarrage")
    logger.info(f"• Date   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"• Source : {os.path.basename(SOURCE_DATA_PATH)}")
    logger.info(f"• Cible  : {os.path.basename(RAW_DATA_PATH)}")

    for k, d in OUTPUT_DIRS.items():
        os.makedirs(os.path.join(RAW_DATA_PATH, d), exist_ok=True)

    air_files = sorted(glob.glob(os.path.join(SOURCE_DATA_PATH, "*.AIR"))) + \
                sorted(glob.glob(os.path.join(SOURCE_DATA_PATH, "*.AIR.gz")))
    if not air_files:
        logger.error(f"Aucun fichier AIR trouvé dans {SOURCE_DATA_PATH}")
        return
    logger.info(f"• À traiter : {len(air_files)} fichiers")

    tasks = [process_file_delayed(fp) for fp in air_files]

    # Fan-out: extraire 2 familles de DDF à partir des outputs retardés
    def _family_ddf(family: str) -> dd.DataFrame:
        delayed_frames = [t.map(lambda d: d.get(family, pd.DataFrame())) for t in tasks]
        # Construire un meta robuste : on évalue une petite tête pour colonnes
        head = []
        for df_del in delayed_frames[:5]:
            try:
                df0 = df_del.compute()
                if not df0.empty:
                    head.append(df0.iloc[:0])
            except Exception:
                pass
        cols = list(pd.concat(head, ignore_index=True).columns) if head else []
        if not cols:  # aucun échantillon non-vide
            cols = ["__dummy__"]  # évite from_delayed vide
        meta = _empty_meta(cols)
        return dd.from_delayed(delayed_frames, meta=meta)

    ddfs = {fam: _family_ddf(fam) for fam in OUTPUT_DIRS.keys()}

    client = _start_client()
    try:
        logger.info("\nSauvegarde Parquet...")
        with dask.config.set(optimizations=[], optimize_graph=False):
            persisted = {k: v.persist() for k, v in ddfs.items() if v is not None}
        for k, ddf in persisted.items():
            out_path = os.path.join(RAW_DATA_PATH, OUTPUT_DIRS[k], f"{OUTPUT_DIRS[k]}.parquet")
            write_one_parquet(ddf, out_path)
        total_rows = 0
        for k, ddf in persisted.items():
            n = int(ddf.map_partitions(len, meta=("rows", "i8")).sum().compute(optimize_graph=False))
            total_rows += n
            logger.info(f"• {k}: {n:,} enregistrements")

        elapsed = time.time() - start
        logger.info("="*70)
        logger.info("Résultats du traitement:")
        logger.info(f"• Fichiers traités : {len(air_files):,}")
        logger.info(f"• Total enregistrements : {total_rows:,}")
        logger.info(f"• Temps d'exécution     : {elapsed:.2f} sec")
        logger.info("="*70 + "\n")
    finally:
        quiet_close(client)

if __name__ == "__main__":
    main()
