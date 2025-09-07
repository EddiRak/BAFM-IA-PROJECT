import os
import glob
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dask.distributed import Client
from modules.unzip_files import unzip_file  # réutilisation
import gzip

# ------------------ Logging ------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)-8s - %(message)s")
logger = logging.getLogger(__name__)

# ------------------ Configuration ------------------
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", os.path.join(BASE_PATH, "raw_data/AIR"))
PROCESSED_DATA_PATH = os.environ.get("PROCESSED_DATA_PATH", os.path.join(BASE_PATH, "processed_data/ETL/AIR"))

# Types d'enregistrements supportés
RECORD_TYPES = {
    "ADJUSTMENT": "AIROUTPUTCDR_413_EC22.DetailOutputRecord.adjustmentRecordV2",
    "REFILL":     "AIROUTPUTCDR_413_EC22.DetailOutputRecord.refillRecordV2",
}

OUTPUT_DIRS = {
    "ADJUSTMENT": "adjustmentRecordV2",
    "REFILL":     "refillRecordV2",
}

# ------------------ Utils ------------------
def _clean_value(v: str) -> Optional[str]:
    """Nettoie les valeurs : quotes, 'D' suffix, vides, hex 'H'."""
    v = v.strip()
    if v == "":
        return None
    # '41232'D -> 41232
    if len(v) >= 3 and v[0] == "'" and v.endswith("'D"):
        return v[1:-2]
    # 'xxx'H -> xxx
    if len(v) > 3 and v.startswith("'") and v.endswith("'H"):
        return v[1:-2]
    # "abc" / 'abc' -> abc
    if (len(v) >= 2 and v[0] == '"' and v[-1] == '"') or (len(v) >= 2 and v[0] == "'" and v[-1] == "'"):
        return v[1:-1]
    return v

def _flatten(d: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """
    Aplatissement récursif en dot-path. Préserve les indices sous forme "[i]" dans le chemin.
    """
    out: Dict[str, Any] = {}
    for k, val in d.items():
        # Préserver exactement la clé (y compris [0], [1], etc.)
        key = f"{prefix}{k}" if prefix == "" else f"{prefix}.{k}"
        if isinstance(val, dict):
            out.update(_flatten(val, key))
        else:
            out[key] = val
    return out

# ------------------ Parser ------------------
def _parse_block(lines: List[str], start_idx: int) -> Tuple[Dict[str, Any], int]:
    """
    Parse un bloc entre accolades `{ ... }` à partir de start_idx (ligne suivant '{').
    Gère :
      - "key : value"
      - "key { ... }"  ou  "key : { ... }"
      - Lignes d'index "[0]" comme clés d'objets (sous-blocs)
      - Clés seules sur une ligne suivies de '{' à la ligne suivante
    """
    node: Dict[str, Any] = {}
    i = start_idx
    n = len(lines)

    def is_open_brace_line(idx: int) -> bool:
        return 0 <= idx < n and lines[idx].strip() == "{"

    while i < n:
        line = lines[i].strip()
        if not line:
            i += 1
            continue
        if line == "}":
            return node, i + 1

        # Cas: ligne = "[0]" et la suivante est '{' -> sous-bloc indexé
        if (line.startswith("[") and line.endswith("]")) and is_open_brace_line(i + 1):
            key = line
            sub, nxt = _parse_block(lines, i + 2)
            node[key] = sub
            i = nxt
            continue

        # Cas: clé seule puis '{' sur la ligne suivante
        if ":" not in line and not line.endswith("{") and is_open_brace_line(i + 1):
            key = line
            sub, nxt = _parse_block(lines, i + 2)
            node[key] = sub
            i = nxt
            continue

        # Cas: "key {" ou "key : {"
        if line.endswith("{"):
            if ":" in line:
                key = line.split(":", 1)[0].strip()
            else:
                key = line[:-1].strip()
            sub, nxt = _parse_block(lines, i + 1)
            node[key] = sub
            i = nxt
            continue

        # Cas: "key : value"
        if ":" in line:
            key, value = line.split(":", 1)
            node[key.strip()] = _clean_value(value)
            i += 1
            continue

        # Autres cas (ignorer la ligne)
        i += 1

    return node, i

def parse_record(lines: List[str]) -> Tuple[Optional[str], Dict[str, Any]]:
    """Parse un enregistrement complet et retourne (record_type, dict)."""
    if not lines:
        return None, {}
    record_type = lines[0].strip()

    # trouver la première '{'
    try:
        open_idx = next(idx for idx, l in enumerate(lines) if l.strip() == "{")
    except StopIteration:
        return record_type, {"__record_type__": record_type}

    node, _ = _parse_block(lines, open_idx + 1)
    node["__record_type__"] = record_type
    return record_type, node

def read_air_file(content: str) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Découpe le contenu en enregistrements AIR supportés et parse chacun.
    """
    SUP = (RECORD_TYPES["ADJUSTMENT"], RECORD_TYPES["REFILL"])
    raw_lines = content.split("\n")
    lines = [ln.rstrip() for ln in raw_lines if ln.strip()]

    records: List[Tuple[str, Dict[str, Any]]] = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i].strip()
        if line in SUP:
            chunk = [line]
            i += 1
            # si la prochaine ligne est '{', collecter jusqu'à fermeture
            if i < n and lines[i].strip() == "{":
                chunk.append("{")
                i += 1
                brace = 1
                while i < n and brace > 0:
                    current = lines[i].strip()
                    chunk.append(current)
                    if current == "{":
                        brace += 1
                    elif current == "}":
                        brace -= 1
                    i += 1
            records.append(parse_record(chunk))
        else:
            i += 1
    return records

# ------------------ Transformations ------------------
def to_dataframe(records: List[Tuple[str, Dict[str, Any]]], wanted_type: str) -> pd.DataFrame:
    """
    Extrait les enregistrements du type voulu et retourne un DataFrame aplati (colonnes dot-path).
    """
    rows: List[Dict[str, Any]] = []
    for rtype, data in records:
        if rtype != wanted_type or not isinstance(data, dict):
            continue
        # on enlève les méta
        top = {k: v for k, v in data.items() if not str(k).startswith("__")}
        rows.append(_flatten(top))

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Conversions communes (si présentes)
    # Certaines dates/times peuvent exister à divers endroits; l'utilisateur pourra convertir selon les colonnes réelles.
    # Exemple: si colonnes 'refillTimestamp' à 14 chiffres AAAAMMJJhhmmss, etc.
    # On évite les conversions fortes ici pour ne pas casser en présence de variations.
    return df

# ------------------ I/O ------------------
def _read_file_text(file_path: str) -> str:
    if file_path.endswith(".gz"):
        # Décompression en mémoire (dask.delayed possible via unzip_file si besoin)
        with gzip.open(file_path, "rt", encoding="utf-8", errors="ignore") as f:
            return f.read()
    else:
        with open(file_path, "rt", encoding="utf-8", errors="ignore") as f:
            return f.read()

def process_file(file_path: str) -> Dict[str, pd.DataFrame]:
    """
    Traite un fichier AIR et retourne un dict { 'ADJUSTMENT': df, 'REFILL': df }.
    """
    try:
        logger.info(f"Traitement : {os.path.basename(file_path)}")
        content = _read_file_text(file_path)
        records = read_air_file(content)

        out: Dict[str, pd.DataFrame] = {"ADJUSTMENT": pd.DataFrame(), "REFILL": pd.DataFrame()}
        if records:
            adj = to_dataframe(records, RECORD_TYPES["ADJUSTMENT"])
            ref = to_dataframe(records, RECORD_TYPES["REFILL"])
            out["ADJUSTMENT"] = adj
            out["REFILL"] = ref
        return out
    except Exception as e:
        logger.error(f"Erreur fichier {file_path}: {e}")
        return {"ADJUSTMENT": pd.DataFrame(), "REFILL": pd.DataFrame()}

# ------------------ Main ------------------
def main():
    start = time.time()
    logger.info("-" * 50)
    logger.info("ETL AIR - Démarrage")
    logger.info(f"• Date   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"• Source : {os.path.basename(RAW_DATA_PATH)}")
    logger.info(f"• Cible  : {os.path.basename(PROCESSED_DATA_PATH)}")

    # Dossiers de sortie
    for k, d in OUTPUT_DIRS.items():
        os.makedirs(os.path.join(PROCESSED_DATA_PATH, d), exist_ok=True)

    # Fichiers à traiter
    air_files = sorted(glob.glob(os.path.join(RAW_DATA_PATH, "*.AIR"))) + \
                sorted(glob.glob(os.path.join(RAW_DATA_PATH, "*.AIR.gz")))

    if not air_files:
        logger.error(f"Aucun fichier AIR trouvé dans {RAW_DATA_PATH}")
        return

    logger.info(f"• À traiter : {len(air_files)} fichiers")

    # Traitement distribué
    # Ajuster n_workers selon la machine/cluster
    with Client(n_workers=4) as client:

        # Traitement des fichiers
        logger.info("\nTraitement des fichiers...")

        futures = [client.submit(process_file, fp) for fp in air_files]

        buckets = {"ADJUSTMENT": [], "REFILL": []}
        ok, ko, total_rows = 0, 0, 0

        for fut in futures:
            res = fut.result()
            if res is None:
                ko += 1
                continue
            has_data = False
            for k in ("ADJUSTMENT", "REFILL"):
                df = res.get(k)
                if isinstance(df, pd.DataFrame) and not df.empty:
                    buckets[k].append(df)
                    total_rows += len(df)
                    has_data = True
            ok += 1 if has_data else 0
            ko += 0 if has_data else 1

    logger.info("\nTraitement des fichiers terminé.")

    # Sauvegarde
    logger.info("\nSauvegarde Parquet...")
    for k, frames in buckets.items():
        if not frames:
            logger.info(f"• {k}: pas de données")
            continue
        combined = pd.concat(frames, ignore_index=True)
        out_path = os.path.join(PROCESSED_DATA_PATH, OUTPUT_DIRS[k], f"{OUTPUT_DIRS[k]}.parquet")
        combined.to_parquet(out_path, index=False)
        logger.info(f"• {k}: {len(combined):,} enregistrements")

    # Résumé
    elapsed = time.time() - start
    logger.info("="*70)
    logger.info("Résultats du traitement:")
    logger.info(f"• Fichiers avec données : {ok:,}")
    logger.info(f"• Fichiers sans données : {ko:,}")
    logger.info(f"• Total enregistrements  : {total_rows:,}")
    logger.info(f"• Temps d'exécution      : {elapsed:.2f} sec")
    logger.info("="*70 + "\n")