import os
import glob
from typing import Dict, List, Tuple, Any, Optional
import pandas as pd
import dask.dataframe as dd
from dask import delayed, compute
from dask.distributed import Client
import logging
import time
from datetime import datetime
import gzip
from modules.unzip_files import unzip_file

# Configuration du logging avec format unifié
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)-8s - %(message)s'
)
logger = logging.getLogger(__name__)

# ------------------ Configuration ------------------
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", os.path.join(BASE_PATH, "raw_data/MSC"))
PROCESSED_DATA_PATH = os.environ.get("PROCESSED_DATA_PATH", os.path.join(BASE_PATH, "processed_data/ETL/MSC"))

# Types d'enregistrements supportés
RECORD_TYPES = {
    'UMTS_GSM': "CME20MSC2013Aber.CallDataRecord.uMTSGSMPLMNCallDataRecord",
    'COMPOSITE': "CME20MSC2013Aber.CallDataRecord.compositeCallDataRecord"
}

# ------------------ Services ------------------
# Détection parsing (inclut transitINOutgoingCall et callForwarding)
SERVICE_BLOCKS = [
    "mSOriginatingSMSinMSC",
    "mSTerminatingSMSinMSC",
    "mSTerminating",
    "sSProcedure",
    "transit",
    "mSOriginating",
    "transitINOutgoingCall",
    "callForwarding",
]

# Sorties finales (dossiers/fichiers écrits)
OUTPUT_SERVICES = [
    "mSOriginatingSMSinMSC",
    "mSTerminatingSMSinMSC",
    "mSTerminating",
    "sSProcedure",
    "transit",
    "mSOriginating",   # inclut fusion composite
    "callForwarding",  # nouvelle sortie dédiée
]

# ------------------ Utils ------------------
def _clean_value(v: str) -> Optional[str]:
    v = v.strip()
    if v == "":
        return None
    if len(v) > 3 and v.startswith("'") and v.endswith("'H"):
        return v[1:-2]
    if len(v) >= 2 and v[0] == '"' and v[-1] == '"':
        return v[1:-1]
    if len(v) >= 2 and v[0] == "'" and v[-1] == "'":
        return v[1:-1]
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

# ---- Recherche récursive de callForwarding (peut être imbriqué) ----
def _find_blocks_by_key(node: Any, target_key: str):
    if isinstance(node, dict):
        for k, v in node.items():
            if k == target_key and isinstance(v, dict):
                yield v
            if isinstance(v, dict):
                yield from _find_blocks_by_key(v, target_key)
    return

def _first_call_forwarding_block(rec_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for blk in _find_blocks_by_key(rec_dict, "callForwarding"):
        return blk
    return None

# ------------------ Parser ------------------

def _top_level_ordered_keys(rec_dict: Dict[str, Any]) -> List[str]:
    return [k for k in rec_dict.keys() if not str(k).startswith("__")]

def _next_following_block(rec_dict: Dict[str, Any], key: str, follower_key: str) -> Optional[Dict[str, Any]]:
    """Return the first follower_key dict that appears AFTER key among top-level ordered keys (not necessarily immediate)."""
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
        if not line:
            i += 1
            continue

        if line == "}":
            return node, i + 1

        # cas: "key" puis ligne suivante "{"
        if ":" not in line and not line.endswith("{") and (i + 1) < len(lines) and lines[i+1].strip() == "{":
            key = line
            sub, nxt = _parse_block(lines, i + 2)
            node[key] = sub
            i = nxt
            continue

        # cas: "key : {" ou "key {"
        if line.endswith("{"):
            if ":" in line:
                key = line.split(":", 1)[0].strip()
            else:
                key = line[:-1].strip()
            sub, nxt = _parse_block(lines, i + 1)
            node[key] = sub
            i = nxt
            continue

        # cas: "key : value"
        if ":" in line:
            key, value = line.split(":", 1)
            node[key.strip()] = _clean_value(value)
            i += 1
            continue

        i += 1

    return node, i

def parse_record(lines: List[str]) -> Tuple[Optional[str], Dict[str, Any]]:
    if not lines:
        return None, {}
    record_type = lines[0].strip()
    try:
        open_idx = next(idx for idx, l in enumerate(lines) if l.strip() == "{")
    except StopIteration:
        return record_type, {"__record_type__": record_type}

    node, _ = _parse_block(lines, open_idx + 1)
    node["__record_type__"] = record_type
    return record_type, node

def read_msc_file(content: str) -> List[Tuple[str, Dict]]:
    """Lit le contenu d'un fichier MSC et retourne la liste des enregistrements."""
    #logger.info("Début de l'analyse du fichier MSC")
    raw_lines = content.split('\n')
    lines = [ln.strip() for ln in raw_lines if ln.strip()]

    records: List[Tuple[str, Dict]] = []
    i = 0
    found_any_type = False
    while i < len(lines):
        line = lines[i].strip()
        if line in (RECORD_TYPES['UMTS_GSM'], RECORD_TYPES['COMPOSITE']):
            found_any_type = True
            chunk = [line]
            i += 1
            if i < len(lines) and lines[i].strip() == "{":
                chunk.append("{")
                i += 1
                brace = 1
                while i < len(lines) and brace > 0:
                    current_line = lines[i].strip()
                    chunk.append(current_line)
                    if current_line == "{":
                        brace += 1
                    elif current_line == "}":
                        brace -= 1
                    i += 1
            records.append(parse_record(chunk))
        else:
            i += 1

    if not found_any_type:
        print("  [WARN] Aucun type d'enregistrement MSC reconnu dans ce fichier "
              f"(attendu: '{RECORD_TYPES['UMTS_GSM']}' ou '{RECORD_TYPES['COMPOSITE']}')")
    return records

# ------------------ Split par service ------------------

def _split_record_by_service(rec_dict: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    - Composite: deux sorties ->
        • 'mSOriginating' (fusion top + mSOriginating.* + transitINOutgoingCall (immédiatement suivant) + callForwarding.*)
        • 'callForwarding' (fusion top + callForwarding.* + transitINOutgoingCall (immédiatement suivant))
      + autres services exportés normalement.
    - Non composite: export standard ; pour 'mSOriginating' ajouter aussi callForwarding.* si trouvé.
    """
    result: Dict[str, Dict[str, Any]] = {}
    top = {k: v for k, v in rec_dict.items() if k not in SERVICE_BLOCKS and not str(k).startswith("__")}
    rec_type = rec_dict.get("__record_type__")

    if rec_type == RECORD_TYPES['COMPOSITE']:
        # Pair transit qui suit immédiatement un service
        mso = rec_dict.get("mSOriginating")
        # find callForwarding anywhere (nested or top-level)
        cfw = _first_call_forwarding_block(rec_dict)
        tio_after_mso = _next_following_block(rec_dict, "mSOriginating", "transitINOutgoingCall")
        # If callForwarding is top-level, pair with next following transit; otherwise fallback to first top-level transit
        if "callForwarding" in rec_dict and isinstance(rec_dict.get("callForwarding"), dict):
            tio_after_cfw = _next_following_block(rec_dict, "callForwarding", "transitINOutgoingCall")
        else:
            tio_after_cfw = rec_dict.get("transitINOutgoingCall") if isinstance(rec_dict.get("transitINOutgoingCall"), dict) else None

        # (1) mSOriginating fusionné
        merged_mso: Dict[str, Any] = {}
        merged_mso.update(_flatten(top))
        if isinstance(mso, dict):
            merged_mso.update(_flatten(mso, "mSOriginating"))
        if isinstance(tio_after_mso, dict):
            merged_mso.update(_flatten(tio_after_mso, "transitINOutgoingCall"))
        if isinstance(cfw, dict):
            merged_mso.update(_flatten(cfw, "callForwarding"))
        if merged_mso:
            result["mSOriginating"] = merged_mso

        # (2) callForwarding fusionné
        if isinstance(cfw, dict):
            merged_cfw: Dict[str, Any] = {}
            merged_cfw.update(_flatten(top))
            merged_cfw.update(_flatten(cfw, "callForwarding"))
            if isinstance(tio_after_cfw, dict):
                merged_cfw.update(_flatten(tio_after_cfw, "transitINOutgoingCall"))
            result["callForwarding"] = merged_cfw

        # autres services (hors ceux déjà gérés et le transit follower)
        for svc in SERVICE_BLOCKS:
            if svc in ("mSOriginating", "callForwarding", "transitINOutgoingCall"):
                continue
            blk = rec_dict.get(svc)
            if isinstance(blk, dict):
                row: Dict[str, Any] = {}
                row.update(_flatten(top))
                row.update(_flatten(blk, svc))
                result[svc] = row
        return result

    # Non composite
    for svc in SERVICE_BLOCKS:
        blk = rec_dict.get(svc)
        if isinstance(blk, dict):
            row: Dict[str, Any] = {}
            row.update(_flatten(top))
            row.update(_flatten(blk, svc))
            if svc == "mSOriginating":
                cfw = _first_call_forwarding_block(rec_dict)
                if isinstance(cfw, dict):
                    row.update(_flatten(cfw, "callForwarding"))
            result[svc] = row
    return result

# ------------------ DataFrame par service ------------------
def transform_to_dataframe(records: List[Tuple[str, Dict]], service_name: str) -> pd.DataFrame:
    """Transforme les enregistrements en DataFrame pour un service donné."""
    #logger.info(f"Transformation des enregistrements pour le service {service_name}")
    rows: List[Dict[str, Any]] = []
    for _, d in records:
        if not isinstance(d, dict):
            continue
        split = _split_record_by_service(d)
        if service_name in split:
            rows.append(split[service_name])
    if not rows:
        return pd.DataFrame()
    
    df = pd.DataFrame(rows)
    
    # Conversion des timestamps si présents
    time_columns = ['startTime', 'eventTime', 'callStartTime', 'answerTime', 'releaseTime']
    for col in time_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], format='%Y%m%d%H%M%S')
            except Exception as e:
                logger.warning(f"Erreur de conversion timestamp pour {col}: {str(e)}")
    
    # Conversion des colonnes numériques communes
    numeric_columns = ['duration', 'charge', 'serviceIdentifier', 'causeForTerm']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='ignore')
            
    return df

# ------------------ Main ------------------
def process_file(file_path: str) -> Dict[str, pd.DataFrame]:
    """Traite un fichier MSC et retourne les DataFrames pour chaque service."""
    logger.info(f"Traitement du fichier : {os.path.basename(file_path)}")
    
    try:
        # Lecture et décompression du fichier si nécessaire
        if file_path.endswith('.gz'):
            #logger.debug(f"Décompression du fichier {file_path}")
            content = unzip_file(file_path)
            content = content.decode('utf-8', errors='ignore')
        else:
            #logger.debug(f"Lecture du fichier {file_path}")
            with open(file_path, 'rt', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
        #logger.info(f"Fichier lu avec succès, taille: {len(content)} caractères")
        
        # Parser le contenu
        records = read_msc_file(content)
        
        if not records:
            logger.warning(f"Aucun enregistrement trouvé dans {file_path}")
            return {svc: pd.DataFrame() for svc in OUTPUT_SERVICES}
            
        # Transformer en DataFrames par service
        result = {}
        for service in OUTPUT_SERVICES:
            df = transform_to_dataframe(records, service)
            result[service] = df
            #if not df.empty:
                #logger.info(f"Service {service}: {len(df)} enregistrements")
                
        return result
        
    except Exception as e:
        logger.error(f"Erreur lors du traitement de {file_path}: {str(e)}")
        return {svc: pd.DataFrame() for svc in OUTPUT_SERVICES}

def process_all_files(tt_files: List[str]) -> Dict[str, List[pd.DataFrame]]:
    """Traite tous les fichiers MSC et retourne les DataFrames par service."""
    buckets: Dict[str, List[pd.DataFrame]] = {svc: [] for svc in OUTPUT_SERVICES}
    total_rows = 0
    successful_files = 0
    failed_files = 0
    
    for file_path in tt_files:
        try:
            # Traiter le fichier
            result = process_file(file_path)
            has_data = False
            
            # Extraire les DataFrames par service
            for svc in OUTPUT_SERVICES:
                if svc in result and isinstance(result[svc], pd.DataFrame) and not result[svc].empty:
                    buckets[svc].append(result[svc])
                    total_rows += len(result[svc])
                    has_data = True
            
            if has_data:
                successful_files += 1
            else:
                failed_files += 1
                
        except Exception as e:
            logger.error(f"Erreur lors du traitement de {file_path}: {str(e)}")
            failed_files += 1
            
    logger.info("\n" + "="*70)
    logger.info("Résumé du traitement:")
    logger.info(f"  • Fichiers traités avec succès: {successful_files}")
    logger.info(f"  • Fichiers en erreur:          {failed_files}")
    logger.info(f"  • Total des lignes extraites:  {total_rows}")
    logger.info("="*70 + "\n")
    
    return buckets

def main():
    """Point d'entrée principal du processus ETL MSC."""
    start_time = time.time()
    
    logger.info("-" * 50)
    logger.info("ETL MSC - Démarrage")
    logger.info(f"• Date      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"• Source    : {os.path.basename(RAW_DATA_PATH)}")
    logger.info(f"• Cible     : {os.path.basename(PROCESSED_DATA_PATH)}")

    # Création des dossiers de sortie
    for service in OUTPUT_SERVICES:
        os.makedirs(os.path.join(PROCESSED_DATA_PATH, service), exist_ok=True)

    # Liste des fichiers à traiter
    input_files = sorted(glob.glob(os.path.join(RAW_DATA_PATH, "TTFILE*")))
    if not input_files:
        logger.error(f"Aucun fichier TTFILE trouvé dans {RAW_DATA_PATH}")
        return
    logger.info(f"• À traiter : {len(input_files)} fichiers")

    # Initialiser le client Dask
    with Client(n_workers=4) as client:
        #logger.info(f"Dask dashboard disponible à : {client.dashboard_link}")

        # Traitement des fichiers
        logger.info("\nTraitement des fichiers...")
        
        # Initialiser les accumulateurs
        buckets: Dict[str, List[pd.DataFrame]] = {svc: [] for svc in OUTPUT_SERVICES}
        total_rows = 0
        successful_files = 0
        failed_files = 0
        
        # Traiter chaque fichier avec Dask
        futures = []
        for file_path in input_files:
            future = client.submit(process_file, file_path)
            futures.append(future)
            
        # Attendre et traiter les résultats au fur et à mesure
        for future in futures:
            result = future.result()
            
            if result is None:
                failed_files += 1
                continue
            
            has_data = False
            for svc in OUTPUT_SERVICES:
                df = result.get(svc)
                if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
                    buckets[svc].append(df)
                    total_rows += len(df)
                    has_data = True
                    
            if has_data:
                successful_files += 1
            else:
                failed_files += 1

        logger.info("\nTraitement des fichiers terminé.")

        # Sauvegarde des résultats en format parquet
        logger.info("\nSauvegarde des fichiers parquet....")
        
        try:
            for service, dataframes in buckets.items():
                if not dataframes:
                    logger.debug(f"• {service}: Pas de données à sauvegarder")
                    continue
                    
                try:
                    combined_df = pd.concat(dataframes, ignore_index=True)
                    output_path = os.path.join(PROCESSED_DATA_PATH, service, f"{service}.parquet")
                    combined_df.to_parquet(output_path, index=False)
                    logger.info(f"• {service}: {len(combined_df):,} enregistrements")
                    
                except Exception as error:
                    logger.error(f"Erreur lors de la sauvegarde de {service}: {str(error)}")
            
        except Exception as error:
            logger.error(f"Erreur globale: {str(error)}")
            raise
            
        finally:
            # Résumé final
            execution_time = time.time() - start_time
            logger.info("="*70)
            logger.info("\nRésultats du traitement:")
            logger.info(f"• Fichiers traités avec succès : {successful_files:,}")
            logger.info(f"• Fichiers en erreur          : {failed_files:,}")
            logger.info(f"• Total des enregistrements   : {total_rows:,}")
            logger.info(f"• Temps d'exécution           : {execution_time:.2f} secondes")
            logger.info("="*70 + "\n")
