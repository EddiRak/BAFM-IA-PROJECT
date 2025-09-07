import os
import pandas as pd
import dask.dataframe as dd
from dask import delayed, compute
from dask.distributed import Client
import glob
from datetime import datetime
from typing import Dict, List, Tuple
from io import BytesIO
import logging
import gzip
import time
from modules.unzip_files import unzip_file

# Configuration du logging avec format unifié
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)-8s - %(message)s'
)
logger = logging.getLogger(__name__)

# ------------------ Configuration ------------------
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", os.path.join(BASE_PATH, "raw_data/SDP"))
PROCESSED_DATA_PATH = os.environ.get("PROCESSED_DATA_PATH", os.path.join(BASE_PATH, "processed_data/ETL/SDP"))

# Types d'enregistrements supportés
RECORD_TYPES = {
    'ACCOUNT_ADJUSTMENT': "SdpoutputCdr2_255_CS6.SDPCallDataRecord.accountAdjustment",
    'PERIODIC_ACCOUNT_MGMT': "SdpoutputCdr2_255_CS6.SDPCallDataRecord.periodicAccountMgmt",
    'LIFE_CYCLE_CHANGE': "SdpoutputCdr2_255_CS6.SDPCallDataRecord.lifeCycleChange"
}

def parse_record(lines: List[str]) -> Tuple[str, Dict]:
    """Parse un enregistrement du fichier ASN et retourne son type et les données."""
    if not lines:
        return None, {}
        
    record_type = lines[0].strip()
    data = {}
    
    for line in lines[2:-1]:  # Skip first and last lines (type and closing brace)
        line = line.strip()
        if not line or line.startswith('{') or line.endswith('}'):
            continue
            
        try:
            # Utiliser split avec maxsplit=1 pour gérer les valeurs contenant des ':'
            parts = line.split(':', 1)
            if len(parts) != 2:
                print(f"Warning: ligne mal formatée ignorée: {line}")
                continue
                
            key, value = parts
            key = key.strip()
            value = value.strip().strip('"\'')
            
            # Nettoyage des valeurs spéciales
            if value.endswith('D'):  # Valeur numérique suivie de 'D'
                value = value[:-2]
            elif value == "":  # Valeur vide
                value = None
                
            data[key] = value
            
        except Exception as e:
            print(f"Warning: erreur lors du parsing de la ligne: {line}")
            print(f"Error: {str(e)}")
            continue
            
    return record_type, data

def process_file(file_path: str):
    """Traite un fichier individuel de manière asynchrone."""
    try:
        logger.info(f"Début du traitement du fichier : {os.path.basename(file_path)}")
        
        # Décompresser le fichier si c'est un fichier .gz
        if file_path.endswith('.gz'):
            #logger.debug(f"Décompression du fichier {file_path}")
            with gzip.open(file_path, 'rt') as f:
                content = f.read()
        else:
            #logger.debug(f"Lecture du fichier non compressé {file_path}")
            with open(file_path, 'r') as f:
                content = f.read()
        
        #logger.info(f"Fichier lu avec succès, taille: {len(content)} caractères")
        
        # Parser les records directement
        records = []
        current_record = []
        
        for line in content.split('\n'):
            line = line.strip()
            if not line:
                continue
                
            if line in [RECORD_TYPES['ACCOUNT_ADJUSTMENT'], 
                      RECORD_TYPES['PERIODIC_ACCOUNT_MGMT'], 
                      RECORD_TYPES['LIFE_CYCLE_CHANGE']]:
                if current_record:
                    records.append(parse_record(current_record))
                current_record = [line]
            else:
                current_record.append(line)
                
        if current_record:
            records.append(parse_record(current_record))
        
        # Transformer en DataFrames
        adj_records = [rec[1] for rec in records if rec[0] == RECORD_TYPES['ACCOUNT_ADJUSTMENT']]
        periodic_records = [rec[1] for rec in records if rec[0] == RECORD_TYPES['PERIODIC_ACCOUNT_MGMT']]
        life_cycle_records = [rec[1] for rec in records if rec[0] == RECORD_TYPES['LIFE_CYCLE_CHANGE']]
        
        adj_df = pd.DataFrame(adj_records) if adj_records else pd.DataFrame()
        periodic_df = pd.DataFrame(periodic_records) if periodic_records else pd.DataFrame()
        life_cycle_df = pd.DataFrame(life_cycle_records) if life_cycle_records else pd.DataFrame()
        
        # Conversion des timestamps et types numériques
        for df in [adj_df, periodic_df, life_cycle_df]:
            if not df.empty:
                if 'adjustmentDate' in df.columns and 'adjustmentTime' in df.columns:
                    df['timestamp'] = pd.to_datetime(
                        df['adjustmentDate'] + df['adjustmentTime'],
                        format='%Y%m%d%H%M%S'
                    )
                elif 'timeStamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(
                        df['timeStamp'].str[:14],
                        format='%Y%m%d%H%M%S'
                    )
        
        return {
            'adjustments': adj_df,
            'periodic': periodic_df,
            'life_cycle': life_cycle_df
        }
        
    except Exception as e:
        logger.error(f"Erreur lors du traitement de {file_path}: {str(e)}")
        return {
            'adjustments': pd.DataFrame(),
            'periodic': pd.DataFrame(),
            'life_cycle': pd.DataFrame()
        }

def main():
    """Point d'entrée principal du processus ETL SDP."""
    start_time = time.time()
    
    logger.info("-" * 50)
    logger.info("ETL SDP - Démarrage")
    logger.info(f"• Date      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"• Source    : {os.path.basename(RAW_DATA_PATH)}")
    logger.info(f"• Cible     : {os.path.basename(PROCESSED_DATA_PATH)}")

    # Création des dossiers de sortie
    output_dirs = ["ACCOUNT_ADJUSTMENT", "PERIODIC_ACCOUNT_MGMT", "LIFE_CYCLE_CHANGE"]
    for dir_name in output_dirs:
        os.makedirs(os.path.join(PROCESSED_DATA_PATH, dir_name), exist_ok=True)

    # Liste des fichiers à traiter
    asn_files = glob.glob(os.path.join(RAW_DATA_PATH, "*.ASN")) + \
                glob.glob(os.path.join(RAW_DATA_PATH, "*.ASN.gz"))
    
    if not asn_files:
        logger.error(f"Aucun fichier ASN ou ASN.gz trouvé dans {RAW_DATA_PATH}")
        return
    
    logger.info(f"• À traiter : {len(asn_files)} fichiers")
    
    logger.info("\nTraitement des fichiers...")
    
    # Initialisation des accumulateurs
    dataframes = {
        'adjustments': [],
        'periodic': [],
        'life_cycle': []
    }
    stats = {
        'réussis': 0,
        'échecs': 0,
        'total_records': 0
    }

    # Traitement des fichiers
    for file_path in asn_files:
        try:
            result = process_file(file_path)
            has_data = False
            
            for key, df in result.items():
                if df is not None and not df.empty:
                    dataframes[key].append(df)
                    stats['total_records'] += len(df)
                    has_data = True
                    
            if has_data:
                stats['réussis'] += 1
            else:
                stats['échecs'] += 1
                
        except Exception as error:
            logger.error(f"Erreur lors du traitement de {os.path.basename(file_path)}: {str(error)}")
            stats['échecs'] += 1

    logger.info("\nTraitement des fichiers terminé.")
    
    # Sauvegarde des résultats
    logger.info("\nSauvegarde des fichiers parquet...")
    
    try:
        # ACCOUNT_ADJUSTMENT
        if dataframes['adjustments']:
            df = pd.concat(dataframes['adjustments'], ignore_index=True)
            output_path = os.path.join(PROCESSED_DATA_PATH, "ACCOUNT_ADJUSTMENT/account_adjustments.parquet")
            df.to_parquet(output_path, index=False)
            logger.info(f"• ACCOUNT_ADJUSTMENT    : {len(df):,} enregistrements")
        
        # PERIODIC_ACCOUNT_MGMT
        if dataframes['periodic']:
            df = pd.concat(dataframes['periodic'], ignore_index=True)
            output_path = os.path.join(PROCESSED_DATA_PATH, "PERIODIC_ACCOUNT_MGMT/periodic_account_mgmt.parquet")
            df.to_parquet(output_path, index=False)
            logger.info(f"• PERIODIC_ACCOUNT_MGMT : {len(df):,} enregistrements")
        
        # LIFE_CYCLE_CHANGE
        if dataframes['life_cycle']:
            df = pd.concat(dataframes['life_cycle'], ignore_index=True)
            output_path = os.path.join(PROCESSED_DATA_PATH, "LIFE_CYCLE_CHANGE/life_cycle_changes.parquet")
            df.to_parquet(output_path, index=False)
            logger.info(f"• LIFE_CYCLE_CHANGE    : {len(df):,} enregistrements")

    except Exception as error:
        logger.error(f"Erreur lors de la sauvegarde: {str(error)}")
        raise

    # Résumé final
    execution_time = time.time() - start_time
    logger.info("="*70)
    logger.info("\nRésultats du traitement:")
    logger.info(f"• Fichiers traités avec succès : {stats['réussis']:,}")
    logger.info(f"• Fichiers en erreur          : {stats['échecs']:,}")
    logger.info(f"• Total des enregistrements   : {stats['total_records']:,}")
    logger.info(f"• Temps d'exécution           : {execution_time:.2f} secondes")
    logger.info("="*70 + "\n")