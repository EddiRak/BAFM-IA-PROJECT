import os
import logging
from datetime import datetime
import time

# Import des modules personnalisés
from sdp_etl import main as sdp_main
from msc_etl import main as msc_main
from air_etl import main as air_main
from occ_etl import main as occ_main
from ccn_etl import main as ccn_main
from sgsn_etl import main as sgsn_main

# Configurer le logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    """Fonction principale du processus ETL."""
    start_time = time.time()
    logger.info("Démarrage du pipeline ETL")
    
    try:
        # Traitement des données SDP
        logger.info("Traitement des données SDP...")
        sdp_main()

        print("\n")
        print("\n")
        
        # Traitement des données MSC
        logger.info("Traitement des données MSC...")
        msc_main()

        
        print("\n")
        print("\n")

        # Traitement des données AIR
        logger.info("Traitement des données AIR...")
        air_main()

        
        print("\n")
        print("\n")

        # Traitement des données OCC
        logger.info("Traitement des données OCC...")
        occ_main()

        print("\n")
        print("\n")

        # Traitement des données CCN
        logger.info("Traitement des données CCN...")
        ccn_main()

        print("\n")
        print("\n")

        # Traitement des données SGSN
        logger.info("Traitement des données SGSN...")
        sgsn_main()
        
        execution_time = time.time() - start_time
        logger.info(f"Pipeline ETL complet terminé avec succès en {execution_time:.2f} secondes")
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du pipeline ETL: {str(e)}")
        raise

if __name__ == "__main__":
    main()
