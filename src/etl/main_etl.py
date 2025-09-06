import os
import logging
from datetime import datetime
import time

# Import des modules personnalisés
from sdp_etl import main as sdp_main
from msc_etl import main as msc_main

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
        
        # Traitement des données MSC
        logger.info("Traitement des données MSC...")
        msc_main()
        
        execution_time = time.time() - start_time
        logger.info(f"Pipeline ETL complet terminé avec succès en {execution_time:.2f} secondes")
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du pipeline ETL: {str(e)}")
        raise

if __name__ == "__main__":
    main()
