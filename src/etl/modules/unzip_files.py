import gzip
import logging
from io import BytesIO
import dask.delayed as delayed

# Configurer le logger
logging.basicConfig(level=logging.INFO)

@delayed
def unzip_file(gz_path: str):
    """Décompresser un fichier .gz en mémoire et retourne le contenu."""
    try:
        logging.info(f"Décompression de {gz_path} en mémoire")
        
        with gzip.open(gz_path, 'rb') as f_in:
            # Lire tout le contenu du fichier gzip dans la mémoire
            file_content = f_in.read()
        
        logging.info(f"Décompression réussie pour {gz_path}")
        return file_content
    except Exception as e:
        logging.error(f"Erreur de décompression pour {gz_path}: {e}")
        raise



