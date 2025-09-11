# -*- coding: utf-8 -*-
"""
Orchestrateur ETL : enchaîne tous les jobs (SDP, AIR, MSC, CCN, OCC, GGSN)
Exécution locale :  python src/etl/main_etl.py
"""

import os
import sys
import time
import logging
from datetime import datetime

# --- Logging compact et lisible ---
logging.basicConfig(level=logging.INFO, format="%(levelname)-8s - %(message)s")
logger = logging.getLogger("main_etl")

# --- S'assurer que 'src' est dans le PYTHONPATH pour les imports ---
HERE = os.path.abspath(os.path.dirname(__file__))
SRC_ROOT = os.path.abspath(os.path.join(HERE, os.pardir))  # src/
if SRC_ROOT not in sys.path:
    sys.path.insert(0, SRC_ROOT)

# --- Imports des jobs ETL (les scripts existants conservent leurs logiques) ---
try:
    from etl.sdp_etl import main as sdp_main
except Exception:  # fallback si exécuté comme script simple depuis src/etl
    from sdp_etl import main as sdp_main

try:
    from etl.air_etl import main as air_main
except Exception:
    from air_etl import main as air_main

try:
    from etl.msc_etl import main as msc_main
except Exception:
    from msc_etl import main as msc_main

try:
    from etl.ccn_etl import main as ccn_main
except Exception:
    from ccn_etl import main as ccn_main

# Ces deux-là vous les avez fournis/mentionnés ensuite
try:
    from etl.occ_etl import main as occ_main  # refactor selon le même modèle CCN/OCC
except Exception:
    try:
        from occ_etl import main as occ_main
    except Exception:
        occ_main = None

try:
    from etl.ggsn_etl import main as ggsn_main  # refactor selon le même modèle GGSN
except Exception:
    try:
        from ggsn_etl import main as ggsn_main
    except Exception:
        ggsn_main = None


def run_all():
    t0 = time.time()
    logger.info("=" * 70)
    logger.info("Démarrage du pipeline ETL")
    logger.info(f"• Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)

    stages = [
        ("SDP", sdp_main),
        ("AIR", air_main),
        ("MSC", msc_main),
        ("CCN", ccn_main),
        ("OCC", occ_main),
        ("GGSN", ggsn_main),
    ]

    errors = []
    for name, fn in stages:
        if fn is None:
            logger.warning(f"[SKIP] {name} : fonction non disponible (module introuvable).")
            continue
        logger.info("")
        logger.info(f"Traitement des données {name}...")
        logger.info("-" * 50)
        try:
            fn()
        except SystemExit:
            # Permettre aux scripts d'utiliser sys.exit() sans stopper le pipeline
            logger.exception(f"[{name}] a levé SystemExit (poursuite du pipeline).")
        except Exception as e:
            logger.exception(f"[{name}] échec : {e}")
            errors.append(name)

    elapsed = time.time() - t0
    logger.info("")
    logger.info("=" * 70)
    logger.info("Pipeline ETL terminé")
    if errors:
        logger.error(f"Étapes en erreur : {', '.join(errors)}")
    else:
        logger.info("Toutes les étapes ont terminé sans erreur.")
    logger.info(f"Temps total : {elapsed:.2f} s")
    logger.info("=" * 70)
    # Optionnel : retourner un code non-zéro si erreurs
    if errors:
        # Laisser Airflow marquer la tâche en échec si on réutilise run_all() dans le DAG
        raise RuntimeError(f"Échec partiel des étapes : {', '.join(errors)}")


def main():
    run_all()


if __name__ == "__main__":
    main()
