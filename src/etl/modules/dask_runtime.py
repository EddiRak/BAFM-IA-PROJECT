# src/etl/modules/dask_runtime.py
import os
import logging
import tempfile
import yaml
from pathlib import Path
from contextlib import contextmanager
from dask.distributed import Client
from distributed import LocalCluster

# -------------------------------------------------------------------
# 0) Chargement de la configuration
# -------------------------------------------------------------------
def load_config():
    config_path = Path(os.getcwd()) / "config" / "dask_settings.yaml"
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.warning(f"Impossible de charger la configuration: {e}. Utilisation des valeurs par défaut.")
        return {}

# Configuration globale
DASK_CONFIG = load_config()

# -------------------------------------------------------------------
# 1) Dossier temporaire fiable (écriture ok) pour spill et config
# -------------------------------------------------------------------
def _resolve_local_dir() -> str:
    paths_config = DASK_CONFIG.get('paths', {})
    default_tmp = paths_config.get('dask_tmp', 'dask_tmp')
    
    for p in (
        os.environ.get("DASK_TEMPORARY_DIRECTORY"),
        os.path.abspath(os.path.join(os.getcwd(), default_tmp)),
        os.path.join(tempfile.gettempdir(), "dask_tmp"),
    ):
        if not p:
            continue
        try:
            path = Path(p)
            path.mkdir(parents=True, exist_ok=True)
            t = path / ".write_test"
            t.write_text("ok", encoding="utf-8")
            t.unlink(missing_ok=True)
            return str(path)
        except Exception:
            continue
    # dernier recours : %TEMP%
    fallback = os.path.join(tempfile.gettempdir(), "dask_tmp")
    Path(fallback).mkdir(parents=True, exist_ok=True)
    return fallback

# -------------------------------------------------------------------
# 2) Crée un dask.yaml *silencieux* et l'impose aux sous-processus
#    (les workers lisent DASK_CONFIG au démarrage)
# -------------------------------------------------------------------
def _ensure_quiet_dask_yaml(config_dir: str) -> None:
    cfg_dir = Path(config_dir)
    cfg_dir.mkdir(parents=True, exist_ok=True)
    yaml_path = cfg_dir / "dask.yaml"
    
    # Utiliser la configuration des logs depuis le fichier de configuration
    logging_config = DASK_CONFIG.get('logging', {
        'distributed': 'error',
        'distributed.worker': 'error',
        'distributed.scheduler': 'error',
        'distributed.nanny': 'error',
        'distributed.comm': 'error',
        'tornado': 'critical',
        'bokeh': 'critical'
    })
    
    yaml_content = yaml.dump({'logging': logging_config})
    
    # Écrire seulement si absent ou différent
    try:
        if (not yaml_path.exists()) or (yaml_path.read_text(encoding="utf-8") != yaml_content):
            yaml_path.write_text(yaml_content, encoding="utf-8")
        # Forcer Dask à utiliser CE dossier de config
        os.environ["DASK_CONFIG"] = str(cfg_dir)
    except Exception:
        # Si la config échoue, on continue (on a d'autres garde-fous)
        pass

# -------------------------------------------------------------------
# 3) Filtre de sécurité côté parent (au cas où)
# -------------------------------------------------------------------
class _DropNoisyRecords(logging.Filter):
    TOKENS = (
        "tcp://", "Start Nanny", "Start worker", "Listening to:",
        "dashboard at", "Registered to:", "Worker plugin shuffle",
        "Waiting to connect to:", "Stopping worker", "Retire worker",
        "Remove worker",
    )
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            return True
        return not any(tok in msg for tok in self.TOKENS)

def _silence_parent_loggers(level=logging.ERROR):
    noisy = [
        "distributed", "distributed.client", "distributed.scheduler",
        "distributed.worker", "distributed.nanny", "distributed.comm",
        "distributed.http", "bokeh", "tornado",
    ]
    for name in noisy:
        lg = logging.getLogger(name)
        lg.setLevel(level)
        lg.propagate = False
        if not lg.handlers:
            lg.addHandler(logging.NullHandler())
    logging.getLogger().addFilter(_DropNoisyRecords())

@contextmanager
def _suppress_all_logging():
    prev = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(prev)

# -------------------------------------------------------------------
# 4) Point d'entrée: Client silencieux
# -------------------------------------------------------------------
def _start_client():
    workers_config = DASK_CONFIG.get('workers', {})
    
    # Configuration des workers avec valeurs par défaut
    n_workers = int(os.environ.get("DASK_WORKERS", 
                                 str(workers_config.get('n_workers', 
                                                     min(2, (os.cpu_count() or 2))))))
    threads_per_worker = int(os.environ.get("DASK_THREADS_PER_WORKER",
                                          str(workers_config.get('threads_per_worker', 1))))
    memory_limit = os.environ.get("DASK_MEMORY_LIMIT",
                                workers_config.get('memory_limit', "900MB"))

    local_dir = _resolve_local_dir()

    # Préparer et activer une config Dask *silencieuse* pour les sous-processus
    paths_config = DASK_CONFIG.get('paths', {})
    config_subdir = paths_config.get('config_subdir', 'dask_config')
    cfg_dir = os.path.join(local_dir, config_subdir)
    _ensure_quiet_dask_yaml(cfg_dir)

    # Réduire aussi les logs dans le process parent
    _silence_parent_loggers(logging.ERROR)

    # Dashboard configuration
    dashboard_config = DASK_CONFIG.get('dashboard', {})
    dashboard_address = None if not dashboard_config.get('enabled', False) else ':8787'

    # Créer le cluster en "mode muet" pour éviter le flot au bootstrap
    with _suppress_all_logging():
        cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            processes=workers_config.get('processes', True),
            memory_limit=memory_limit,
            local_directory=local_dir,
            dashboard_address=dashboard_address,
            silence_logs=logging.ERROR,
        )
    return Client(cluster)

def quiet_close(client: Client | None):
    if client is None:
        return
    with _suppress_all_logging():
        try:
            if getattr(client, "cluster", None):
                try:
                    client.cluster.close(timeout=2)
                except Exception:
                    pass
            client.close(timeout=2)
        except Exception:
            pass